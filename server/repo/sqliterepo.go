package repo

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteRepo struct {
	db *sql.DB
}

// NewSQLiteRepo initializes a new SQLiteRepo instance with the given database path.
// It configures SQLite settings for performance optimization.
func NewSQLiteRepo(dbPath string) (*SQLiteRepo, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	_, err = db.Exec(`
	PRAGMA synchronous = OFF;
	PRAGMA journal_mode = WAL;
	PRAGMA temp_store = MEMORY;
	PRAGMA busy_timeout = 5000;
	PRAGMA cache_size = 10000;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to configure SQLite settings: %w", err)
	}

	// Configure connection pool for concurrency
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)

	return &SQLiteRepo{db: db}, nil
}

// RunMigrations creates the necessary database tables if they do not already exist.
func (r *SQLiteRepo) RunMigrations() error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	query := `
	CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		queue_name TEXT NOT NULL,
		event_type TEXT NOT NULL,
		message_payload TEXT NOT NULL,
		UNIQUE(queue_name, id)
	);

	CREATE TABLE IF NOT EXISTS consumer_cursors (
		consumer_name TEXT PRIMARY KEY,
		queue_name TEXT NOT NULL,
		cursor INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS acks (
		consumer_name TEXT NOT NULL,
		queue_name TEXT NOT NULL,
		message_id INTEGER NOT NULL,
		PRIMARY KEY (consumer_name, queue_name, message_id)
	);
	`
	_, err = tx.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return tx.Commit()
}

// AddMessage inserts a new message into the specified queue.
func (r *SQLiteRepo) AddMessage(queueName string, message MessageModel) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	query := `
	INSERT INTO messages (queue_name, event_type, message_payload)
	VALUES (?, ?, ?);
	`
	_, err = tx.Exec(query, queueName, message.EventType, message.MessagePayload)
	if err != nil {
		return fmt.Errorf("failed to add message: %w", err)
	}

	return tx.Commit()
}

// GetMessage retrieves the next available message for a consumer from the specified queue.
// It also updates the consumer's cursor to the retrieved message's ID.
func (r *SQLiteRepo) GetMessage(queueName, consumerName string) (MessageModel, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return MessageModel{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	var cursor int64
	err = tx.QueryRow(`
	SELECT cursor FROM consumer_cursors
	WHERE consumer_name = ? AND queue_name = ?;
	`, consumerName, queueName).Scan(&cursor)
	if err != nil && err != sql.ErrNoRows {
		return MessageModel{}, fmt.Errorf("failed to get consumer cursor: %w", err)
	}

	// Find the lowest unacknowledged message for this consumer
	var unackedMessageId int64
	err = tx.QueryRow(`
	SELECT id FROM messages
	WHERE queue_name = ?
	AND id NOT IN (
		SELECT message_id FROM acks
		WHERE consumer_name = ? AND queue_name = ?
	)
	ORDER BY id ASC
	LIMIT 1;
	`, queueName, consumerName, queueName).Scan(&unackedMessageId)
	if err != nil && err != sql.ErrNoRows {
		return MessageModel{}, fmt.Errorf("failed to check for unacknowledged messages: %w", err)
	}

	// If there's an unacknowledged message with ID lower than the cursor,
	// use that as the starting point
	if err != sql.ErrNoRows && unackedMessageId < cursor {
		cursor = unackedMessageId
	}

	// Get the next unacknowledged message for this specific consumer
	var msg MessageModel
	err = tx.QueryRow(`
	SELECT id, event_type, message_payload
	FROM messages
	WHERE queue_name = ? 
	AND id NOT IN (
		SELECT message_id FROM acks
		WHERE consumer_name = ? AND queue_name = ?
	)
	ORDER BY id ASC
	LIMIT 1;
	`, queueName, consumerName, queueName).Scan(&msg.Id, &msg.EventType, &msg.MessagePayload)
	if err != nil {
		if err == sql.ErrNoRows {
			err = tx.Rollback()
			if err != nil {
				return MessageModel{}, fmt.Errorf("failed to rollback transaction: %w", err)
			}
			return MessageModel{}, nil // No new messages
		}
		return MessageModel{}, fmt.Errorf("failed to get message: %w", err)
	}

	// Update the consumer's cursor to the message's ID
	_, err = tx.Exec(`
	INSERT INTO consumer_cursors (consumer_name, queue_name, cursor)
	VALUES (?, ?, ?)
	ON CONFLICT(consumer_name) DO UPDATE SET cursor = excluded.cursor;
	`, consumerName, queueName, msg.Id)
	if err != nil {
		return MessageModel{}, fmt.Errorf("failed to update consumer cursor: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return MessageModel{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return msg, nil
}

// PeekMessage retrieves the next available message for a consumer from the specified queue
// without updating the consumer's cursor.
func (r *SQLiteRepo) PeekMessage(queueName, consumerName string) (MessageModel, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return MessageModel{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var cursor int64
	err = tx.QueryRow(`
	SELECT cursor FROM consumer_cursors
	WHERE consumer_name = ? AND queue_name = ?;
	`, consumerName, queueName).Scan(&cursor)
	if err != nil && err != sql.ErrNoRows {
		return MessageModel{}, fmt.Errorf("failed to get consumer cursor: %w", err)
	}

	var unackedMessageId int64
	err = tx.QueryRow(`
	SELECT id FROM messages
	WHERE queue_name = ?
	AND id NOT IN (
		SELECT message_id FROM acks
		WHERE consumer_name = ? AND queue_name = ?
	)
	ORDER BY id ASC
	LIMIT 1;
	`, queueName, consumerName, queueName).Scan(&unackedMessageId)
	if err != nil && err != sql.ErrNoRows {
		return MessageModel{}, fmt.Errorf("failed to check for unacknowledged messages: %w", err)
	}

	if err != sql.ErrNoRows && unackedMessageId < cursor {
		cursor = unackedMessageId
	}

	// Get the next unacknowledged message for this specific consumer
	var msg MessageModel
	err = tx.QueryRow(`
	SELECT id, event_type, message_payload
	FROM messages
	WHERE queue_name = ? 
	AND id NOT IN (
		SELECT message_id FROM acks
		WHERE consumer_name = ? AND queue_name = ?
	)
	ORDER BY id ASC
	LIMIT 1;
	`, queueName, consumerName, queueName).Scan(&msg.Id, &msg.EventType, &msg.MessagePayload)
	if err != nil {
		if err == sql.ErrNoRows {
			return MessageModel{}, nil // No new messages
		}
		return MessageModel{}, fmt.Errorf("failed to peek message: %w", err)
	}

	// Note: We do NOT update the cursor for a peek operation

	return msg, nil
}

// AckMessage acknowledges the processing of a message by a consumer.
func (r *SQLiteRepo) AckMessage(queueName, consumerName string, messageId int64) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(`
	INSERT INTO acks (consumer_name, queue_name, message_id)
	VALUES (?, ?, ?)
	ON CONFLICT DO NOTHING;
	`, consumerName, queueName, messageId)
	if err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	return tx.Commit()
}

// AddMessages inserts multiple messages into the specified queue in a single transaction.
func (r *SQLiteRepo) AddMessages(messages []MessageModelWithQueue) error {
	if len(messages) == 0 {
		return nil // Nothing to do
	}

	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	query := `
	INSERT INTO messages (queue_name, event_type, message_payload)
	VALUES (?, ?, ?);
	`
	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, message := range messages {
		_, err = stmt.Exec(
			message.QueueName,
			message.Message.EventType,
			message.Message.MessagePayload,
		)
		if err != nil {
			return fmt.Errorf("failed to add message: %w", err)
		}
	}

	return tx.Commit()
}
