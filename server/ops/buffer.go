package ops

import (
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/TheLazyLemur/highway/server/repo"
)

type BufferedMessage struct {
	QueueName string
	Message   repo.MessageModelWithQueue
	Timestamp time.Time
}

type MessageBuffer struct {
	mu       *sync.Mutex
	messages []BufferedMessage
	dbRepo   repo.Repo
	timer    *time.Timer
	size     int64
	timeOut  int64
}

func NewMessageBuffer(size int64, timeOutSeconds int64, dbRepo repo.Repo) *MessageBuffer {
	buf := &MessageBuffer{
		messages: make([]BufferedMessage, 0, size),
		dbRepo:   dbRepo,
		timer:    time.NewTimer(time.Duration(timeOutSeconds) * time.Second),
		size:     size,
		mu:       &sync.Mutex{},
		timeOut:  timeOutSeconds,
	}

	go buf.periodicFlush()

	return buf
}

func (b *MessageBuffer) AddMessage(queueName string, message repo.MessageModel) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.messages = append(b.messages, BufferedMessage{
		QueueName: queueName,
		Message: repo.MessageModelWithQueue{
			QueueName: queueName,
			Message:   message,
		},
		Timestamp: time.Now(),
	})

	if len(b.messages) >= int(b.size) {
		return b.flushBufferWithLock(true) // Pass true to indicate lock is already held
	}

	return nil
}

func (b *MessageBuffer) periodicFlush() {
	for {
		<-b.timer.C
		if err := b.flushBufferWithLock(false); err != nil {
			slog.Error("Failed to flush message buffer", "error", err)
		}
		b.timer.Reset(time.Duration(b.timeOut) * time.Second)
	}
}

func (b *MessageBuffer) flushBuffer() error {
	return b.flushBufferWithLock(false)
}

func (b *MessageBuffer) flushBufferWithLock(lockAcquired bool) error {
	if !lockAcquired {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	if len(b.messages) == 0 {
		return nil // Nothing to flush
	}

	sort.Slice(b.messages, func(i, j int) bool {
		return b.messages[i].Timestamp.Before(b.messages[j].Timestamp)
	})

	msgs := make([]repo.MessageModelWithQueue, len(b.messages))
	for i, msg := range b.messages {
		msgs[i] = msg.Message
	}

	var err error
	if err = withRetry(5, func() error {
		return b.dbRepo.AddMessages(msgs)
	}); err != nil {
		slog.Error("Failed to add message to queue", "error", err)
	}

	b.messages = make([]BufferedMessage, 0, b.size)
	return err
}
