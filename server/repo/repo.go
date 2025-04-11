package repo

import "sync"

type Repo interface {
	AddMessage(queueName string, message MessageModel) error
	GetMessage(queueName, consumerName string) (MessageModel, error)
}

type MessageModel struct {
	Id             int64
	EventType      string
	MessagePayload string
}

type MessageRepo struct {
	mutex           sync.RWMutex
	queues          map[string][]MessageModel
	lastID          int64
	consumerCursors map[string]int64 // Map to store cursors for each consumer
}

func NewMessageRepo() *MessageRepo {
	return &MessageRepo{
		mutex:           sync.RWMutex{},
		queues:          make(map[string][]MessageModel),
		lastID:          0,
		consumerCursors: make(map[string]int64),
	}
}

func (m *MessageRepo) AddMessage(queueName string, message MessageModel) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, ok := m.queues[queueName]
	if !ok {
		m.queues[queueName] = []MessageModel{}
	}

	message.Id = m.nextID()
	m.queues[queueName] = append(m.queues[queueName], message)

	return nil
}

func (m *MessageRepo) nextID() int64 {
	m.lastID++
	return m.lastID
}

func (m *MessageRepo) GetMessage(queueName, consumerName string) (MessageModel, error) {
	_, ok := m.queues[queueName]
	if !ok {
		m.queues[queueName] = []MessageModel{}
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	var msg MessageModel
	cursor, ok := m.consumerCursors[consumerName]
	if !ok {
		cursor = 0
	}

	for _, mm := range m.queues[queueName] {
		if mm.Id > cursor {
			msg = mm
			m.consumerCursors[consumerName] = mm.Id
			break
		}
	}
	return msg, nil
}
