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
	mutex  sync.RWMutex
	queues map[string][]MessageModel
	lastID int64
	cursor int64
}

func NewMessageRepo() *MessageRepo {
	return &MessageRepo{
		mutex:  sync.RWMutex{},
		queues: make(map[string][]MessageModel),
		lastID: 0,
		cursor: 0,
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

	var msg MessageModel
	for _, mm := range m.queues[queueName] {
		if mm.Id > m.cursor {
			// TODO: Implement cursor per consumer
			msg = mm
			m.cursor++
			break
		}
	}
	return msg, nil
}
