package ops

import "sync"

type MessageModel struct {
	Id             int    `json:"id"`
	EventType      string `json:"event_type"`
	MessagePayload string `json:"message_payload"`
}

type MessageRepo struct {
	messages []MessageModel
	mu       sync.RWMutex
}

func NewMessageRepo() *MessageRepo {
	return &MessageRepo{
		mu:       sync.RWMutex{},
		messages: []MessageModel{},
	}
}

func (m *MessageRepo) AddMessage(message MessageModel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, message)
}

func (m *MessageRepo) NextID() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.messages) + 1
}
