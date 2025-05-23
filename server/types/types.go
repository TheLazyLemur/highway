package types

type (
	Role   string
	Action string
	Type   string
)

type Message struct {
	Action  Action `json:"type"`
	Message any    `json:"message"`
}

type InitMessage struct {
	Role Role `json:"role"`
}

type PushMessage struct {
	EventType      string `json:"event_type"`
	QueueName      string `json:"queue_name"`
	MessagePayload string `json:"message_payload"`
}

type ConsumeMessage struct {
	QueueName    string `json:"queue_name"`
	ConsumerName string `json:"consumer_name"`
}

type AckMessage struct {
	QueueName    string `json:"queue_name"`
	ConsumerName string `json:"consumer_name"`
	MessageId    int64  `json:"message_id"`
}

type PeekMessage struct {
	QueueName    string `json:"queue_name"`
	ConsumerName string `json:"consumer_name"`
}