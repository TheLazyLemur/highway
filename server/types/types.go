package types

type Message struct {
	Type    string `json:"type"`
	Message any    `json:"message"`
}

type InitMessage struct {
	Role      string `json:"role"`
	Name      string `json:"name"`
	QueueName string `json:"queue_name"`
}

type PushMessage struct {
	EventType      string `json:"event_type"`
	MessagePayload string `json:"message_payload"`
}

type ConsumeMessage struct {
	QueueName    string `json:"queue_name"`
	ConsumerName string `json:"consumer_name"`
}
