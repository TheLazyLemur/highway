package repo

type Repo interface {
	AddMessage(queueName string, message MessageModel) error
	GetMessage(queueName, consumerName string) (MessageModel, error)
	AckMessage(queueName, consumerName string, messageId int64) error
}

type MessageModel struct {
	Id             int64
	EventType      string
	MessagePayload string
}
