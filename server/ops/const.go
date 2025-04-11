package ops

import (
	"errors"

	"highway/server/types"
)

const (
	Producer types.Role = "producer"
	Consumer types.Role = "consumer"
)

const (
	Push    types.Action = "push"
	Consume types.Action = "consume"
)

var (
	ErrorQueueNameRequired    = errors.New("queue name is required")
	ErrorConsumerNameRequired = errors.New("consumer name is required")
	ErrorInvalidMessageType   = errors.New("invalid message type")
	ErrorInvalidRole          = errors.New("invalid role")
	ErrorInvalidAction        = errors.New("invalid action type")
)
