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
	Init    types.Action = "init"
	Push    types.Action = "push"
	Consume types.Action = "consume"
	Ack     types.Action = "ack"
)

var (
	ErrorConnectionClosed     = errors.New("connection closed by client")
	ErrorConsumerNameRequired = errors.New("consumer name is required")
	ErrorInvalidAction        = errors.New("invalid action type")
	ErrorInvalidDataShape     = errors.New("invalid data shape, expected map[string]any")
	ErrorInvalidMessageType   = errors.New("invalid message type")
	ErrorInvalidRole          = errors.New("invalid role")
	ErrorMarshalData          = errors.New("failed to marshal data")
	ErrorQueueNameRequired    = errors.New("queue name is required")
	ErrorUnmarshalData        = errors.New("failed to unmarshal data")
)
