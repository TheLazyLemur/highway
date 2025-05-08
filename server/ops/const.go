package ops

import (
	"errors"

	"github.com/TheLazyLemur/highway/server/types"
)

const (
	Producer types.Role = "producer"
	Consumer types.Role = "consumer"
	Cache    types.Role = "cache"
)

const (
	Init        types.Action = "init"
	Push        types.Action = "push"
	Consume     types.Action = "consume"
	Ack         types.Action = "ack"
	Peek        types.Action = "peek"
	CacheSet    types.Action = "cache_set"
	CacheGet    types.Action = "cache_get"
	CacheDelete types.Action = "cache_delete"
	CacheClear  types.Action = "cache_clear"
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
	ErrorCacheKeyRequired     = errors.New("cache key is required")
	ErrorCacheValueRequired   = errors.New("cache value is required")
	ErrorCacheKeyNotFound     = errors.New("cache key not found")
)