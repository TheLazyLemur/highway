package ops

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TheLazyLemur/highway/server/cache"
	"github.com/TheLazyLemur/highway/server/repo"
	"github.com/TheLazyLemur/highway/server/types"
)

func createTestInitMessage(role types.Role, queueName, name string) types.Message {
	return types.Message{
		Message: map[string]any{
			"role":       role,
			"queue_name": queueName,
			"name":       name,
		},
	}
}

func TestCacheOperations(t *testing.T) {
	// Initialize cache
	memCache := cache.NewMemoryCache()

	// Test cases for caching operations
	tests := []struct {
		name         string
		action       types.Action
		message      any
		expectedResp map[string]any
		setup        func()
		validate     func(t *testing.T, resp map[string]any)
	}{
		{
			name:   "Set Cache Key",
			action: CacheSet,
			message: types.CacheSetMessage{
				Key:        "test-key",
				Value:      "test-value",
				Expiration: 1000, // 1 second
			},
			expectedResp: map[string]any{
				"success": true,
			},
			setup: func() {},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, true, resp["success"])

				// Verify the key was actually set
				value, found := memCache.Get("test-key")
				assert.True(t, found, "Key should be found in cache")
				assert.Equal(t, "test-value", string(value), "Stored value should match")
			},
		},
		{
			name:   "Get Cache Key",
			action: CacheGet,
			message: types.CacheGetMessage{
				Key: "test-key",
			},
			setup: func() {
				memCache.Set("test-key", []byte("test-value"), 10*time.Second)
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, true, resp["success"])
				assert.Equal(t, "test-value", resp["value"])
			},
		},
		{
			name:   "Get Non-existent Cache Key",
			action: CacheGet,
			message: types.CacheGetMessage{
				Key: "non-existent-key",
			},
			setup: func() {},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, false, resp["success"])
				assert.Contains(t, resp["error"], "not found")
			},
		},
		{
			name:   "Delete Cache Key",
			action: CacheDelete,
			message: types.CacheDeleteMessage{
				Key: "test-key",
			},
			setup: func() {
				memCache.Set("test-key", []byte("test-value"), 10*time.Second)
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, true, resp["success"])

				// Verify the key was actually deleted
				_, found := memCache.Get("test-key")
				assert.False(t, found, "Key should not be found after delete")
			},
		},
		{
			name:    "Clear Cache",
			action:  CacheClear,
			message: types.CacheClearMessage{},
			setup: func() {
				memCache.Set("key1", []byte("value1"), 10*time.Second)
				memCache.Set("key2", []byte("value2"), 10*time.Second)
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, true, resp["success"])

				// Verify all keys were cleared
				_, found1 := memCache.Get("key1")
				_, found2 := memCache.Get("key2")
				assert.False(t, found1, "Key1 should not be found after clear")
				assert.False(t, found2, "Key2 should not be found after clear")
			},
		},
		{
			name:   "Set Cache with Empty Key",
			action: CacheSet,
			message: types.CacheSetMessage{
				Key:   "",
				Value: "test-value",
			},
			setup: func() {},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, false, resp["success"])
				assert.Contains(t, resp["error"], "key is required")
			},
		},
		{
			name:   "Set Cache with Empty Value",
			action: CacheSet,
			message: types.CacheSetMessage{
				Key:   "test-key",
				Value: "",
			},
			setup: func() {},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, false, resp["success"])
				assert.Contains(t, resp["error"], "value is required")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			tt.setup()

			// Prepare the message
			msgBytes, err := json.Marshal(tt.message)
			require.NoError(t, err)

			var rawMsg map[string]any
			err = json.Unmarshal(msgBytes, &rawMsg)
			require.NoError(t, err)

			msg := types.Message{
				Action:  tt.action,
				Message: rawMsg,
			}

			// Create a buffer to capture the output
			var output bytes.Buffer
			encoder := json.NewEncoder(&output)

			// Handle the cache operation based on the action
			var err2 error
			switch tt.action {
			case CacheSet:
				err2 = handleCacheSet(memCache, msg, encoder)
			case CacheGet:
				err2 = handleCacheGet(memCache, msg, encoder)
			case CacheDelete:
				err2 = handleCacheDelete(memCache, msg, encoder)
			case CacheClear:
				err2 = handleCacheClear(memCache, msg, encoder)
			}
			require.NoError(t, err2)

			// Parse the response
			var resp map[string]any
			err = json.Unmarshal(output.Bytes(), &resp)
			require.NoError(t, err)

			// Validate the response
			tt.validate(t, resp)
		})
	}
}

func TestCacheExpiration(t *testing.T) {
	// Initialize cache
	memCache := cache.NewMemoryCache()

	// Set a key with a very short expiration (10ms)
	key := "expiring-key"
	value := "expiring-value"
	expiration := 10 * time.Millisecond

	// Set the key
	err := memCache.Set(key, []byte(value), expiration)
	require.NoError(t, err)

	// Verify it's there immediately
	storedValue, found := memCache.Get(key)
	require.True(t, found)
	require.Equal(t, value, string(storedValue))

	// Wait for the key to expire
	time.Sleep(20 * time.Millisecond)

	// Verify it's gone after expiration
	_, found = memCache.Get(key)
	require.False(t, found, "Key should be expired")
}

func TestCacheHandlerWithInvalidMessage(t *testing.T) {
	// Initialize cache
	memCache := cache.NewMemoryCache()

	// Invalid message type (not a map)
	invalidMsg := types.Message{
		Action:  CacheSet,
		Message: "not a map",
	}

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	err := handleCacheSet(memCache, invalidMsg, encoder)
	require.Error(t, err)
	assert.Equal(t, ErrorInvalidDataShape, err)

	// Invalid message structure
	invalidStructMsg := types.Message{
		Action: CacheGet,
		Message: map[string]any{
			"not_key": "some value", // Doesn't match CacheGetMessage structure
		},
	}

	output.Reset()
	err = handleCacheGet(memCache, invalidStructMsg, encoder)
	require.NoError(t, err) // Should respond with error in the response, not return error

	// Parse the response
	var resp map[string]any
	err = json.Unmarshal(output.Bytes(), &resp)
	require.NoError(t, err)

	// Validate the response indicates an error
	assert.Equal(t, false, resp["success"])
	assert.Contains(t, resp["error"], "key is required")
}

func TestInitConnectionConsumerBasic(t *testing.T) {
	msg := createTestInitMessage("consumer", "test-queue", "consumer-1")
	decoder := json.NewDecoder(bytes.NewBufferString(""))

	svc := NewService(nil, nil, nil)
	err := svc.initConnection(msg, decoder, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestInitConnectionConsumerInvalidInputs(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError error
	}{
		{
			name: "MissingConsumerName",
			input: `
			{
				"type": "consume",
				"message": {
					"queue_name": "test-queue",
					"consumer_name": ""
				}
			}
			`,
			expectError: ErrorConsumerNameRequired,
		},
		{
			name: "MissingQueueName",
			input: `
			{
				"type": "consume",
				"message": {
					"queue_name": "",
					"consumer_name": "test-name"
				}
			}
			`,
			expectError: ErrorQueueNameRequired,
		},
		{
			name: "InvalidAction",
			input: `
			{
				"type": "invalid",
				"message": {
					"queue_name": "test-name",
					"consumer_name": "test-name"
				}
			}
			`,
			expectError: ErrorInvalidAction,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestInitMessage("consumer", "", "")

			decoder := json.NewDecoder(bytes.NewBufferString(tt.input))

			svc := NewService(nil, nil, nil)
			err := svc.initConnection(msg, decoder, nil)
			assert.True(t, errors.Is(err, tt.expectError))
		})
	}
}

func TestInitConnectionProducerBasic(t *testing.T) {
	msg := createTestInitMessage(Producer, "test-queue", "")

	decoder := json.NewDecoder(bytes.NewBufferString(""))
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	svc := NewService(nil, nil, nil)
	err := svc.initConnection(msg, decoder, encoder)
	assert.NoError(t, err)
}

func TestInitConnectionVariousScenarios(t *testing.T) {
	tests := []struct {
		name        string
		role        types.Role
		input       string
		expectError error
	}{
		{
			name: "ProducerMissingQueueName",
			role: "producer",
			input: `{
				"type": "push",
				"message": {
					"event_type": "test_event",
					"message_payload": "test_payload"
				}
			}`,
			expectError: ErrorQueueNameRequired,
		},
		{
			name:        "ProducerInvalidAction",
			role:        "producer",
			input:       `{"type": "invalid"}`,
			expectError: ErrorInvalidAction,
		},
		{
			name:        "InvalidRole",
			role:        "invalid",
			input:       "",
			expectError: ErrorInvalidRole,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestInitMessage(tt.role, "", "")
			decoder := json.NewDecoder(bytes.NewBufferString(tt.input))

			svc := NewService(nil, nil, nil)
			err := svc.initConnection(msg, decoder, nil)
			assert.True(t, errors.Is(err, tt.expectError))
		})
	}
}

func TestProducerPushMessage(t *testing.T) {
	input := `
	{
		"type": "push",
		"message": {
			"event_type": "test_event",
			"queue_name": "test-queue",
			"message_payload": "payload"
		}
	}
	{
		"type": "push",
		"message": {
			"event_type": "test_event",
			"queue_name": "test-queue-other",
			"message_payload": "payload"
		}
	}
	{
		"type": "push",
		"message": {
			"event_type": "test_event",
			"queue_name": "test-queue",
			"message_payload": "payload"
		}
	}
	`

	decoder := json.NewDecoder(bytes.NewBufferString(input))

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	r, _ := repo.NewSQLiteRepo(":memory:")
	r.RunMigrations()
	b := NewMessageBuffer(1, 100, r)
	cacheInstance := cache.NewMemoryCache()

	svc := NewService(r, cacheInstance, b)

	err := svc.handleProducerMessages(decoder, encoder)
	assert.NoError(t, err)

	for {
		testDec := json.NewDecoder(&output)
		var result map[string]string
		err := testDec.Decode(&result)
		if err != nil {
			if err == io.EOF {
				break
			}
		}

		assert.NoError(t, err)
		assert.Equal(t, map[string]string{
			"response": "pushed message to queue test-queue with type test_event with payload payload",
		}, result)
	}

	time.Sleep(2 * time.Second)

	msg, err := r.GetMessage("test-queue", "test-consumer")
	r.AckMessage("test-queue", "test-consumer", 1)
	assert.NoError(t, err)
	assert.Equal(t, repo.MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: "payload",
	}, msg)

	msg, err = r.GetMessage("test-queue", "test-consumer")
	assert.NoError(t, err)
	assert.Equal(t, repo.MessageModel{
		Id:             3,
		EventType:      "test_event",
		MessagePayload: "payload",
	}, msg)

	msg, err = r.GetMessage("test-queue-other", "test-consumer-other")
	assert.NoError(t, err)
	assert.Equal(t, repo.MessageModel{
		Id:             2,
		EventType:      "test_event",
		MessagePayload: "payload",
	}, msg)
}

func TestConsumerRetrieveMessage(t *testing.T) {
	r, _ := repo.NewSQLiteRepo(":memory:")
	r.RunMigrations()
	b := NewMessageBuffer(2, 1, r)
	cacheInstance := cache.NewMemoryCache()

	r.AddMessage("test_queue", repo.MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: `{"name": "test_event"}`,
	})

	input := `{
		"type": "consume",
		"message": {
			"queue_name": "test_queue",
			"consumer_name": "test_consumer"
		}
	}`
	decoder := json.NewDecoder(bytes.NewBufferString(input))

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	svc := NewService(r, cacheInstance, b)
	svc.handleConsumerMessages(decoder, encoder)

	var result map[string]any
	err := json.Unmarshal(output.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"Id":             float64(1),
		"EventType":      "test_event",
		"MessagePayload": `{"name": "test_event"}`,
	}, result)
}

func TestConsumerNoMessagesAvailable(t *testing.T) {
	r, _ := repo.NewSQLiteRepo(":memory:")
	r.RunMigrations()
	b := NewMessageBuffer(2, 1, r)
	cacheInstance := cache.NewMemoryCache()
	input := `{
		"type": "consume",
		"message": {
			"queue_name": "test_queue",
			"consumer_name": "test_consumer"
		}
	}`
	decoder := json.NewDecoder(bytes.NewBufferString(input))

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	svc := NewService(r, cacheInstance, b)
	svc.handleConsumerMessages(decoder, encoder)

	var result map[string]any
	err := json.Unmarshal(output.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"Id":             float64(0),
		"EventType":      "",
		"MessagePayload": "",
	}, result)
}

func TestConsumerMultipleRequestsWithInsufficientMessages(t *testing.T) {
	r, _ := repo.NewSQLiteRepo(":memory:")
	r.RunMigrations()
	b := NewMessageBuffer(2, 1, r)

	r.AddMessage("test_queue", repo.MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: "my_payload",
	})

	r.AddMessage("test_queue_other", repo.MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: "my_payload",
	})

	input := `
{
  "type": "consume",
  "message": {
    "queue_name": "test_queue",
    "consumer_name": "test_consumer"
  }
}
{
	"type": "ack",
	"message": {
		"queue_name": "test_queue",
		"consumer_name": "test_consumer",
		"message_id": 1
	}
}
{
  "type": "consume",
  "message": {
    "queue_name": "test_queue",
    "consumer_name": "test_consumer"
  }
}
`
	decoder := json.NewDecoder(strings.NewReader(input))

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	cacheInstance := cache.NewMemoryCache()

	svc := NewService(r, cacheInstance, b)
	svc.handleConsumerMessages(decoder, encoder)

	testDec := json.NewDecoder(&output)

	var result1, result2 map[string]any

	err := testDec.Decode(&result1)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"Id":             float64(1),
		"EventType":      "test_event",
		"MessagePayload": "my_payload",
	}, result1)

	err = testDec.Decode(&result2)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"Id":             float64(0),
		"EventType":      "",
		"MessagePayload": "",
	}, result2)
}

func TestPeekMessage(t *testing.T) {
	r, _ := repo.NewSQLiteRepo(":memory:")
	r.RunMigrations()

	r.AddMessage("test_queue", repo.MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: "payload_1",
	})

	r.AddMessage("test_queue", repo.MessageModel{
		Id:             2,
		EventType:      "test_event",
		MessagePayload: "payload_2",
	})

	input := `{
		"type": "peek",
		"message": {
			"queue_name": "test_queue",
			"consumer_name": "test_consumer"
		}
	}`
	decoder := json.NewDecoder(strings.NewReader(input))

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	svc := NewService(r, nil, nil)
	svc.handleConsumerMessages(decoder, encoder)

	var result map[string]any
	err := json.Unmarshal(output.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"message": map[string]any{
			"id":              float64(1),
			"event_type":      "test_event",
			"message_payload": "payload_1",
		},
		"type": "peek",
	}, result)
}

func TestPublishSubscribePattern(t *testing.T) {
	r, _ := repo.NewSQLiteRepo(":memory:")
	r.RunMigrations()

	b := NewMessageBuffer(1, 100, r)
	svc := NewService(r, nil, b)

	eventType := "pubsub-event"

	for i := 1; i <= 3; i++ {
		payload := fmt.Sprintf("pubsub-message-%d", i)
		err := r.AddMessage("pubsub-queue", repo.MessageModel{
			Id:             int64(i), // Set explicit ID
			EventType:      eventType,
			MessagePayload: payload,
		})
		assert.NoError(t, err, "Failed to add message to queue")
	}

	time.Sleep(100 * time.Millisecond)

	var consumer1Output bytes.Buffer
	consumer1Encoder := json.NewEncoder(&consumer1Output)

	consumer1Input := `
	{
		"type": "consume",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer1"
		}
	}
	{
		"type": "ack",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer1",
			"message_id": 1
		}
	}
	{
		"type": "consume",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer1"
		}
	}
	{
		"type": "ack",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer1",
			"message_id": 2
		}
	}
	{
		"type": "consume",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer1"
		}
	}
	{
		"type": "ack",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer1",
			"message_id": 3
		}
	}`

	consumer1Decoder := json.NewDecoder(strings.NewReader(consumer1Input))

	svc.handleConsumerMessages(consumer1Decoder, consumer1Encoder)

	var consumer2Output bytes.Buffer
	consumer2Encoder := json.NewEncoder(&consumer2Output)

	consumer2Input := `
	{
		"type": "consume",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer2"
		}
	}
	{
		"type": "ack",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer2",
			"message_id": 1
		}
	}
	{
		"type": "consume",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer2"
		}
	}
	{
		"type": "ack",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer2",
			"message_id": 2
		}
	}
	{
		"type": "consume",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer2"
		}
	}
	{
		"type": "ack",
		"message": {
			"queue_name": "pubsub-queue",
			"consumer_name": "pubsub-consumer2",
			"message_id": 3
		}
	}
	`

	consumer2Decoder := json.NewDecoder(strings.NewReader(consumer2Input))

	svc.handleConsumerMessages(consumer2Decoder, consumer2Encoder)

	testDec := json.NewDecoder(&consumer2Output)

	var results []map[string]any
	for range 3 {
		var result map[string]any
		err := testDec.Decode(&result)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Failed to decode result: %v", err)
		}
		results = append(results, result)
	}

	assert.Equal(t, 3, len(results), "Expected 3 messages for consumer2")

	sort.Slice(results, func(i, j int) bool {
		return results[i]["Id"].(float64) < results[j]["Id"].(float64)
	})

	for i := range 3 {
		expectedId := float64(i + 1)
		expectedPayload := fmt.Sprintf("pubsub-message-%d", i+1)

		assert.Equal(t, expectedId, results[i]["Id"],
			fmt.Sprintf("Wrong message ID for consumer2 result %d", i))
		assert.Equal(t, eventType, results[i]["EventType"],
			fmt.Sprintf("Wrong event type for consumer2 result %d", i))
		assert.Equal(t, expectedPayload, results[i]["MessagePayload"],
			fmt.Sprintf("Wrong payload for consumer2 result %d", i))
	}

	var extraResult map[string]any
	err := testDec.Decode(&extraResult)
	if err == nil {
		t.Errorf("Unexpected extra message received: %v", extraResult)
	}
}
