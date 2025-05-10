package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TheLazyLemur/highway/server/cache"
	"github.com/TheLazyLemur/highway/server/ops"
	"github.com/TheLazyLemur/highway/server/repo"
	"github.com/TheLazyLemur/highway/server/types"
)

func TestHandleConnection_E2E(t *testing.T) {
	testRepo, err := repo.NewSQLiteRepo("file::memory:?cache=shared")
	require.NoError(t, err, "Failed to create SQLite repo")

	// Clean up any existing tables in case of previous test runs
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS messages")
	require.NoError(t, err, "Failed to drop messages table")
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS consumer_cursors")
	require.NoError(t, err, "Failed to drop consumer_cursors table")
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS acks")
	require.NoError(t, err, "Failed to drop acks table")

	err = testRepo.RunMigrations()
	require.NoError(t, err, "Failed to run migrations")

	buffer := ops.NewMessageBuffer(1, 1, testRepo)

	service := ops.NewService(testRepo, nil, buffer)

	server := &Server{
		port:    "0",
		service: service,
		wg:      sync.WaitGroup{},
	}

	ctx := t.Context()

	err = server.Start(ctx)
	require.NoError(t, err, "Failed to start server")

	_, portStr, err := net.SplitHostPort(server.listener.Addr().String())
	require.NoError(t, err, "Failed to get port")

	conn, err := net.Dial("tcp", ":"+portStr)
	require.NoError(t, err, "Failed to connect to server")

	encoder := json.NewEncoder(conn)

	initMsg := types.Message{
		Action: ops.Init,
		Message: types.InitMessage{
			Role: ops.Producer,
		},
	}

	err = encoder.Encode(initMsg)
	require.NoError(t, err, "Failed to send init message")

	testData := "test message"
	pushMsg := types.Message{
		Action: ops.Push,
		Message: types.PushMessage{
			EventType:      "test-event",
			QueueName:      "test-queue",
			MessagePayload: testData,
		},
	}

	err = encoder.Encode(pushMsg)
	require.NoError(t, err, "Failed to send push message")

	err = encoder.Encode(pushMsg)
	require.NoError(t, err, "Failed to send push message")

	time.Sleep(500 * time.Millisecond) // Wait for the message to be processed

	consumerConn, err := net.Dial("tcp", ":"+portStr)
	require.NoError(t, err, "Failed to connect consumer to server")

	consumerEncoder := json.NewEncoder(consumerConn)
	consumerDecoder := json.NewDecoder(consumerConn)

	consumerInitMsg := types.Message{
		Action: ops.Init,
		Message: types.InitMessage{
			Role: ops.Consumer,
		},
	}

	err = consumerEncoder.Encode(consumerInitMsg)
	require.NoError(t, err, "Failed to send consumer init message")

	consumeMsg := types.Message{
		Action: ops.Consume,
		Message: types.ConsumeMessage{
			QueueName:    "test-queue",
			ConsumerName: "test-consumer",
		},
	}

	err = consumerEncoder.Encode(consumeMsg)
	require.NoError(t, err, "Failed to send consume message")

	var response map[string]any
	err = consumerDecoder.Decode(&response)
	require.NoError(t, err, "Failed to read response")

	require.Equal(t, map[string]any{
		"Id":             float64(1),
		"EventType":      "test-event",
		"MessagePayload": testData,
	}, response)

	consumerAckMsg := types.Message{
		Action: "ack",
		Message: map[string]any{
			"queue_name":    "test-queue",
			"consumer_name": "test-consumer",
			"message_id":    1,
		},
	}

	err = consumerEncoder.Encode(consumerAckMsg)
	require.NoError(t, err, "Failed to send ack message")

	var response2 map[string]any
	err = consumerEncoder.Encode(consumeMsg)
	require.NoError(t, err, "Failed to send consume message")

	err = consumerDecoder.Decode(&response2)
	require.NoError(t, err, "Failed to read response")
	require.Equal(t, map[string]any{
		"Id":             float64(2),
		"EventType":      "test-event",
		"MessagePayload": testData,
	}, response2)

	consumerConn.Close()
	conn.Close()
	err = server.Stop(ctx)
	require.NoError(t, err, "Failed to stop server")
}

func TestMultipleConsumers_PubSubPattern(t *testing.T) {
	// Test that multiple consumers with different names can access the same messages
	// This demonstrates how Highway can be used for pub/sub-like patterns

	// Use a file-based database for test to ensure shared database across connections
	testRepo, err := repo.NewSQLiteRepo("file::memory:?cache=shared")
	require.NoError(t, err, "Failed to create SQLite repo")

	// Clean up any existing tables in case of previous test runs
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS messages")
	require.NoError(t, err, "Failed to drop messages table")
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS consumer_cursors")
	require.NoError(t, err, "Failed to drop consumer_cursors table")
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS acks")
	require.NoError(t, err, "Failed to drop acks table")

	err = testRepo.RunMigrations()
	require.NoError(t, err, "Failed to run migrations")

	buffer := ops.NewMessageBuffer(1, 1, testRepo)
	service := ops.NewService(testRepo, nil, buffer)

	server := &Server{
		port:    "0",
		service: service,
		wg:      sync.WaitGroup{},
	}

	ctx := t.Context()

	err = server.Start(ctx)
	require.NoError(t, err, "Failed to start server")
	defer func() {
		err := server.Stop(ctx)
		require.NoError(t, err, "Failed to stop server")
	}()

	_, portStr, err := net.SplitHostPort(server.listener.Addr().String())
	require.NoError(t, err, "Failed to get port")

	// Connect as a producer and send messages
	producerConn, err := net.Dial("tcp", ":"+portStr)
	require.NoError(t, err, "Failed to connect producer to server")
	defer producerConn.Close()

	producerEncoder := json.NewEncoder(producerConn)

	// Initialize as producer
	initMsg := types.Message{
		Action: ops.Init,
		Message: types.InitMessage{
			Role: ops.Producer,
		},
	}
	err = producerEncoder.Encode(initMsg)
	require.NoError(t, err, "Failed to send producer init message")

	// Send 3 messages to the queue
	queueName := "pubsub-test-queue"
	for i := 1; i <= 3; i++ {
		payload := fmt.Sprintf("message-%d", i)
		pushMsg := types.Message{
			Action: ops.Push,
			Message: types.PushMessage{
				EventType:      "test-event",
				QueueName:      queueName,
				MessagePayload: payload,
			},
		}
		err = producerEncoder.Encode(pushMsg)
		require.NoError(t, err, fmt.Sprintf("Failed to send push message %d", i))
	}

	// Wait a moment for messages to be processed
	time.Sleep(500 * time.Millisecond)

	// Connect as first consumer
	consumer1Conn, err := net.Dial("tcp", ":"+portStr)
	require.NoError(t, err, "Failed to connect consumer1 to server")
	defer consumer1Conn.Close()

	consumer1Encoder := json.NewEncoder(consumer1Conn)
	consumer1Decoder := json.NewDecoder(consumer1Conn)

	// Initialize as consumer
	consumer1InitMsg := types.Message{
		Action: ops.Init,
		Message: types.InitMessage{
			Role: ops.Consumer,
		},
	}
	err = consumer1Encoder.Encode(consumer1InitMsg)
	require.NoError(t, err, "Failed to send consumer1 init message")

	// Connect as second consumer (with a different name)
	consumer2Conn, err := net.Dial("tcp", ":"+portStr)
	require.NoError(t, err, "Failed to connect consumer2 to server")
	defer consumer2Conn.Close()

	consumer2Encoder := json.NewEncoder(consumer2Conn)
	consumer2Decoder := json.NewDecoder(consumer2Conn)

	// Initialize as consumer
	consumer2InitMsg := types.Message{
		Action: ops.Init,
		Message: types.InitMessage{
			Role: ops.Consumer,
		},
	}
	err = consumer2Encoder.Encode(consumer2InitMsg)
	require.NoError(t, err, "Failed to send consumer2 init message")

	// Consume messages with first consumer
	consume1Msg := types.Message{
		Action: ops.Consume,
		Message: types.ConsumeMessage{
			QueueName:    queueName,
			ConsumerName: "consumer1",
		},
	}

	// Add a small delay to ensure connections are fully established
	time.Sleep(100 * time.Millisecond)

	// Read all 3 messages with first consumer
	for i := range 3 {
		err = consumer1Encoder.Encode(consume1Msg)
		require.NoError(
			t,
			err,
			fmt.Sprintf("Failed to send consume message for consumer1 (%d)", i+1),
		)

		var response map[string]any
		err = consumer1Decoder.Decode(&response)
		require.NoError(t, err, fmt.Sprintf("Failed to read response for consumer1 (%d)", i+1))

		msgId := int(response["Id"].(float64))

		// Verify message ID is a positive number (without relying on specific values)
		require.Greater(
			t,
			msgId,
			0,
			fmt.Sprintf("Message ID should be positive for consumer1 (%d)", i+1),
		)
		require.Equal(
			t,
			"test-event",
			response["EventType"],
			fmt.Sprintf("Wrong event type for consumer1 (%d)", i+1),
		)
		require.Equal(t, fmt.Sprintf("message-%d", i+1), response["MessagePayload"],
			fmt.Sprintf("Wrong payload for consumer1 (%d)", i+1))

		// Acknowledge the message
		ackMsg := types.Message{
			Action: "ack",
			Message: map[string]any{
				"queue_name":    queueName,
				"consumer_name": "consumer1",
				"message_id":    msgId,
			},
		}
		err = consumer1Encoder.Encode(ackMsg)
		require.NoError(t, err, fmt.Sprintf("Failed to send ack for consumer1 (%d)", i+1))

		// Add a small delay between operations to reduce database contention
		time.Sleep(50 * time.Millisecond)
	}

	// Add a delay before starting with the second consumer
	time.Sleep(200 * time.Millisecond)

	// Now read all 3 messages with second consumer
	// This demonstrates pub/sub behavior - multiple consumers can read the same messages
	consume2Msg := types.Message{
		Action: ops.Consume,
		Message: types.ConsumeMessage{
			QueueName:    queueName,
			ConsumerName: "consumer2", // Different consumer name
		},
	}

	for i := range 3 {
		// Add a small delay between each consume operation
		if i > 0 {
			time.Sleep(100 * time.Millisecond)
		}

		err = consumer2Encoder.Encode(consume2Msg)
		require.NoError(
			t,
			err,
			fmt.Sprintf("Failed to send consume message for consumer2 (%d)", i+1),
		)

		var response map[string]any
		err = consumer2Decoder.Decode(&response)
		require.NoError(t, err, fmt.Sprintf("Failed to read response for consumer2 (%d)", i+1))

		msgId := int(response["Id"].(float64))

		// Verify message content - second consumer should get the same messages
		require.Equal(
			t,
			"test-event",
			response["EventType"],
			fmt.Sprintf("Wrong event type for consumer2 (%d)", i+1),
		)
		require.Equal(t, fmt.Sprintf("message-%d", i+1), response["MessagePayload"],
			fmt.Sprintf("Wrong payload for consumer2 (%d)", i+1))

		// Acknowledge the message to properly simulate pub/sub behavior
		ack2Msg := types.Message{
			Action: "ack",
			Message: map[string]any{
				"queue_name":    queueName,
				"consumer_name": "consumer2",
				"message_id":    msgId,
			},
		}
		err = consumer2Encoder.Encode(ack2Msg)
		require.NoError(t, err, fmt.Sprintf("Failed to send ack for consumer2 (%d)", i+1))

		// Add a delay after each acknowledgment to reduce database contention
		time.Sleep(50 * time.Millisecond)
	}

	// Add a final delay before test cleanup
	time.Sleep(100 * time.Millisecond)
}

func TestCacheFunctionality(t *testing.T) {
	// Skip the test if in short mode
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	testRepo, err := repo.NewSQLiteRepo("file::memory:?cache=shared")
	require.NoError(t, err, "Failed to create SQLite repo")

	// Clean up any existing tables in case of previous test runs
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS messages")
	require.NoError(t, err, "Failed to drop messages table")
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS consumer_cursors")
	require.NoError(t, err, "Failed to drop consumer_cursors table")
	_, err = testRepo.GetDB().Exec("DROP TABLE IF EXISTS acks")
	require.NoError(t, err, "Failed to drop acks table")

	err = testRepo.RunMigrations()
	require.NoError(t, err, "Failed to run migrations")
	cacheInstance := cache.NewMemoryCache()
	buffer := ops.NewMessageBuffer(1, 1, testRepo)

	service := ops.NewService(testRepo, cacheInstance, buffer)

	server := &Server{
		port:    "0",
		service: service,
		wg:      sync.WaitGroup{},
	}

	err = server.Start(ctx)
	require.NoError(t, err, "Failed to start server")
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		if err := server.Stop(shutdownCtx); err != nil {
			t.Logf("Error stopping server: %v", err)
		}
	}()

	_, portStr, err := net.SplitHostPort(server.listener.Addr().String())
	require.NoError(t, err, "Failed to get port")

	// Set connection timeouts
	dialer := net.Dialer{
		Timeout: 2 * time.Second,
	}

	// Connect as a producer that will use cache operations
	conn, err := dialer.DialContext(ctx, "tcp", ":"+portStr)
	require.NoError(t, err, "Failed to connect to server")
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	// Set read deadline
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Initialize as producer
	initMsg := types.Message{
		Action: ops.Init,
		Message: types.InitMessage{
			Role: ops.Producer,
		},
	}

	err = encoder.Encode(initMsg)
	require.NoError(t, err, "Failed to send init message")

	// Test cache set operation
	cacheKey := "test-key"
	cacheValue := "test-value"
	cacheExpiration := int64(5000) // 5 seconds in milliseconds

	cacheSetMsg := types.Message{
		Action: ops.CacheSet,
		Message: types.CacheSetMessage{
			Key:        cacheKey,
			Value:      cacheValue,
			Expiration: cacheExpiration,
		},
	}

	err = encoder.Encode(cacheSetMsg)
	require.NoError(t, err, "Failed to send cache set message")

	// Update read deadline
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var setResponse map[string]any
	err = decoder.Decode(&setResponse)
	require.NoError(t, err, "Failed to read set response")
	require.True(t, setResponse["success"].(bool), "Cache set operation failed")

	// Test cache get operation
	cacheGetMsg := types.Message{
		Action: ops.CacheGet,
		Message: types.CacheGetMessage{
			Key: cacheKey,
		},
	}

	err = encoder.Encode(cacheGetMsg)
	require.NoError(t, err, "Failed to send cache get message")

	// Update read deadline
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var getResponse map[string]any
	err = decoder.Decode(&getResponse)
	require.NoError(t, err, "Failed to read get response")

	require.True(t, getResponse["success"].(bool), "Cache get operation failed")
	require.Equal(t, cacheValue, getResponse["value"], "Retrieved value doesn't match set value")

	// Test cache delete operation
	cacheDeleteMsg := types.Message{
		Action: ops.CacheDelete,
		Message: types.CacheDeleteMessage{
			Key: cacheKey,
		},
	}

	err = encoder.Encode(cacheDeleteMsg)
	require.NoError(t, err, "Failed to send cache delete message")

	// Update read deadline
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var deleteResponse map[string]any
	err = decoder.Decode(&deleteResponse)
	require.NoError(t, err, "Failed to read delete response")
	require.True(t, deleteResponse["success"].(bool), "Cache delete operation failed")

	// Verify the key is deleted by trying to get it again
	err = encoder.Encode(cacheGetMsg)
	require.NoError(t, err, "Failed to send cache get message after delete")

	// Update read deadline
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var getAfterDeleteResponse map[string]any
	err = decoder.Decode(&getAfterDeleteResponse)
	require.NoError(t, err, "Failed to read get response after delete")
	require.False(
		t,
		getAfterDeleteResponse["success"].(bool),
		"Cache get operation should fail after delete",
	)
	require.Contains(
		t,
		getAfterDeleteResponse["error"],
		"not found",
		"Error message should indicate key not found",
	)

	// Close producer connection explicitly before creating a new one
	conn.Close()

	// Test connecting specifically as a cache client
	cacheConn, err := dialer.DialContext(ctx, "tcp", ":"+portStr)
	require.NoError(t, err, "Failed to connect cache client to server")
	defer cacheConn.Close()

	cacheEncoder := json.NewEncoder(cacheConn)
	cacheDecoder := json.NewDecoder(cacheConn)

	// Set read deadline for cache connection
	cacheConn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Initialize as cache role
	cacheInitMsg := types.Message{
		Action: ops.Init,
		Message: types.InitMessage{
			Role: ops.Cache, // Use Cache constant
		},
	}

	err = cacheEncoder.Encode(cacheInitMsg)
	require.NoError(t, err, "Failed to send cache client init message")

	// Set a new key
	newKey := "another-key"
	newValue := "another-value"

	anotherSetMsg := types.Message{
		Action: ops.CacheSet,
		Message: types.CacheSetMessage{
			Key:        newKey,
			Value:      newValue,
			Expiration: int64(5000),
		},
	}

	err = cacheEncoder.Encode(anotherSetMsg)
	require.NoError(t, err, "Failed to send another cache set message")

	// Update read deadline
	cacheConn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var anotherSetResponse map[string]any
	err = cacheDecoder.Decode(&anotherSetResponse)
	require.NoError(t, err, "Failed to read another set response")
	require.True(t, anotherSetResponse["success"].(bool), "Another cache set operation failed")

	// Test cache clear operation
	cacheClearMsg := types.Message{
		Action:  ops.CacheClear,
		Message: types.CacheClearMessage{},
	}

	err = cacheEncoder.Encode(cacheClearMsg)
	require.NoError(t, err, "Failed to send cache clear message")

	// Update read deadline
	cacheConn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var clearResponse map[string]any
	err = cacheDecoder.Decode(&clearResponse)
	require.NoError(t, err, "Failed to read clear response")
	require.True(t, clearResponse["success"].(bool), "Cache clear operation failed")

	// Verify the key is cleared by trying to get it
	getNewKeyMsg := types.Message{
		Action: ops.CacheGet,
		Message: types.CacheGetMessage{
			Key: newKey,
		},
	}

	err = cacheEncoder.Encode(getNewKeyMsg)
	require.NoError(t, err, "Failed to send cache get message after clear")

	// Update read deadline
	cacheConn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var getAfterClearResponse map[string]any
	err = cacheDecoder.Decode(&getAfterClearResponse)
	require.NoError(t, err, "Failed to read get response after clear")
	require.False(
		t,
		getAfterClearResponse["success"].(bool),
		"Cache get operation should fail after clear",
	)

	// Close both connections explicitly before stopping the server
	cacheConn.Close()
}
