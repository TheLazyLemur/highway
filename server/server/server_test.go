package server

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TheLazyLemur/highway/server/ops"
	"github.com/TheLazyLemur/highway/server/repo"
	"github.com/TheLazyLemur/highway/server/types"
)

func TestHandleConnection_E2E(t *testing.T) {
	testRepo, _ := repo.NewSQLiteRepo(":memory:")
	testRepo.RunMigrations()

	buffer := ops.NewMessageBuffer(1, 1, testRepo)

	service := ops.NewService(testRepo, buffer)

	server := &Server{
		port:    "0",
		service: service,
		wg:      sync.WaitGroup{},
	}

	ctx := t.Context()

	err := server.Start(ctx)
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
