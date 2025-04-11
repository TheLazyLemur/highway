package ops

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"highway/server/repo"
	"highway/server/types"
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

func TestInitConnectionConsumerBasic(t *testing.T) {
	msg := createTestInitMessage("consumer", "test-queue", "consumer-1")
	decoder := json.NewDecoder(bytes.NewBufferString(""))

	svc := NewService(nil)
	err := svc.initConnection(msg, decoder, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestInitConnectionConsumerInvalidInputs(t *testing.T) {
	tests := []struct {
		name        string
		queueName   string
		nameField   string
		expectError error
	}{
		{
			name:        "MissingConsumerName",
			queueName:   "test-queue",
			nameField:   "",
			expectError: ErrorConsumerNameRequired,
		},
		{
			name:        "MissingQueueName",
			queueName:   "",
			nameField:   "test-name",
			expectError: ErrorQueueNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestInitMessage("consumer", tt.queueName, tt.nameField)

			input := fmt.Sprintf(`{
	"type": "consume",
	"message": {
		"queue_name": "%s",
		"consumer_name": "%s"
	}
}`, tt.queueName, tt.nameField)
			decoder := json.NewDecoder(bytes.NewBufferString(input))

			svc := NewService(nil)
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

	svc := NewService(nil)
	err := svc.initConnection(msg, decoder, encoder)
	assert.NoError(t, err)
}

func TestInitConnectionVariousScenarios(t *testing.T) {
	tests := []struct {
		name        string
		role        types.Role
		queueName   string
		nameField   string
		input       string
		expectError error
	}{
		{
			name:      "ProducerMissingQueueName",
			role:      "producer",
			queueName: "",
			nameField: "",
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
			queueName:   "test-queue",
			nameField:   "",
			input:       `{"type": "invalid"}`,
			expectError: ErrorInvalidAction,
		},
		{
			name:        "InvalidRole",
			role:        "invalid",
			queueName:   "test-queue",
			nameField:   "",
			input:       "",
			expectError: ErrorInvalidRole,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestInitMessage(tt.role, tt.queueName, tt.nameField)
			decoder := json.NewDecoder(bytes.NewBufferString(tt.input))

			svc := NewService(nil)
			err := svc.initConnection(msg, decoder, nil)
			assert.True(t, errors.Is(err, tt.expectError))
		})
	}
}

func TestProducerPushMessage(t *testing.T) {
	input := `{
		"type": "push",
		"message": {
			"event_type": "test_event",
			"queue_name": "test-queue",
			"message_payload": "payload"
		}
	}`
	decoder := json.NewDecoder(bytes.NewBufferString(input))

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	r := repo.NewMessageRepo()
	svc := NewService(r)

	err := svc.handleProducerMessages(decoder, encoder)
	assert.NoError(t, err)

	var result map[string]string
	err = json.Unmarshal(output.Bytes(), &result)
	assert.NoError(t, err)

	assert.Equal(t, map[string]string{
		"response": "pushed message to queue test-queue",
	}, result)

	msg, err := r.GetMessage("test-queue", "test-consumer")
	assert.NoError(t, err)
	assert.Equal(t, repo.MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: "payload",
	}, msg)
}

func TestConsumerRetrieveMessage(t *testing.T) {
	r := repo.NewMessageRepo()
	r.AddMessage("test_queue", repo.MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: "my_payload",
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

	svc := NewService(r)
	svc.handleConsumerMessages(decoder, encoder)

	var result map[string]any
	err := json.Unmarshal(output.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"Id":             float64(1),
		"EventType":      "test_event",
		"MessagePayload": "my_payload",
	}, result)
}

func TestConsumerNoMessagesAvailable(t *testing.T) {
	r := repo.NewMessageRepo()
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

	svc := NewService(r)
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
	r := repo.NewMessageRepo()
	r.AddMessage("test_queue", repo.MessageModel{
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

	svc := NewService(r)
	svc.handleConsumerMessages(decoder, encoder)

	dec := json.NewDecoder(&output)

	var result1, result2 map[string]any

	err := dec.Decode(&result1)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"Id":             float64(1),
		"EventType":      "test_event",
		"MessagePayload": "my_payload",
	}, result1)

	err = dec.Decode(&result2)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"Id":             float64(0),
		"EventType":      "",
		"MessagePayload": "",
	}, result2)
}
