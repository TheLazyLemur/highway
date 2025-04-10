package ops

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"highway/server/types"
)

func createTestInitMessage(role, queueName, name string) types.Message {
	return types.Message{
		Message: map[string]any{
			"role":       role,
			"queue_name": queueName,
			"name":       name,
		},
	}
}

func TestInitConnection_Consumer_Basic(t *testing.T) {
	msg := createTestInitMessage("consumer", "test-queue", "consumer-1")
	decoder := json.NewDecoder(bytes.NewBufferString(""))

	err := initConnection(msg, decoder, nil, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestInitConnection_Consumer_InvalidInputs(t *testing.T) {
	tests := []struct {
		name        string
		queueName   string
		nameField   string
		expectError bool
	}{
		{
			name:        "Missing Name",
			queueName:   "test-queue",
			nameField:   "",
			expectError: true,
		},
		{
			name:        "Missing QueueName",
			queueName:   "",
			nameField:   "test-name",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestInitMessage("consumer", tt.queueName, tt.nameField)

			err := initConnection(msg, nil, nil, nil)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestInitConnection_Producer_Basic(t *testing.T) {
	msg := createTestInitMessage("producer", "test-queue", "")

	decoder := json.NewDecoder(bytes.NewBufferString(""))
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	repo := NewMessageRepo()

	err := initConnection(msg, decoder, encoder, repo)
	assert.NoError(t, err)
}

func TestInitConnection(t *testing.T) {
	tests := []struct {
		name        string
		role        string
		queueName   string
		nameField   string
		input       string
		expectError bool
	}{
		{
			name:      "Producer Missing QueueName",
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
			expectError: true,
		},
		{
			name:        "Producer Invalid Action",
			role:        "producer",
			queueName:   "test-queue",
			nameField:   "",
			input:       `{"type": "invalid"}`,
			expectError: true,
		},
		{
			name:        "Invalid Role",
			role:        "invalid",
			queueName:   "test-queue",
			nameField:   "",
			input:       "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestInitMessage(tt.role, tt.queueName, tt.nameField)
			decoder := json.NewDecoder(bytes.NewBufferString(tt.input))

			err := initConnection(msg, decoder, nil, nil)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestProducer_Basic(t *testing.T) {
	input := `{
		"type": "push",
		"message": {
			"event_type": "test_event",
			"message_payload": "payload"
		}
	}`
	decoder := json.NewDecoder(bytes.NewBufferString(input))

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	repo := NewMessageRepo()

	err := handlerProducerConnection("test-queue", decoder, encoder, repo)
	assert.NoError(t, err)

	var result map[string]string
	err = json.Unmarshal(output.Bytes(), &result)
	assert.NoError(t, err)

	assert.Equal(t, map[string]string{
		"response": "pushed message to queue test-queue",
	}, result)

	assert.Equal(t, MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: "payload",
	}, repo.messages[0])
}
