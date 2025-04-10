package ops

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"highway/server/types"
)

func TestInitConnection_Consumer_Basic(t *testing.T) {
	msg := types.Message{
		Message: map[string]any{
			"role":       "consumer",
			"queue_name": "test-queue",
			"name":       "consumer-1",
		},
	}

	err := initConnection(msg, nil, nil, nil)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestInitConnection_Consumer_MissingName(t *testing.T) {
	msg := types.Message{
		Message: map[string]any{
			"role":       "consumer",
			"queue_name": "test-queue",
			"name":       "",
		},
	}

	err := initConnection(msg, nil, nil, nil)
	if err == nil {
		t.Errorf("Expected error, got %v", err)
	}
}

func TestInitConnection_Consumer_MissingQueueName(t *testing.T) {
	msg := types.Message{
		Message: map[string]any{
			"role":       "consumer",
			"queue_name": "",
			"name":       "test-name",
		},
	}

	err := initConnection(msg, nil, nil, nil)
	if err == nil {
		t.Errorf("Expected error, got %v", err)
	}
}

func TestInitConnection_Producer_Basic(t *testing.T) {
	msg := types.Message{
		Message: map[string]any{
			"role":       "producer",
			"queue_name": "test-queue",
		},
	}

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

	err := initConnection(msg, decoder, encoder, repo)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	expected := map[string]string{
		"response": "pushed message to queue test-queue",
	}

	result := make(map[string]string)
	err = json.Unmarshal(output.Bytes(), &result)
	if err != nil {
		t.Errorf("Error unmarshalling output: %v", err)
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}

	expectedMessageModal := MessageModel{
		Id:             1,
		EventType:      "test_event",
		MessagePayload: "payload",
	}
	if repo.messages[0] != expectedMessageModal {
		t.Errorf("Expected %v, got %v", expectedMessageModal, repo.messages[0])
	}
}

func TestInitConnection_Producer_Missing_QueueName(t *testing.T) {
	msg := types.Message{
		Message: map[string]any{
			"role": "producer",
		},
	}

	input := `{
		"type": "push",
		"message": {
			"event_type": "test_event",
			"message_payload": "test_payload"
		}
	}`
	decoder := json.NewDecoder(bytes.NewBufferString(input))

	err := initConnection(msg, decoder, nil, nil)
	if err == nil {
		t.Errorf("Expected error, got %v", err)
	}
}

func TestInitConnection_Producer_InvalidAction(t *testing.T) {
	msg := types.Message{
		Message: map[string]any{
			"role":       "producer",
			"queue_name": "test-queue",
		},
	}

	input := `{"type": "invalid"}`
	decoder := json.NewDecoder(bytes.NewBufferString(input))

	err := initConnection(msg, decoder, nil, nil)
	if err == nil {
		t.Errorf("Expected error, got %v", err)
	}
}

func TestInitConnection_InvalidRole(t *testing.T) {
	msg := types.Message{
		Message: map[string]any{
			"role":       "invalid",
			"queue_name": "test-queue",
		},
	}

	err := initConnection(msg, nil, nil, nil)
	if err == nil {
		t.Errorf("Expected error, got %v", err)
	}
}
