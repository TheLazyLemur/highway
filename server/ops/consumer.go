package ops

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/TheLazyLemur/highway/server/repo"
	"github.com/TheLazyLemur/highway/server/types"
)

func handleConsume(
	input types.Message,
	connWriter *json.Encoder,
	dbRepo repo.Repo,
) error {
	data, ok := input.Message.(map[string]any)
	if !ok {
		return ErrorInvalidDataShape
	}
	consumeMessage, err := mapToStruct[types.ConsumeMessage](data)
	if err != nil {
		return err
	}
	if consumeMessage.ConsumerName == "" {
		return ErrorConsumerNameRequired
	}
	if consumeMessage.QueueName == "" {
		return ErrorQueueNameRequired
	}

	var msg repo.MessageModel
	if err := withRetry(5, func() error {
		dbMsg, err := dbRepo.GetMessage(consumeMessage.QueueName, consumeMessage.ConsumerName)
		if err != nil {
			return err
		}
		msg = dbMsg
		return nil
	}); err != nil {
		return err
	}

	err = connWriter.Encode(msg)
	if err != nil {
		return err
	}

	return nil
}

func handleAck(input types.Message, dbRepo repo.Repo) error {
	data, ok := input.Message.(map[string]any)
	if !ok {
		return ErrorInvalidDataShape
	}
	ackMessage, err := mapToStruct[types.AckMessage](data)
	if err != nil {
		return err
	}

	err = withRetry(5, func() error {
		return dbRepo.AckMessage(
			ackMessage.QueueName,
			ackMessage.ConsumerName,
			ackMessage.MessageId,
		)
	})
	return err
}

func handlePeek(msg types.Message, connWriter *json.Encoder, repo repo.Repo) error {
	rawMessage, ok := msg.Message.(map[string]any)
	if !ok {
		slog.Error("Invalid message format for peek operation")
		return ErrorInvalidDataShape
	}

	peekMsg, err := mapToStruct[types.PeekMessage](rawMessage)
	if err != nil {
		return err
	}

	if peekMsg.QueueName == "" {
		return ErrorQueueNameRequired
	}

	if peekMsg.ConsumerName == "" {
		return ErrorConsumerNameRequired
	}

	// Get the message without updating cursor
	message, err := repo.PeekMessage(peekMsg.QueueName, peekMsg.ConsumerName)
	if err != nil {
		return fmt.Errorf("failed to peek message: %w", err)
	}

	// If there's no message, return an empty response
	if message.Id == 0 {
		err = connWriter.Encode(types.Message{
			Action:  Peek,
			Message: nil,
		})
		if err != nil {
			return fmt.Errorf("failed to send empty peek response: %w", err)
		}
		return nil
	}

	// Send the message back to the client
	err = connWriter.Encode(types.Message{
		Action: Peek,
		Message: map[string]any{
			"id":              message.Id,
			"event_type":      message.EventType,
			"message_payload": message.MessagePayload,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send peek response: %w", err)
	}

	return nil
}
