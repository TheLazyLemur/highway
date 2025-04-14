package ops

import (
	"encoding/json"

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
