package ops

import (
	"encoding/json"

	"highway/server/repo"
	"highway/server/types"
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

	msg, err := dbRepo.GetMessage(consumeMessage.QueueName, consumeMessage.ConsumerName)
	if err != nil {
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

	dbRepo.AckMessage(ackMessage.QueueName, ackMessage.ConsumerName, ackMessage.MessageId)

	return nil
}
