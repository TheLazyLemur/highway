package ops

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/TheLazyLemur/highway/server/repo"
	"github.com/TheLazyLemur/highway/server/types"
)

func handlePush(
	msg types.Message,
	connWriter MessageEncoder,
	buffer *MessageBuffer,
) error {
	data, ok := msg.Message.(map[string]any)
	if !ok {
		return ErrorInvalidDataShape
	}
	pushMessage, err := mapToStruct[types.PushMessage](data)
	if err != nil {
		return err
	}
	if pushMessage.QueueName == "" {
		return ErrorQueueNameRequired
	}

	if err := buffer.AddMessage(
		pushMessage.QueueName,
		repo.MessageModel{
			EventType:      pushMessage.EventType,
			MessagePayload: pushMessage.MessagePayload,
		},
	); err != nil {
		return err
	}

	resp := map[string]any{
		"response": fmt.Sprintf(
			"pushed message to queue %s with type %s with payload %s",
			pushMessage.QueueName,
			pushMessage.EventType,
			pushMessage.MessagePayload,
		),
	}
	if err := connWriter.Encode(resp); err != nil {
		if err == io.EOF {
			slog.Error("Connection closed by client")
			return nil
		}
		return err
	}

	return nil
}
