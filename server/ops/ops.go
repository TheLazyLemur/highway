package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"highway/server/repo"
	"highway/server/types"
)

func MapToStruct[T any](data map[string]any) (T, error) {
	b, err := json.Marshal(data)
	if err != nil {
		var zero T
		return zero, err
	}

	var result T
	err = json.Unmarshal(b, &result)
	return result, err
}

type Service struct {
	repo *repo.MessageRepo
}

func NewService(repo *repo.MessageRepo) *Service {
	return &Service{
		repo: repo,
	}
}

func (s *Service) HandleNewConnection(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Recovered from panic in connection", "error", r)
		}
		conn.Close()
	}()

	var msg types.Message
	err := decoder.Decode(&msg)
	if err != nil {
		slog.Error("Error decoding message", "error", err.Error())
		return
	}

	switch msg.Type {
	case "init":
		if err := s.initConnection(msg, decoder, encoder); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			slog.Error("Error in initConnection", "error", err.Error())
			return
		}

	default:
		slog.Error("Expected InitMessage, got", "type", msg.Type)
		return
	}
}

func (s *Service) initConnection(
	msg types.Message,
	decoder *json.Decoder,
	encoder *json.Encoder,
) error {
	rawMessage, ok := msg.Message.(map[string]any)
	if !ok {
		slog.Error("Expected 'msg.Message' to be a map", "got", fmt.Sprintf("%T", msg.Message))
		return ErrorInvalidMessageType
	}

	initMsg, err := MapToStruct[types.InitMessage](rawMessage)
	if err != nil {
		slog.Error(
			"Error casting to InitMessage",
			"error",
			err.Error(),
		)
		return err
	}

	switch initMsg.Role {
	case Producer:
		if err := s.handleProducerMessages(decoder, encoder); err != nil {
			slog.Error("Error in handlerProducer", "error", err.Error())
			return err
		}
	case Consumer:
		if err := s.handleConsumerMessages(decoder, encoder); err != nil {
			slog.Error("Error in handlerConsumer", "error", err.Error())
			return err
		}
	default:
		slog.Error("Unknown role", "role", initMsg.Role)
		return ErrorInvalidRole
	}

	return nil
}

func (s *Service) handleProducerMessages(
	connReader *json.Decoder,
	connWriter *json.Encoder,
) error {
	for {
		var msg types.Message
		err := connReader.Decode(&msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		switch msg.Type {
		case Push:
			pushMessage, err := MapToStruct[types.PushMessage](msg.Message.(map[string]any))
			if err != nil {
				return err
			}
			if pushMessage.QueueName == "" {
				return ErrorQueueNameRequired
			}

			s.repo.AddMessage(
				pushMessage.QueueName,
				repo.MessageModel{
					EventType:      pushMessage.EventType,
					MessagePayload: pushMessage.MessagePayload,
				},
			)

			resp := map[string]any{
				"response": fmt.Sprintf("pushed message to queue %s", pushMessage.QueueName),
			}
			if err := connWriter.Encode(resp); err != nil {
				return err
			}
		default:
			return ErrorInvalidAction
		}
	}
}

func (s *Service) handleConsumerMessages(
	connReader *json.Decoder,
	connWriter *json.Encoder,
) error {
	for {
		var msg types.Message
		err := connReader.Decode(&msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		switch msg.Type {
		case Consume:
			consumeMessage, err := MapToStruct[types.ConsumeMessage](msg.Message.(map[string]any))
			if err != nil {
				return err
			}
			if consumeMessage.ConsumerName == "" {
				return ErrorConsumerNameRequired
			}
			if consumeMessage.QueueName == "" {
				return ErrorQueueNameRequired
			}

			msg, err := s.repo.GetMessage(consumeMessage.QueueName, consumeMessage.ConsumerName)
			if err != nil {
				return err
			}

			err = connWriter.Encode(msg)
			if err != nil {
				return err
			}
		default:
			return ErrorInvalidAction
		}
	}
}
