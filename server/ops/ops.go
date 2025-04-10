package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

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
	repo *MessageRepo
}

func NewService(repo *MessageRepo) *Service {
	return &Service{
		repo: repo,
	}
}

func (s *Service) HandleNewConnection(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	defer conn.Close()

	var msg types.Message
	err := decoder.Decode(&msg)
	if err != nil {
		slog.Error("Error decoding message", "error", err.Error())
		return
	}

	slog.Info("Received message", "msg", msg)

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
		return errors.New("invalid message type: expected map[string]any")
	}

	initMsg, err := MapToStruct[types.InitMessage](rawMessage)
	if err != nil {
		slog.Error(
			"Error casting to InitMessage",
			"error",
			"message is not of type InitMessage",
		)
		return err
	}

	switch initMsg.Role {
	case "producer":
		if initMsg.QueueName == "" {
			slog.Error("Queue name is required")
			return errors.New("queue name is required")
		}

		if err := s.handlerProducerConnection(initMsg.QueueName, decoder, encoder); err != nil {
			slog.Error("Error in handlerProducer", "error", err.Error())
			return err
		}
	case "consumer":
		if initMsg.Name == "" {
			slog.Error("Consumer name is required")
			return errors.New("consumer name is required")
		}
		if initMsg.QueueName == "" {
			slog.Error("Queue name is required")
			return errors.New("queue name is required")
		}

		if err := s.handlerConsumerConnection(initMsg.QueueName, initMsg.Name, decoder, encoder); err != nil {
			slog.Error("Error in handlerConsumer", "error", err.Error())
			return err
		}
	default:
		slog.Error("Unknown role", "role", initMsg.Role)
		return errors.New("unknown role")
	}

	return nil
}

func (s *Service) handlerProducerConnection(
	queueName string,
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
		case "push":
			pushMessage, err := MapToStruct[types.PushMessage](msg.Message.(map[string]any))
			if err != nil {
				return err
			}
			slog.Info("Received push message", "payload", pushMessage.MessagePayload)

			s.repo.AddMessage(
				MessageModel{
					Id:             s.repo.NextID(),
					EventType:      pushMessage.EventType,
					MessagePayload: pushMessage.MessagePayload,
				},
			)

			resp := map[string]any{
				"response": fmt.Sprintf("pushed message to queue %s", queueName),
			}
			if err := connWriter.Encode(resp); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown message type: %s", msg.Type)
		}
	}
}

func (s *Service) handlerConsumerConnection(
	queueName string,
	consumerName string,
	connReader *json.Decoder,
	connWriter *json.Encoder,
) error {
	var msg types.Message
	err := connReader.Decode(&msg)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}

	return nil
}
