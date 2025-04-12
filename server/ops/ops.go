package ops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"strings"

	"highway/server/repo"
	"highway/server/types"
)

func mapToStruct[T any](data map[string]any) (T, error) {
	b, err := json.Marshal(data)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("failed to marshal data: %w", err)
	}

	var result T
	err = json.Unmarshal(b, &result)
	if err != nil {
		return result, fmt.Errorf("failed to unmarshal data into %T: %w", result, err)
	}
	return result, nil
}

func getRawMessage(connReader *json.Decoder) (types.Message, error) {
	var msg types.Message
	err := connReader.Decode(&msg)
	if err != nil {
		if err == io.EOF {
			slog.Error("Connection closed by client")
			return types.Message{}, ErrorConnectionClosed
		}
		return types.Message{}, err
	}

	return msg, nil
}

type Service struct {
	repo repo.Repo
}

func NewService(repo repo.Repo) *Service {
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
			slog.Error("Stack trace", "stack", fmt.Sprintf("%+v", debug.Stack()))
		}
		conn.Close()
	}()

	msg, err := getRawMessage(decoder)
	if err != nil {
		slog.Error("Error reading message", "error", err.Error())
		return
	}

	switch msg.Action {
	case Init:
		if err := s.initConnection(msg, decoder, encoder); err != nil {
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "connection reset by peer") {
				slog.Error("Connection closed by client")
				return
			}
			slog.Error("Error in initConnection", "error", err.Error())
			return
		}

	default:
		slog.Error("Expected InitMessage, got", "type", msg.Action)
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

	initMsg, err := mapToStruct[types.InitMessage](rawMessage)
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
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "connection reset by peer") {
				slog.Error("Connection closed by client")
				return nil
			}
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
		msg, err := getRawMessage(connReader)
		if err != nil {
			if errors.Is(err, ErrorConnectionClosed) {
				return nil
			}
			return err
		}

		switch msg.Action {
		case Push:
			if err := handlePush(msg, connReader, connWriter, s.repo); err != nil {
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
		msg, err := getRawMessage(connReader)
		if err != nil {
			if errors.Is(err, ErrorConnectionClosed) {
				return nil
			}
			return err
		}

		switch msg.Action {
		case Consume:
			if err := handleConsume(msg, connReader, connWriter, s.repo); err != nil {
				return err
			}
		case Ack:
			if err := handleAck(msg, s.repo); err != nil {
				return err
			}
		default:
			return ErrorInvalidAction
		}
	}
}
