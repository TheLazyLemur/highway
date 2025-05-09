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

	"github.com/TheLazyLemur/highway/server/cache"
	"github.com/TheLazyLemur/highway/server/repo"
	"github.com/TheLazyLemur/highway/server/types"
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
		return result, fmt.Errorf("failed to unmarshal into %T: %w", result, err)
	}
	return result, nil
}

func getRawMessage(connReader MessageDecoder) (types.Message, error) {
	var msg types.Message
	err := connReader.Decode(&msg)
	if err != nil {
		if err == io.EOF {
			slog.Error("Connection closed unexpectedly by client", "cause", "EOF")
			return types.Message{}, ErrorConnectionClosed
		}
		slog.Error("Failed to decode message", "error", err)
		return types.Message{}, fmt.Errorf("failed to decode message: %w", err)
	}

	slog.Info("Successfully decoded message", "message", msg)
	return msg, nil
}

type Service struct {
	repo   repo.Repo
	buffer *MessageBuffer
	cache  cache.Cache
}

func NewService(repo repo.Repo, cacheInstance *cache.MemoryCache, buffer *MessageBuffer) *Service {
	return &Service{
		repo:   repo,
		buffer: buffer,
		cache:  cacheInstance,
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
		slog.Error("Failed to decode incoming message", "error", err.Error())
		return
	}

	switch msg.Action {
	case Init:
		if err := s.initConnection(msg, decoder, encoder); err != nil {
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "connection reset by peer") {
				slog.Error("Client terminated connection during initialization")
				return
			}
			slog.Error("Failed to initialize client connection", "error", err.Error())
			return
		}

	default:
		slog.Error("Invalid initial message action", "expected", "Init", "received", msg.Action)
		return
	}
}

func (s *Service) initConnection(
	msg types.Message,
	decoder MessageDecoder,
	encoder MessageEncoder,
) error {
	rawMessage, ok := msg.Message.(map[string]any)
	if !ok {
		slog.Error(
			"Message format error",
			"expected",
			"map[string]any",
			"got",
			fmt.Sprintf("%T", msg.Message),
		)
		return ErrorInvalidMessageType
	}

	initMsg, err := mapToStruct[types.InitMessage](rawMessage)
	if err != nil {
		slog.Error(
			"Failed to parse initialization message",
			"error",
			err.Error(),
		)
		return err
	}

	switch initMsg.Role {
	case Producer:
		if err := s.handleProducerMessages(decoder, encoder); err != nil {
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "connection reset by peer") {
				slog.Error("Producer connection terminated by client")
				return nil
			}
			slog.Error("Producer message handling failed", "error", err.Error())
			return err
		}
	case Consumer:
		if err := s.handleConsumerMessages(decoder, encoder); err != nil {
			slog.Error("Consumer message handling failed", "error", err.Error())
			return err
		}
	case Cache:
		if err := s.handleCacheMessages(decoder, encoder); err != nil {
			slog.Error("Cache message handling failed", "error", err.Error())
			return err
		}
	default:
		slog.Error(
			"Client specified invalid role in initialization",
			"role",
			initMsg.Role,
			"valid_roles",
			"Producer,Consumer,Cache",
		)
		return ErrorInvalidRole
	}

	return nil
}

func (s *Service) handleProducerMessages(
	connReader MessageDecoder,
	connWriter MessageEncoder,
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
			if err := handlePush(msg, connWriter, s.buffer); err != nil {
				return err
			}
		case CacheSet, CacheGet, CacheDelete, CacheClear:
			if err := s.handleCacheOperation(msg, connWriter); err != nil {
				return err
			}
		default:
			return ErrorInvalidAction
		}
	}
}

// handleCacheOperation dispatches to the appropriate cache handler based on the message action
func (s *Service) handleCacheOperation(msg types.Message, connWriter MessageEncoder) error {
	switch msg.Action {
	case CacheSet:
		return handleCacheSet(s.cache, msg, connWriter)
	case CacheGet:
		return handleCacheGet(s.cache, msg, connWriter)
	case CacheDelete:
		return handleCacheDelete(s.cache, msg, connWriter)
	case CacheClear:
		return handleCacheClear(s.cache, msg, connWriter)
	default:
		return ErrorInvalidAction
	}
}

// handleCacheMessages handles messages for clients that connect specifically as cache clients
func (s *Service) handleCacheMessages(
	connReader MessageDecoder,
	connWriter MessageEncoder,
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
		case CacheSet, CacheGet, CacheDelete, CacheClear:
			if err := s.handleCacheOperation(msg, connWriter); err != nil {
				slog.Error("Failed to process cache operation", "error", err.Error())
				return err
			}
		default:
			return ErrorInvalidAction
		}
	}
}

func (s *Service) handleConsumerMessages(
	connReader MessageDecoder,
	connWriter MessageEncoder,
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
			if err := handleConsume(msg, connWriter, s.repo); err != nil {
				slog.Error("Failed to process consumption request", "error", err.Error())
				return err
			}
		case Ack:
			if err := handleAck(msg, s.repo); err != nil {
				slog.Error("Failed to acknowledge message", "error", err.Error())
				return err
			}
		case Peek:
			if err := handlePeek(msg, connWriter, s.repo); err != nil {
				slog.Error("Failed to process peek request", "error", err.Error())
				return err
			}
		case CacheSet, CacheGet, CacheDelete, CacheClear:
			if err := s.handleCacheOperation(msg, connWriter); err != nil {
				return err
			}
		default:
			return ErrorInvalidAction
		}
	}
}
