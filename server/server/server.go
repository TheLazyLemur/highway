package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/TheLazyLemur/highway/server/ops"
)

type Server struct {
	port     string
	service  *ops.Service
	listener net.Listener
	wg       sync.WaitGroup
}

func NewServer(port string, service *ops.Service) *Server {
	return &Server{
		port:    port,
		service: service,
		wg:      sync.WaitGroup{},
	}
}

func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", s.port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	slog.Info("Server started", "port", s.port)

	s.listener = ln

	s.wg.Add(1)
	go s.acceptConnections(ctx, ln)

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if err := s.listener.Close(); err != nil {
		return fmt.Errorf("error closing listener: %w", err)
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) acceptConnections(ctx context.Context, ln net.Listener) {
	defer s.wg.Done()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				slog.Info("Stopped accepting new connections")
				return
			default:
				slog.Info("Error accepting connection", "error", err.Error())
				return
			}
		}

		s.wg.Add(1)
		go s.handleConnection(ctx, conn)
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-connCtx.Done()
		conn.SetDeadline(time.Now().Add(10 * time.Second))
	}()

	s.service.HandleNewConnection(conn)
}
