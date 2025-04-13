package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"highway/server/ops"
	"highway/server/repo"
)

func main() {
	godotenv.Load()

	port := os.Getenv("PORT")
	DbUrl := os.Getenv("DB_URL")

	// Create context for coordinating shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	slog.Info("Server started", "port", port)

	repo, err := repo.NewSQLiteRepo(DbUrl)
	if err != nil {
		log.Fatal(err)
	}

	repo.RunMigrations()
	service := ops.NewService(repo)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	go func() {
		<-sig
		slog.Info("Shutdown signal received, stopping server...")

		cancel()
		ln.Close()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			slog.Info("All connections closed successfully")
		case <-shutdownCtx.Done():
			slog.Info("Shutdown timed out, forcing exit")
		}

		os.Exit(0)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				slog.Info("Stopped accepting new connections")
				wg.Wait() // Wait for ongoing connections to complete
				return
			default:
				slog.Info("Error accepting connection", "error", err.Error())
				continue
			}
		}

		wg.Add(1)
		go func(c net.Conn, ctx context.Context) {
			defer wg.Done()
			_, connCancel := context.WithCancel(ctx)

			go func() {
				<-ctx.Done()
				c.SetDeadline(time.Now().Add(10 * time.Second))
			}()

			defer connCancel()
			service.HandleNewConnection(c)
		}(conn, ctx)
	}
}
