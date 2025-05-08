package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/TheLazyLemur/highway/server/cache"
	"github.com/TheLazyLemur/highway/server/ops"
	"github.com/TheLazyLemur/highway/server/repo"
	"github.com/TheLazyLemur/highway/server/server"
)

func main() {
	godotenv.Load()

	port := os.Getenv("PORT")
	DbUrl := os.Getenv("DB_URL")
	slog.Info("env", "port", port, "db_url", DbUrl)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repository, err := repo.NewSQLiteRepo(DbUrl)
	if err != nil {
		slog.Error("Failed to initialize repository", "error", err)
		os.Exit(1)
	}

	if err := repository.RunMigrations(); err != nil {
		slog.Error("Failed to run migrations", "error", err)
		os.Exit(1)
	}

	buffer := ops.NewMessageBuffer(100, 100, repository)

	cacheInstance := cache.NewMemoryCache()
	service := ops.NewService(repository, cacheInstance, buffer)

	server := server.NewServer(port, service)
	if err := server.Start(ctx); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	slog.Info("Shutdown signal received, stopping server...")

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Stop(shutdownCtx); err != nil {
		slog.Error("Error during server shutdown", "error", err)
		os.Exit(1)
	}

	slog.Info("Server stopped gracefully")
}
