package main

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"

	"github.com/joho/godotenv"

	"highway/server/ops"
	"highway/server/repo"
)

func main() {
	godotenv.Load()

	port := os.Getenv("PORT")
	DbUrl := os.Getenv("DB_URL")

	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}

	repo, _ := repo.NewSQLiteRepo(DbUrl)
	if err != nil {
		log.Fatal(err)
	}

	repo.RunMigrations()
	service := ops.NewService(repo)

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Info("Error accepting connection", "error", err.Error())
		}

		go service.HandleNewConnection(conn)
	}
}
