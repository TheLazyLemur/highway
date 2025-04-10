package main

import (
	"log"
	"log/slog"
	"net"

	"highway/server/ops"
)

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}

	repo := ops.NewMessageRepo()
	service := ops.NewService(repo)

	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Info("Error accepting connection", "error", err.Error())
		}

		go service.HandleNewConnection(conn)
	}
}
