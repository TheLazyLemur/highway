package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	client "github.com/TheLazyLemur/highway"
)

func main() {
	cl := client.NewProducer("0.0.0.0:8080")
	if err := cl.ConnectAsProducer(context.Background()); err != nil {
		panic(err)
	}
	defer cl.Close()

	count := 1
	for {
		messageType := rand.Intn(3)

		var resp map[string]any
		var err error
		switch messageType {
		case 0:
			resp, err = cl.Push("holborn-events", "login", map[string]any{
				"email":      "danrousseau@example.com",
				"login-time": time.Now().Unix(),
				"session":    fmt.Sprintf("session-%d", count),
			})
		case 1:
			resp, err = cl.Push("holborn-events", "logout", map[string]any{
				"email":       "danrousseau@example.com",
				"logout-time": time.Now().Unix(),
				"session":     fmt.Sprintf("session-%d", count),
				"duration":    rand.Intn(3600), // Random session duration in seconds
			})
		case 2:
			resp, err = cl.Push("holborn-events", "data-update", map[string]any{
				"user":        "danrousseau@example.com",
				"timestamp":   time.Now().Unix(),
				"data_type":   "profile",
				"update_id":   fmt.Sprintf("update-%d", count),
				"change_size": rand.Intn(1000) + 1,
			})
		}
		if err != nil {
			slog.Error("Failed to push message", "error", err.Error())
		}

		fmt.Println("Response from server:", resp)
		count++
		time.Sleep(time.Millisecond * 200)
	}
}
