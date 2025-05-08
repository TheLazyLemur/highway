package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	client "github.com/TheLazyLemur/highway"
)

type LoginEvent struct {
	Email     string `json:"email"`
	LoginTime int64  `json:"login-time"`
	Session   string `json:"session"`
}

type LogoutEvent struct {
	Email     string `json:"email"`
	LoginTime int64  `json:"login-time"`
	Session   string `json:"session"`
	Duration  int64  `json:"duration"`
}

type DataUpdateEvent struct {
	User       string `json:"user"`
	Timestamp  int64  `json:"timestamp"`
	DataType   string `json:"data_type"`
	UpdateID   string `json:"update_id"`
	ChangeSize int64  `json:"change_size"`
}

func TransformerHandler[T any](
	handler func(id int64, eventType string, params T) error,
) func(id int64, eventType string, pl string) error {
	return func(id int64, eventType string, pl string) error {
		var params T
		// Attempt to unmarshal the JSON payload into the params variable
		err := json.Unmarshal([]byte(pl), &params)
		if err != nil {
			// Log the error for debugging purposes
			fmt.Printf("Failed to unmarshal payload: %v, error: %v\n", pl, err)
			return fmt.Errorf("invalid payload format: %w", err)
		}
		// Call the provided handler with the unmarshalled params
		err = handler(id, eventType, params)
		if err != nil {
			// Log the error from the handler
			fmt.Printf("Handler error for event %s: %v\n", eventType, err)
			return fmt.Errorf("handler error: %w", err)
		}
		return nil
	}
}

func LoginEventHandler(db *sql.DB) func(id int64, eventType string, loginEvent LoginEvent) error {
	return func(id int64, eventType string, loginEvent LoginEvent) error {
		// fmt.Println("Login event received:", loginEvent)
		fmt.Println(id)
		return nil
	}
}

func LogoutEventHandler(
	db *sql.DB,
) func(id int64, eventType string, logoutEvent LogoutEvent) error {
	return func(id int64, eventType string, logoutEvent LogoutEvent) error {
		// fmt.Println("Logout event received:", logoutEvent)
		fmt.Println(id)
		return nil
	}
}

func DataUpdateEventHandler(
	db *sql.DB,
) func(id int64, eventType string, dataUpdateEvent DataUpdateEvent) error {
	return func(id int64, eventType string, dataUpdateEvent DataUpdateEvent) error {
		// fmt.Println("Data update event received:", dataUpdateEvent)
		fmt.Println(id)
		return nil
	}
}

func main() {
	consumerClient := client.NewConsumer(
		"0.0.0.0:8080",
		"holborn_test_consumer_asdasd",
	)
	if err := consumerClient.ConnectAsConsumer(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer consumerClient.Close()

	consumerClient.RegisterHandler("login", TransformerHandler(LoginEventHandler(nil)))
	consumerClient.RegisterHandler("logout", TransformerHandler(LogoutEventHandler(nil)))
	consumerClient.RegisterHandler("data-update", TransformerHandler(DataUpdateEventHandler(nil)))

	log.Fatal(consumerClient.Run("holborn-events"))
}
