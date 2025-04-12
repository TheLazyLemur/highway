package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

func main() {
	go func() {
		time.Sleep(time.Second * 5)
		consumer()
	}()

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	pl := map[string]any{
		"type": "init",
		"message": map[string]any{
			"role": "producer",
		},
	}

	pl2 := map[string]any{
		"type": "push",
		"message": map[string]any{
			"event_type":      "test_event",
			"queue_name":      "test_queue",
			"message_payload": `{"name": "Daniel"}`,
		},
	}

	encoder := json.NewEncoder(conn)

	encoder.Encode(pl)
	count := 1
	for {
		pl2["message"].(map[string]any)["message_payload"] = fmt.Sprintf(
			`{"name": "Daniel %d"}`,
			count,
		)
		encoder.Encode(pl2)
		count++
		time.Sleep(time.Second * 1)
	}
}

type Person struct {
	Name string `json:"name"`
}

func consumer() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	pl := map[string]any{
		"type": "init",
		"message": map[string]any{
			"role": "consumer",
		},
	}

	pl2 := map[string]any{
		"type": "consume",
		"message": map[string]any{
			"queue_name":    "test_queue",
			"consumer_name": "test_consumer",
		},
	}

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	encoder.Encode(pl)

	for {
		encoder.Encode(pl2)

		var rawData map[string]any
		decoder.Decode(&rawData)

		eventType := rawData["EventType"].(string)
		data := rawData["MessagePayload"].(string)
		eventID := int64(rawData["Id"].(float64))
		if data == "" || eventType == "" {
			continue
		}

		fmt.Printf("Event Type: %s\n", eventType)
		fmt.Printf("Event ID: %d\n", eventID)
		fmt.Printf("Data: %s\n", data)

		var person Person
		json.Unmarshal([]byte(data), &person)
		fmt.Println(person)

		ack := map[string]any{
			"type": "ack",
			"message": map[string]any{
				"message_id":    eventID,
				"consumer_name": "test_consumer",
				"queue_name":    "test_queue",
			},
		}

		encoder.Encode(ack)

		time.Sleep(time.Second * 5)
	}
}
