package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"highway/example_client/client"
)

type Person struct {
	Name string `json:"name"`
}

func main() {
	go func() {
		time.Sleep(time.Second * 5)
		cl := client.NewClient("test_queue", "test_consumer")
		err := cl.ConnectAsConsumer()
		if err != nil {
			log.Fatal(err)
		}

		cl.Consume(func(id int64, eventType string, pl string) error {
			var person Person
			err := json.Unmarshal([]byte(pl), &person)
			if err != nil {
				return err
			}
			fmt.Println(person)

			if strings.Contains(pl, "Daniel 5") {
				return errors.New("stop consuming")
			}

			return nil
		})
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
