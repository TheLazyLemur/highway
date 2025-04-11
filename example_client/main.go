package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	pl := `
	{
		"type": "init",
		"message": {
			"role": "producer"
		}
	}
	`

	pl2 := `
	{
		"type": "push",
		"message": {
			"event_type": "test_event",
			"queue_name": "test-queue",
			"message_payload": "This is a payload"
		}
	}
	`

	conn.Write([]byte(pl))
	conn.Write([]byte(pl2))

	decoder := json.NewDecoder(conn)
	var obj map[string]any
	err = decoder.Decode(&obj)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Received from server:", obj)
}
