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
			"role": "producer",
			"queue_name": "test_queue"
		}
	}
	`

	conn.Write([]byte(pl))

	pl2 := `
	{
		"type": "push",
		"message": {
			"message_payload": "hello"
		}
	}
	`

	conn.Write([]byte(pl2))

	decoder := json.NewDecoder(conn)
	var obj map[string]any
	err = decoder.Decode(&obj)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Received from server:", obj)
}
