package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"highway/example_client/client"
)

type Person struct {
	Name string `json:"name"`
}

func ConsumerHandler() func(id int64, eventType string, pl string) error {
	return func(id int64, eventType string, pl string) error {
		var person Person
		err := json.Unmarshal([]byte(pl), &person)
		if err != nil {
			return err
		}
		fmt.Println(person)

		return nil
	}
}

func main() {
	consumerClient := client.NewClient("test_queue", "test_consumer")
	err := consumerClient.ConnectAsConsumer()
	if err != nil {
		log.Fatal(err)
	}
	consumerClient.Consume(ConsumerHandler())

	cl := client.NewClient("test_queue", "test_producer")
	cl.ConnectAsProducer()
	count := 1
	for {
		x := fmt.Sprintf(
			`{"name": "Daniel %d"}`,
			count,
		)

		cl.Push("test_event", x)
		count++
		time.Sleep(time.Second * 1)
	}
}
