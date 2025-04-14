package main

import (
	"fmt"
	"log"

	"github.com/TheLazyLemur/highway/client"
)

type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func ConsumerHandler(id int64, eventType string, pl string) error {
	eventData := "\n"
	eventData += fmt.Sprintf("EventType: %s\n", eventType)
	eventData += fmt.Sprintf("Event Payload: %s\n", pl)
	eventData += "\n"

	fmt.Println(eventData)

	// var person Person
	// err := json.Unmarshal([]byte(pl), &person)
	// if err != nil {
	// 	return err
	// }

	// if eventType == "meeting-created" {
	// 	fmt.Println(eventType)
	// 	fmt.Println(pl)
	// } else {
	// 	slog.Info("Event type not recognized", "eventType", eventType)
	// }

	return nil
}

func main() {
	consumerClient := client.NewClient("168.220.87.215:8080", "holborn", "holborn_test_consumer_10")
	err := consumerClient.ConnectAsConsumer()
	if err != nil {
		log.Fatal(err)
	}
	consumerClient.Consume(ConsumerHandler)
	select {}

	// cl := client.NewClient("test_queue", "test_producer")
	// cl.ConnectAsProducer()
	// count := 1
	// for {
	// 	cl.Push("test_event", map[string]any{
	// 		"name": fmt.Sprintf("Daniel %d", count),
	// 		"age":  30,
	// 	})
	// 	count++
	// 	time.Sleep(time.Second * 1)
	// }
}
