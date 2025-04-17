package main

import (
	"fmt"
	"log"
	"sync"
	"time"

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

	return nil
}

func ConsumerHandler2(id int64, eventType string, pl string) error {
	eventData := "\n"
	eventData += fmt.Sprintf("EventType: %s\n", eventType)
	eventData += fmt.Sprintf("Event Payload: %s\n", pl)
	eventData += "\n"

	fmt.Println(eventData)

	return nil
}

func main() {
	consumerClient := client.NewClient(
		"0.0.0.0:8080",
		"holborn",
		"holborn_test_consumer_10",
	)
	err := consumerClient.ConnectAsConsumer()
	if err != nil {
		log.Fatal(err)
	}

	// consumerClient.Consume(ConsumerHandler)

	consumerClient.RegisterHandler("test-event-1", ConsumerHandler)
	consumerClient.RegisterHandler("cool-event", ConsumerHandler2)

	go consumerClient.Run()

	cl := client.NewClient(
		"0.0.0.0:8080",
		"holborn",
		"holborn_test_consumer_10",
	)
	cl.ConnectAsProducer()
	count := 1
	for {

		wg := &sync.WaitGroup{}
		wg.Add(2)

		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			cl.Push("test-event-1", map[string]any{
				"name": fmt.Sprintf("Daniel %d", count),
				"age":  30,
			})
		}(wg)

		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			cl.Push("cool-event", map[string]any{
				"occasion": "Birthday",
				"guest":    "Daniel",
			})
		}(wg)

		wg.Wait()

		count++
		time.Sleep(time.Second * 1)
	}
}
