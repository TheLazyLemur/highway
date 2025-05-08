package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/TheLazyLemur/highway"
)

func main() {
	// Get the server address from the environment or use a default
	serverAddr := os.Getenv("HIGHWAY_SERVER")
	if serverAddr == "" {
		serverAddr = "localhost:8080"
	}

	// Example 1: Basic caching operations
	runBasicCacheExample(serverAddr)

	// Example 2: Producer that uses cache
	runProducerWithCacheExample(serverAddr)
}

func runBasicCacheExample(serverAddr string) {
	fmt.Println("=== Running Basic Cache Example ===")

	// Create a new cache client
	client := highway.NewCache(serverAddr)

	// Connect to the Highway server
	ctx := context.Background()
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer client.Close()

	// Set a value in the cache
	key := "test-key"
	value := "Hello, Cache World!"
	expiration := 1 * time.Minute

	resp, err := client.CacheSet(key, value, expiration)
	if err != nil {
		log.Fatalf("Failed to set cache value: %v", err)
	}
	printResponse("CacheSet", resp)

	// Get the value from the cache
	resp, err = client.CacheGet(key)
	if err != nil {
		log.Fatalf("Failed to get cache value: %v", err)
	}
	printResponse("CacheGet", resp)

	// Set another value
	resp, err = client.CacheSet("another-key", "Another value", 5*time.Minute)
	if err != nil {
		log.Fatalf("Failed to set cache value: %v", err)
	}
	printResponse("CacheSet", resp)

	// Delete a key
	resp, err = client.CacheDelete(key)
	if err != nil {
		log.Fatalf("Failed to delete cache key: %v", err)
	}
	printResponse("CacheDelete", resp)

	// Try to get the deleted key
	resp, err = client.CacheGet(key)
	if err != nil {
		log.Fatalf("Failed to get cache value: %v", err)
	}
	printResponse("CacheGet (after delete)", resp)

	// Clear the cache
	resp, err = client.CacheClear()
	if err != nil {
		log.Fatalf("Failed to clear cache: %v", err)
	}
	printResponse("CacheClear", resp)

	fmt.Println("Basic cache operations completed successfully!")
}

func runProducerWithCacheExample(serverAddr string) {
	fmt.Println("\n=== Running Producer with Cache Example ===")

	// Create a new producer
	producer := highway.NewProducer(serverAddr)

	// Connect to the Highway server
	ctx := context.Background()
	if err := producer.ConnectAsProducer(ctx); err != nil {
		log.Fatalf("Failed to connect as producer: %v", err)
	}
	defer producer.Close()

	// First, cache some data that the producer might need
	cacheKey := "config:rate-limit"
	cacheValue := "100"
	expiration := 10 * time.Minute

	resp, err := producer.CacheSet(cacheKey, cacheValue, expiration)
	if err != nil {
		log.Fatalf("Failed to set cache value: %v", err)
	}
	printResponse("Producer CacheSet", resp)

	// Now the producer can use the cached value
	resp, err = producer.CacheGet(cacheKey)
	if err != nil {
		log.Fatalf("Failed to get cache value: %v", err)
	}
	printResponse("Producer CacheGet", resp)

	// Check if the response was successful and contains a value
	if success, ok := resp["success"].(bool); ok && success {
		if cachedValue, ok := resp["value"].(string); ok {
			fmt.Printf("Producer retrieved cached value: %s\n", cachedValue)

			// Use the cached value for something
			rateLimit := cachedValue
			
			// Now push a message to a queue using the cached configuration
			message := map[string]interface{}{
				"data":       "Some payload data",
				"rate_limit": rateLimit,
				"timestamp":  time.Now().UnixNano(),
			}

			pushResp, err := producer.Push("example-queue", "cache-example", message)
			if err != nil {
				log.Fatalf("Failed to push message: %v", err)
			}
			printResponse("Push with cached config", pushResp)
		}
	}

	fmt.Println("Producer with cache operations completed successfully!")
}

func printResponse(operation string, response map[string]any) {
	fmt.Printf("[%s] Response: ", operation)
	jsonResp, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		fmt.Printf("Error formatting response: %v\n", err)
		return
	}
	fmt.Println(string(jsonResp))
}