# Highway Cache: In-Memory Key-Value Store

This example demonstrates how to use Highway's built-in key-value store caching functionality, which provides key-value store capabilities for storing and retrieving data.

## Overview

Highway Cache provides a simple, efficient way to cache key-value pairs in memory, with features including:

- Set key-value pairs with optional expiration times
- Get values by key
- Delete specific keys
- Clear all cached data
- Automatic garbage collection of expired items

## Connection Modes

There are three ways to use the cache functionality:

1. Connect as a dedicated cache client
2. Use cache operations from a producer
3. Use cache operations from a consumer

## Basic Usage

### 1. Connect as a Cache Client

```go
// Create a new cache client
client := highway.NewCache("localhost:8080")

// Connect to the Highway server
ctx := context.Background()
if err := client.Connect(ctx); err != nil {
    log.Fatalf("Failed to connect to server: %v", err)
}
defer client.Close()

// Now you can use the client for cache operations
```

### 2. Basic Operations

```go
// Set a value (with 5 minute expiration)
resp, err := client.CacheSet("user:profile:123", `{"name":"John Doe"}`, 5*time.Minute)
if err != nil {
    log.Fatalf("Failed to set cache value: %v", err)
}

// Get a value
resp, err = client.CacheGet("user:profile:123")
if err != nil {
    log.Fatalf("Failed to get cache value: %v", err)
}

// Check if operation was successful and get value
if success, ok := resp["success"].(bool); ok && success {
    if cachedValue, ok := resp["value"].(string); ok {
        fmt.Printf("Retrieved cached value: %s\n", cachedValue)
    }
}

// Delete a key
resp, err = client.CacheDelete("user:profile:123")
if err != nil {
    log.Fatalf("Failed to delete cache key: %v", err)
}

// Clear entire cache
resp, err = client.CacheClear()
if err != nil {
    log.Fatalf("Failed to clear cache: %v", err)
}
```

### 3. Using Cache with Producers

Producers can use the cache to store configuration or other frequently accessed data:

```go
// Create a new producer
producer := highway.NewProducer("localhost:8080")

// Connect to the Highway server
ctx := context.Background()
if err := producer.ConnectAsProducer(ctx); err != nil {
    log.Fatalf("Failed to connect as producer: %v", err)
}
defer producer.Close()

// Cache rate limiting configuration
producer.CacheSet("config:rate-limit", "100", 10*time.Minute)

// Later, use the cached configuration
resp, _ := producer.CacheGet("config:rate-limit")
if success, ok := resp["success"].(bool); ok && success {
    rateLimit := resp["value"].(string)
    // Use rate limit in your producer logic
}
```

## Best Practices

1. **Use namespaced keys**: Prefix keys with a context (e.g., `user:`, `config:`, `session:`) to avoid collisions.
2. **Set appropriate expiration times**: Avoid filling memory with stale data by setting expiration times.
3. **Handle cache misses gracefully**: Always check if the cache operation was successful before using the value.
4. **Don't store sensitive data**: The cache is in-memory and not encrypted.

## Limitations

- The current implementation is in-memory only, so data is lost when the server restarts.
- There is no built-in persistence mechanism.
- Cache operations are not atomic or transaction-based.

## Example

See the `main.go` file in this directory for a complete working example.
