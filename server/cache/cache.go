package cache

import (
	"sync"
	"time"
)

// Cache defines the interface for key-value cache operations
type Cache interface {
	// Get retrieves a value from the cache by key
	Get(key string) ([]byte, bool)
	// Set stores a value in the cache with optional expiration
	Set(key string, value []byte, expiration time.Duration) error
	// Delete removes a value from the cache
	Delete(key string) error
	// Clear empties the entire cache
	Clear() error
}

// Item represents a cache item with expiration
type Item struct {
	Value      []byte
	Expiration int64 // Unix timestamp in nanoseconds
}

// MemoryCache implements an in-memory key-value cache
type MemoryCache struct {
	items map[string]Item
	mu    sync.RWMutex
}

// NewMemoryCache creates a new in-memory cache instance
func NewMemoryCache() *MemoryCache {
	cache := &MemoryCache{
		items: make(map[string]Item),
	}

	// Start the garbage collection routine for expired items
	go cache.startGC()

	return cache
}

// Get retrieves a value from the cache by key
func (c *MemoryCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, found := c.items[key]
	if !found {
		return nil, false
	}

	// Check if the item has expired
	if item.Expiration > 0 && item.Expiration < time.Now().UnixNano() {
		return nil, false
	}

	return item.Value, true
}

// Set stores a value in the cache with optional expiration
func (c *MemoryCache) Set(key string, value []byte, expiration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var exp int64
	if expiration > 0 {
		exp = time.Now().Add(expiration).UnixNano()
	}

	c.items[key] = Item{
		Value:      value,
		Expiration: exp,
	}

	return nil
}

// Delete removes a value from the cache
func (c *MemoryCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, key)
	return nil
}

// Clear empties the entire cache
func (c *MemoryCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]Item)
	return nil
}

// startGC starts a goroutine to periodically clean up expired items
func (c *MemoryCache) startGC() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		<-ticker.C
		c.deleteExpired()
	}
}

// deleteExpired removes all expired items from the cache
func (c *MemoryCache) deleteExpired() {
	now := time.Now().UnixNano()

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.items {
		if v.Expiration > 0 && v.Expiration < now {
			delete(c.items, k)
		}
	}
}