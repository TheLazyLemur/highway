package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryCacheOperations(t *testing.T) {
	// Create a new cache instance
	cache := NewMemoryCache()

	// Test basic cache operations
	key := "test-key"
	value := "test-value"

	// Set a value
	err := cache.Set(key, []byte(value), 0) // No expiration
	require.NoError(t, err, "Setting cache value should not fail")

	// Get the value
	storedValue, found := cache.Get(key)
	require.True(t, found, "Should find the key in cache")
	assert.Equal(t, value, string(storedValue), "Retrieved value should match stored value")

	// Test delete operation
	err = cache.Delete(key)
	require.NoError(t, err, "Deleting cache value should not fail")
	
	// Verify key is gone
	_, found = cache.Get(key)
	assert.False(t, found, "Key should not be found after deletion")
	
	// Test that clear works
	key1 := "key1"
	key2 := "key2"
	err = cache.Set(key1, []byte("value1"), 0)
	require.NoError(t, err)
	err = cache.Set(key2, []byte("value2"), 0)
	require.NoError(t, err)
	
	err = cache.Clear()
	require.NoError(t, err, "Clearing cache should not fail")
	
	// Verify cache is empty
	_, found1 := cache.Get(key1)
	_, found2 := cache.Get(key2)
	assert.False(t, found1, "Key1 should not be found after clear")
	assert.False(t, found2, "Key2 should not be found after clear")
}

func TestMemoryCacheExpiration(t *testing.T) {
	// Create a new cache instance
	cache := NewMemoryCache()
	
	// Set a key with a very short expiration (10ms)
	key := "expiring-key"
	value := "expiring-value"
	expiration := 10 * time.Millisecond

	// Set the key
	err := cache.Set(key, []byte(value), expiration)
	require.NoError(t, err)

	// Verify it's there immediately
	storedValue, found := cache.Get(key)
	require.True(t, found)
	require.Equal(t, value, string(storedValue))

	// Wait for the key to expire
	time.Sleep(20 * time.Millisecond)

	// Verify it's gone after expiration
	_, found = cache.Get(key)
	assert.False(t, found, "Key should be expired")
}

func TestMemoryCacheGarbageCollection(t *testing.T) {
	// Create a new cache instance
	cache := NewMemoryCache()
	
	// Add several keys with short expiration
	for i := 0; i < 10; i++ {
		key := "gc-test-key-" + string(rune('a'+i))
		cache.Set(key, []byte("value"), 5*time.Millisecond)
	}
	
	// Wait for keys to expire and GC to potentially run
	time.Sleep(100 * time.Millisecond)
	
	// Verify the cache is empty (or at least doesn't contain our keys)
	for i := 0; i < 10; i++ {
		key := "gc-test-key-" + string(rune('a'+i))
		_, found := cache.Get(key)
		assert.False(t, found, "Key should be removed by GC: "+key)
	}
}

func TestMemoryCacheConcurrency(t *testing.T) {
	// Create a new cache instance
	cache := NewMemoryCache()
	
	// Test concurrent reads and writes
	concurrentKey := "concurrent-key"
	cache.Set(concurrentKey, []byte("initial-value"), 0)
	
	// Run multiple goroutines that read and write
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(index int) {
			// Alternate between reads and writes
			if index%2 == 0 {
				value, found := cache.Get(concurrentKey)
				assert.True(t, found)
				assert.NotEmpty(t, value)
			} else {
				err := cache.Set(concurrentKey, []byte("value-"+string(rune('0'+index))), 0)
				assert.NoError(t, err)
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Final verification
	_, found := cache.Get(concurrentKey)
	assert.True(t, found, "Key should still exist after concurrent operations")
}