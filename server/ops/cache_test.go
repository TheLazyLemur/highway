package ops

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TheLazyLemur/highway/server/cache"
	"github.com/TheLazyLemur/highway/server/types"
)

func TestCacheHandlers(t *testing.T) {
	// Initialize cache
	memCache := cache.NewMemoryCache()

	// Test that Init sets up cacheInstance correctly
	tests := []struct {
		name     string
		action   types.Action
		setup    func()
		message  any
		validate func(t *testing.T, resp map[string]any)
	}{
		{
			name:   "Set Cache Key",
			action: CacheSet,
			setup:  func() {},
			message: types.CacheSetMessage{
				Key:        "test-key",
				Value:      "test-value",
				Expiration: 1000, // 1 second
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, true, resp["success"])

				// Verify the key was actually set
				value, found := memCache.Get("test-key")
				assert.True(t, found, "Key should be found in cache")
				assert.Equal(t, "test-value", string(value), "Stored value should match")
			},
		},
		{
			name:   "Get Cache Key",
			action: CacheGet,
			setup: func() {
				memCache.Set("test-key", []byte("test-value"), 10*time.Second)
			},
			message: types.CacheGetMessage{
				Key: "test-key",
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, true, resp["success"])
				assert.Equal(t, "test-value", resp["value"])
			},
		},
		{
			name:   "Get Non-existent Cache Key",
			action: CacheGet,
			setup:  func() {},
			message: types.CacheGetMessage{
				Key: "non-existent-key",
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, false, resp["success"])
				assert.Contains(t, resp["error"], "not found")
			},
		},
		{
			name:   "Delete Cache Key",
			action: CacheDelete,
			setup: func() {
			},
			message: types.CacheDeleteMessage{
				Key: "test-key",
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, true, resp["success"])

				// Verify the key was actually deleted
				_, found := memCache.Get("test-key")
				assert.False(t, found, "Key should not be found after delete")
			},
		},
		{
			name:    "Clear Cache",
			action:  CacheClear,
			setup:   func() {},
			message: types.CacheClearMessage{},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, true, resp["success"])
			},
		},
		{
			name:   "Set Cache with Empty Key",
			action: CacheSet,
			setup:  func() {},
			message: types.CacheSetMessage{
				Key:   "",
				Value: "test-value",
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, false, resp["success"])
				assert.Contains(t, resp["error"], "key is required")
			},
		},
		{
			name:   "Set Cache with Empty Value",
			action: CacheSet,
			setup:  func() {},
			message: types.CacheSetMessage{
				Key:   "test-key",
				Value: "",
			},
			validate: func(t *testing.T, resp map[string]any) {
				assert.Equal(t, false, resp["success"])
				assert.Contains(t, resp["error"], "value is required")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			tt.setup()

			// Prepare the message
			msgBytes, err := json.Marshal(tt.message)
			require.NoError(t, err)

			var rawMsg map[string]any
			err = json.Unmarshal(msgBytes, &rawMsg)
			require.NoError(t, err)

			msg := types.Message{
				Action:  tt.action,
				Message: rawMsg,
			}

			// Create a buffer to capture the output
			var output bytes.Buffer
			encoder := json.NewEncoder(&output)

			// Handle the operation
			switch tt.action {
			case CacheSet:
				err = handleCacheSet(memCache, msg, encoder)
			case CacheGet:
				err = handleCacheGet(memCache, msg, encoder)
			case CacheDelete:
				err = handleCacheDelete(memCache, msg, encoder)
			case CacheClear:
				err = handleCacheClear(memCache, msg, encoder)
			}
			require.NoError(t, err, "Handler should not return an error")

			// Parse the response
			var resp map[string]any
			err = json.Unmarshal(output.Bytes(), &resp)
			require.NoError(t, err, "Should be able to parse response")

			// Validate the response
			tt.validate(t, resp)
		})
	}
}

func TestCacheHandlerWithInvalidMessageTypes(t *testing.T) {
	// Initialize cache
	memCache := cache.NewMemoryCache()

	// Invalid message type (not a map)
	invalidMsg := types.Message{
		Action:  CacheSet,
		Message: "not a map",
	}

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	err := handleCacheSet(memCache, invalidMsg, encoder)
	require.Error(t, err)
	assert.Equal(t, ErrorInvalidDataShape, err)

	// Invalid message structure
	invalidStructMsg := types.Message{
		Action: CacheGet,
		Message: map[string]any{
			"not_key": "some value", // Doesn't match CacheGetMessage structure
		},
	}

	output.Reset()
	err = handleCacheGet(memCache, invalidStructMsg, encoder)
	require.NoError(t, err) // Should respond with error in the response, not return error

	// Parse the response
	var resp map[string]any
	err = json.Unmarshal(output.Bytes(), &resp)
	require.NoError(t, err)

	// Validate the response indicates an error
	assert.Equal(t, false, resp["success"])
	assert.Contains(t, resp["error"], "key is required")
}

func TestCacheOperationsWithNilCache(t *testing.T) {
	// Test with nil cache instance
	var nilCache cache.Cache = nil

	// Test Set with nil cache
	setMsg := types.Message{
		Action: CacheSet,
		Message: map[string]any{
			"key":   "test-key",
			"value": "test-value",
		},
	}

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	err := handleCacheSet(nilCache, setMsg, encoder)
	require.NoError(t, err) // Should respond with error in the response, not return error

	var setResp map[string]any
	err = json.Unmarshal(output.Bytes(), &setResp)
	require.NoError(t, err)
	assert.Equal(t, false, setResp["success"])
	assert.Contains(t, setResp["error"], "Cache not initialized")

	// Test Get with nil cache
	getMsg := types.Message{
		Action: CacheGet,
		Message: map[string]any{
			"key": "test-key",
		},
	}

	output.Reset()
	err = handleCacheGet(nilCache, getMsg, encoder)
	require.NoError(t, err)

	var getResp map[string]any
	err = json.Unmarshal(output.Bytes(), &getResp)
	require.NoError(t, err)
	assert.Equal(t, false, getResp["success"])
	assert.Contains(t, getResp["error"], "Cache not initialized")

	// No need to restore anything since we're using local variables
}

func TestCacheMessageHandling(t *testing.T) {
	// Initialize the service with a cache
	memCache := cache.NewMemoryCache()

	// Create a mock service
	service := &Service{
		cache: memCache,
	}

	// Test handleCacheOperation
	cacheSetMsg := types.Message{
		Action: CacheSet,
		Message: map[string]any{
			"key":        "test-key",
			"value":      "test-value",
			"expiration": float64(1000),
		},
	}

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)

	// Test the operation handling
	err := service.handleCacheOperation(cacheSetMsg, encoder)
	require.NoError(t, err)

	var setResp map[string]any
	err = json.Unmarshal(output.Bytes(), &setResp)
	require.NoError(t, err)
	assert.Equal(t, true, setResp["success"])

	// Verify the key was set
	value, found := memCache.Get("test-key")
	assert.True(t, found)
	assert.Equal(t, "test-value", string(value))
}
