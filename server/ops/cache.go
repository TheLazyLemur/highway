package ops

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/TheLazyLemur/highway/server/cache"
	"github.com/TheLazyLemur/highway/server/types"
)

func handleCacheSet(cacheInst cache.Cache, msg types.Message, connWriter *json.Encoder) error {
	if cacheInst == nil {
		slog.Error("Cache not initialized")
		return respondWithError(connWriter, "Cache not initialized")
	}

	rawMessage, ok := msg.Message.(map[string]any)
	if !ok {
		slog.Error("Invalid message format")
		return ErrorInvalidDataShape
	}

	cacheMsg, err := mapToStruct[types.CacheSetMessage](rawMessage)
	if err != nil {
		slog.Error("Failed to parse cache set message", "error", err.Error())
		return respondWithError(connWriter, "Invalid message format")
	}

	if cacheMsg.Key == "" {
		slog.Error("Cache key is required")
		return respondWithError(connWriter, ErrorCacheKeyRequired.Error())
	}

	if cacheMsg.Value == "" {
		slog.Error("Cache value is required")
		return respondWithError(connWriter, ErrorCacheValueRequired.Error())
	}

	// Set the value in the cache
	var expiration time.Duration
	if cacheMsg.Expiration > 0 {
		expiration = time.Duration(cacheMsg.Expiration) * time.Millisecond
	}

	err = cacheInst.Set(cacheMsg.Key, []byte(cacheMsg.Value), expiration)
	if err != nil {
		slog.Error("Failed to set cache value", "error", err.Error())
		return respondWithError(connWriter, "Failed to set cache value")
	}

	response := types.CacheResponse{
		Success: true,
	}

	return connWriter.Encode(response)
}

func handleCacheGet(cacheInst cache.Cache, msg types.Message, connWriter *json.Encoder) error {
	if cacheInst == nil {
		slog.Error("Cache not initialized")
		return respondWithError(connWriter, "Cache not initialized")
	}

	rawMessage, ok := msg.Message.(map[string]any)
	if !ok {
		slog.Error("Invalid message format")
		return ErrorInvalidDataShape
	}

	cacheMsg, err := mapToStruct[types.CacheGetMessage](rawMessage)
	if err != nil {
		slog.Error("Failed to parse cache get message", "error", err.Error())
		return respondWithError(connWriter, "Invalid message format")
	}

	if cacheMsg.Key == "" {
		slog.Error("Cache key is required")
		return respondWithError(connWriter, ErrorCacheKeyRequired.Error())
	}

	// Get the value from the cache
	value, found := cacheInst.Get(cacheMsg.Key)
	if !found {
		return respondWithError(connWriter, ErrorCacheKeyNotFound.Error())
	}

	response := types.CacheResponse{
		Success: true,
		Value:   string(value),
	}

	return connWriter.Encode(response)
}

func handleCacheDelete(cacheInst cache.Cache, msg types.Message, connWriter *json.Encoder) error {
	if cacheInst == nil {
		slog.Error("Cache not initialized")
		return respondWithError(connWriter, "Cache not initialized")
	}

	rawMessage, ok := msg.Message.(map[string]any)
	if !ok {
		slog.Error("Invalid message format")
		return ErrorInvalidDataShape
	}

	cacheMsg, err := mapToStruct[types.CacheDeleteMessage](rawMessage)
	if err != nil {
		slog.Error("Failed to parse cache delete message", "error", err.Error())
		return respondWithError(connWriter, "Invalid message format")
	}

	if cacheMsg.Key == "" {
		slog.Error("Cache key is required")
		return respondWithError(connWriter, ErrorCacheKeyRequired.Error())
	}

	// Delete the key from the cache
	err = cacheInst.Delete(cacheMsg.Key)
	if err != nil {
		slog.Error("Failed to delete cache key", "error", err.Error())
		return respondWithError(connWriter, "Failed to delete cache key")
	}

	response := types.CacheResponse{
		Success: true,
	}

	return connWriter.Encode(response)
}

// handleCacheClear handles a request to clear the entire cache
func handleCacheClear(cacheInst cache.Cache, _ types.Message, connWriter *json.Encoder) error {
	if cacheInst == nil {
		slog.Error("Cache not initialized")
		return respondWithError(connWriter, "Cache not initialized")
	}

	// Clear the entire cache
	err := cacheInst.Clear()
	if err != nil {
		slog.Error("Failed to clear cache", "error", err.Error())
		return respondWithError(connWriter, "Failed to clear cache")
	}

	response := types.CacheResponse{
		Success: true,
	}

	return connWriter.Encode(response)
}

// respondWithError sends an error response back to the client
func respondWithError(connWriter *json.Encoder, errorMessage string) error {
	response := types.CacheResponse{
		Success: false,
		Error:   errorMessage,
	}

	return connWriter.Encode(response)
}
