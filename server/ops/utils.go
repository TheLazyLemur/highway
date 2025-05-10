package ops

import (
	"log/slog"
	"math"
	"strings"
	"time"
)

func withRetry(maxRetries int, operation func() error) error {
	retryCount := 0
	for retryCount < maxRetries {
		err := operation()
		if err == nil {
			return nil
		}

		if strings.Contains(err.Error(), "database is locked") {
			retryCount++
			if retryCount >= maxRetries {
				return err
			}
			backoffDuration := time.Duration(math.Pow(2, float64(retryCount))) * time.Millisecond
			slog.Error(
				"Retrying operation due to database lock",
				"attempt",
				retryCount,
				"backoff",
				backoffDuration,
			)
			time.Sleep(backoffDuration)
			continue
		} else {
			return err
		}
	}
	return nil
}
