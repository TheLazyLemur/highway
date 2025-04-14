package ops

import (
	"log/slog"
	"strings"
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
			slog.Error("Retrying operation due to database lock", "attempt", retryCount)
			continue
		} else {
			return err
		}
	}
	return nil
}
