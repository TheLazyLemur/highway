package ops

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_withRetry(t *testing.T) {
	count := 0
	withRetry(5, func() error {
		count++
		return errors.New("database is locked")
	})
	assert.Equal(t, 5, count)

	count = 0
	withRetry(5, func() error {
		count++
		if count == 3 {
			return nil
		}
		return errors.New("database is locked")
	})
	assert.Equal(t, 3, count)
}
