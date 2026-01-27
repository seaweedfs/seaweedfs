package util

import (
	"errors"
	"testing"
)

func TestRetryUntil(t *testing.T) {
	// Test case 1: Function succeeds immediately
	t.Run("SucceedsImmediately", func(t *testing.T) {
		callCount := 0
		err := RetryUntil("test", func() error {
			callCount++
			return nil
		}, func(err error) bool {
			return false
		})

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if callCount != 1 {
			t.Errorf("Expected 1 call, got %d", callCount)
		}
	})

	// Test case 2: Function fails with retryable error, then succeeds
	t.Run("SucceedsAfterRetry", func(t *testing.T) {
		callCount := 0
		err := RetryUntil("test", func() error {
			callCount++
			if callCount < 3 {
				return errors.New("retryable error")
			}
			return nil
		}, func(err error) bool {
			return err.Error() == "retryable error"
		})

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if callCount != 3 {
			t.Errorf("Expected 3 calls, got %d", callCount)
		}
	})

	// Test case 3: Function fails with non-retryable error
	t.Run("FailsNonRetryable", func(t *testing.T) {
		callCount := 0
		err := RetryUntil("test", func() error {
			callCount++
			return errors.New("fatal error")
		}, func(err error) bool {
			return err.Error() == "retryable error"
		})

		if err == nil || err.Error() != "fatal error" {
			t.Errorf("Expected 'fatal error', got %v", err)
		}
		if callCount != 1 {
			t.Errorf("Expected 1 call, got %d", callCount)
		}
	})
}
