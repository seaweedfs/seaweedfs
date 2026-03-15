package util

import (
	"context"
	"errors"
	"testing"
	"time"
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

func TestRetryWithBackoff(t *testing.T) {
	retryableErr := errors.New("unavailable")
	shouldRetry := func(err error) bool { return err == retryableErr }

	t.Run("SucceedsAfterRetries", func(t *testing.T) {
		callCount := 0
		err := RetryWithBackoff(context.Background(), "test", 30*time.Second, shouldRetry, func() error {
			callCount++
			if callCount < 3 {
				return retryableErr
			}
			return nil
		})
		if err != nil {
			t.Errorf("expected success, got %v", err)
		}
		if callCount != 3 {
			t.Errorf("expected 3 calls, got %d", callCount)
		}
	})

	t.Run("StopsOnNonRetryableError", func(t *testing.T) {
		callCount := 0
		fatalErr := errors.New("fatal")
		err := RetryWithBackoff(context.Background(), "test", 30*time.Second, shouldRetry, func() error {
			callCount++
			return fatalErr
		})
		if err != fatalErr {
			t.Errorf("expected fatal error, got %v", err)
		}
		if callCount != 1 {
			t.Errorf("expected 1 call, got %d", callCount)
		}
	})

	t.Run("StopsOnContextCancel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		callCount := 0
		start := time.Now()
		err := RetryWithBackoff(ctx, "test", 30*time.Second, shouldRetry, func() error {
			callCount++
			return retryableErr
		})
		elapsed := time.Since(start)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}
		if callCount <= 1 {
			t.Errorf("expected multiple calls, got %d", callCount)
		}
		if elapsed > 5*time.Second {
			t.Errorf("took %v, expected to stop near 2s deadline", elapsed)
		}
	})

	t.Run("StopsOnMaxDuration", func(t *testing.T) {
		callCount := 0
		start := time.Now()
		err := RetryWithBackoff(context.Background(), "test", 3*time.Second, shouldRetry, func() error {
			callCount++
			return retryableErr
		})
		elapsed := time.Since(start)
		if err != retryableErr {
			t.Errorf("expected retryable error, got %v", err)
		}
		if callCount <= 1 {
			t.Errorf("expected multiple calls, got %d", callCount)
		}
		// Should stop around 3s (maxDuration), not run forever
		if elapsed > 6*time.Second {
			t.Errorf("took %v, expected to stop near 3s maxDuration", elapsed)
		}
	})
}
