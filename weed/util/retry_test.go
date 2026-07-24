package util

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestIsTransientError(t *testing.T) {
	transient := []error{
		// the S3 failure that motivated widening the gate: aws-sdk-go wraps the
		// net error in an opaque type, so only the message survives
		errors.New(`RequestError: send request failed caused by: Post "https://s3.eu-west-2.amazonaws.com/b/k?uploads=": read tcp 10.0.0.1:53868->1.2.3.4:443: read: connection reset by peer`),
		errors.New("rpc error: code = Unavailable desc = transport is closing"),
		errors.New("SlowDown: Please reduce your request rate."),
		errors.New("InternalError: We encountered an internal error. Please try again."),
		fmt.Errorf("send: %w", syscall.ETIMEDOUT),
		&net.DNSError{Err: "operation timed out", IsTimeout: true},
		io.ErrUnexpectedEOF,
	}
	for _, err := range transient {
		if !IsTransientError(err) {
			t.Errorf("expected transient: %v", err)
		}
	}

	permanent := []error{
		nil,
		errors.New("AccessDenied: Access Denied"),
		errors.New("NoSuchBucket: The specified bucket does not exist"),
		context.Canceled,
		fmt.Errorf("write: %w", context.DeadlineExceeded),
	}
	for _, err := range permanent {
		if IsTransientError(err) {
			t.Errorf("expected permanent: %v", err)
		}
	}
}

func TestIsTransientErrorMessage(t *testing.T) {
	transient := []string{
		"read tcp 10.0.0.1:8082->10.0.0.1:54848: i/o timeout",
		// the same condition relayed by a volume server inside a JSON string
		"Upload result: read tcp 10.0.0.1:8082->10.0.0.1:54848: I/O timeout",
		"Connection reset by peer",
		"dial tcp 10.0.0.1:8888: connect: no route to host",
		"rpc error: code = Unavailable desc = the connection is unavailable",
	}
	for _, msg := range transient {
		if !IsTransientErrorMessage(msg) {
			t.Errorf("expected transient: %q", msg)
		}
	}

	permanent := []string{
		"",
		"not found",
		"invalid file id",
		"chunk size mismatch",
	}
	for _, msg := range permanent {
		if IsTransientErrorMessage(msg) {
			t.Errorf("expected permanent: %q", msg)
		}
	}
}

func TestRetryTransientError(t *testing.T) {
	callCount := 0
	err := Retry("test", func() error {
		callCount++
		if callCount < 2 {
			return errors.New("read: connection reset by peer")
		}
		return nil
	})
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}

	callCount = 0
	err = Retry("test", func() error {
		callCount++
		return errors.New("AccessDenied: Access Denied")
	})
	if err == nil {
		t.Error("expected error")
	}
	if callCount != 1 {
		t.Errorf("expected 1 call for a permanent error, got %d", callCount)
	}
}

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
