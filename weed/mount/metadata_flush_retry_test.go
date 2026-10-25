package mount

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryMetadataFlushEventuallySucceeds(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})

	var sleeps []time.Duration
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {
		sleeps = append(sleeps, d)
	}

	attempts := 0
	err := retryMetadataFlush(context.Background(), func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary failure")
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("retryMetadataFlush returned error: %v", err)
	}

	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}

	wantSleeps := []time.Duration{time.Second, 2 * time.Second}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("sleep count = %d, want %d", len(sleeps), len(wantSleeps))
	}
	for i, want := range wantSleeps {
		if sleeps[i] != want {
			t.Fatalf("sleep[%d] = %v, want %v", i, sleeps[i], want)
		}
	}
}

func TestRetryMetadataFlushReturnsLastError(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})

	var sleeps []time.Duration
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {
		sleeps = append(sleeps, d)
	}

	expectedErr := errors.New("permanent failure")
	attempts := 0
	err := retryMetadataFlush(context.Background(), func() error {
		attempts++
		return expectedErr
	}, nil)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("retryMetadataFlush error = %v, want %v", err, expectedErr)
	}

	if attempts != metadataFlushRetries+1 {
		t.Fatalf("attempts = %d, want %d", attempts, metadataFlushRetries+1)
	}

	wantSleeps := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("sleep count = %d, want %d", len(sleeps), len(wantSleeps))
	}
	for i, want := range wantSleeps {
		if sleeps[i] != want {
			t.Fatalf("sleep[%d] = %v, want %v", i, sleeps[i], want)
		}
	}
}

// TestRetryMetadataFlushStopsOnCancel verifies that once the flush context is
// done (the flush deadline elapsed against an overwhelmed filer) the retry loop
// abandons its retries immediately instead of sleeping out the backoff, so
// close() is not held longer than the deadline.
func TestRetryMetadataFlushStopsOnCancel(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})

	var sleeps []time.Duration
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {
		sleeps = append(sleeps, d)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // flush deadline already elapsed before the first attempt completes

	attempts := 0
	flushErr := errors.New("filer overwhelmed")
	err := retryMetadataFlush(ctx, func() error {
		attempts++
		return flushErr
	}, nil)
	if !errors.Is(err, flushErr) {
		t.Fatalf("retryMetadataFlush error = %v, want %v", err, flushErr)
	}

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no retries after cancel)", attempts)
	}
	if len(sleeps) != 0 {
		t.Fatalf("sleeps = %v, want none (no backoff after cancel)", sleeps)
	}
}
