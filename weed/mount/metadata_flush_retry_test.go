package mount

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
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

func TestRetryMetadataFlushUntilOutlastsOutage(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})

	var sleeps []time.Duration
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {
		sleeps = append(sleeps, d)
	}

	const failures = 10
	attempts := 0
	var unbounded []int
	err := retryMetadataFlushUntil(context.Background(), func() error {
		attempts++
		if attempts <= failures {
			return errors.New("filer unreachable")
		}
		return nil
	}, func(error) bool { return true }, func(nextAttempt, totalAttempts int, backoff time.Duration, err error) {
		if totalAttempts == 0 {
			unbounded = append(unbounded, nextAttempt)
		}
	})
	if err != nil {
		t.Fatalf("retryMetadataFlushUntil returned error: %v", err)
	}
	if attempts != failures+1 {
		t.Fatalf("attempts = %d, want %d", attempts, failures+1)
	}

	wantSleeps := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	for len(wantSleeps) < failures {
		wantSleeps = append(wantSleeps, util.RetryWaitTime)
	}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("sleeps = %v, want %v", sleeps, wantSleeps)
	}
	for i, want := range wantSleeps {
		if sleeps[i] != want {
			t.Fatalf("sleep[%d] = %v, want %v", i, sleeps[i], want)
		}
	}
	if len(unbounded) != failures-metadataFlushRetries || unbounded[0] != metadataFlushRetries+2 {
		t.Fatalf("unbounded retry attempts = %v, want %d of them starting at %d",
			unbounded, failures-metadataFlushRetries, metadataFlushRetries+2)
	}
}

func TestRetryMetadataFlushUntilStopsOnRejectedError(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {}

	transient := errors.New("filer unreachable")
	permanent := errors.New("permission denied")
	attempts := 0
	err := retryMetadataFlushUntil(context.Background(), func() error {
		attempts++
		if attempts < 7 {
			return transient
		}
		return permanent
	}, func(err error) bool { return errors.Is(err, transient) }, nil)
	if !errors.Is(err, permanent) {
		t.Fatalf("retryMetadataFlushUntil error = %v, want %v", err, permanent)
	}
	if attempts != 7 {
		t.Fatalf("attempts = %d, want 7", attempts)
	}

	attempts = 0
	err = retryMetadataFlushUntil(context.Background(), func() error {
		attempts++
		return permanent
	}, func(err error) bool { return errors.Is(err, transient) }, nil)
	if !errors.Is(err, permanent) {
		t.Fatalf("retryMetadataFlushUntil error = %v, want %v", err, permanent)
	}
	if attempts != metadataFlushRetries+1 {
		t.Fatalf("attempts = %d, want the bounded %d", attempts, metadataFlushRetries+1)
	}
}

func TestRetryMetadataFlushUntilStopsOnCancel(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})

	ctx, cancel := context.WithCancel(context.Background())
	sleeps := 0
	metadataFlushSleep = func(ctx context.Context, d time.Duration) {
		sleeps++
		if d == util.RetryWaitTime {
			cancel()
		}
	}

	attempts := 0
	err := retryMetadataFlushUntil(ctx, func() error {
		attempts++
		return errors.New("filer unreachable")
	}, func(error) bool { return true }, nil)
	if err == nil {
		t.Fatal("retryMetadataFlushUntil returned nil after cancel")
	}
	if attempts != metadataFlushRetries+1 {
		t.Fatalf("attempts = %d, want %d (none after cancel)", attempts, metadataFlushRetries+1)
	}
	if sleeps != metadataFlushRetries+1 {
		t.Fatalf("sleeps = %d, want %d", sleeps, metadataFlushRetries+1)
	}
}
