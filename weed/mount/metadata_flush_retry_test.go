package mount

import (
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
	metadataFlushSleep = func(d time.Duration) {
		sleeps = append(sleeps, d)
	}

	attempts := 0
	err := retryMetadataFlush(func() error {
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
	metadataFlushSleep = func(d time.Duration) {
		sleeps = append(sleeps, d)
	}

	expectedErr := errors.New("permanent failure")
	attempts := 0
	err := retryMetadataFlush(func() error {
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
