package s3lifecycle

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestGetVersionTimestamp(t *testing.T) {
	t.Run("new_format_inverted_timestamp", func(t *testing.T) {
		// Simulate a new-format version ID (inverted timestamp above threshold).
		now := time.Now()
		inverted := math.MaxInt64 - now.UnixNano()
		versionId := fmt.Sprintf("%016x", inverted) + "0000000000000000"

		got := GetVersionTimestamp(versionId)
		// Should recover the original timestamp within 1 second.
		diff := got.Sub(now)
		if diff < -time.Second || diff > time.Second {
			t.Errorf("timestamp diff too large: %v (got %v, want ~%v)", diff, got, now)
		}
	})

	t.Run("old_format_raw_timestamp", func(t *testing.T) {
		// Simulate an old-format version ID (raw nanosecond timestamp below threshold).
		// Use a timestamp from 2023 which would be below threshold.
		ts := time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)
		versionId := fmt.Sprintf("%016x", ts.UnixNano()) + "abcdef0123456789"

		got := GetVersionTimestamp(versionId)
		if !got.Equal(ts) {
			t.Errorf("expected %v, got %v", ts, got)
		}
	})

	t.Run("null_version_id", func(t *testing.T) {
		got := GetVersionTimestamp("null")
		if !got.IsZero() {
			t.Errorf("expected zero time for null version, got %v", got)
		}
	})

	t.Run("empty_version_id", func(t *testing.T) {
		got := GetVersionTimestamp("")
		if !got.IsZero() {
			t.Errorf("expected zero time for empty version, got %v", got)
		}
	})

	t.Run("short_version_id", func(t *testing.T) {
		got := GetVersionTimestamp("abc")
		if !got.IsZero() {
			t.Errorf("expected zero time for short version, got %v", got)
		}
	})

	t.Run("high_bit_overflow_returns_zero", func(t *testing.T) {
		// Version ID with first 16 hex chars > math.MaxInt64 should return zero,
		// not a wrapped negative timestamp.
		versionId := "80000000000000000000000000000000"
		got := GetVersionTimestamp(versionId)
		if !got.IsZero() {
			t.Errorf("expected zero time for overflow version ID, got %v", got)
		}
	})

	t.Run("invalid_hex", func(t *testing.T) {
		got := GetVersionTimestamp("zzzzzzzzzzzzzzzz0000000000000000")
		if !got.IsZero() {
			t.Errorf("expected zero time for invalid hex, got %v", got)
		}
	})
}
