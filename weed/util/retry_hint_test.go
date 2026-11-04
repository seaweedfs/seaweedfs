package util

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func unavailableWithDelay(t *testing.T, d *durationpb.Duration) error {
	t.Helper()
	st, err := status.New(codes.Unavailable, "warming up").WithDetails(&errdetails.RetryInfo{RetryDelay: d})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	return st.Err()
}

// Every guard on a server hint, without sleeping: a hint only ever shortens
// the backoff step, and only a valid, positive one counts.
func TestShortenStep(t *testing.T) {
	const step, slack = time.Second, 50 * time.Millisecond
	hinted := func(d time.Duration) error { return unavailableWithDelay(t, durationpb.New(d)) }
	for _, c := range []struct {
		name string
		err  error
		want time.Duration
	}{
		{"valid shorter hint", hinted(200 * time.Millisecond), 250 * time.Millisecond},
		{"wrapped status keeps its hint", fmt.Errorf("assign: %w", hinted(200*time.Millisecond)), 250 * time.Millisecond},
		{"zero hint", hinted(0), step},
		{"negative hint", hinted(-200 * time.Millisecond), step},
		{"hint longer than the step", hinted(10 * time.Second), step},
		{"huge hint does not overflow", hinted(time.Duration(math.MaxInt64)), step},
		{"malformed duration", unavailableWithDelay(t, &durationpb.Duration{Seconds: 1, Nanos: -999999999}), step},
		{"no hint", status.Error(codes.Unavailable, "warming up"), step},
		{"not a status error", errors.New("plain"), step},
	} {
		if got := shortenStep(step, c.err, slack); got != c.want {
			t.Errorf("%s: step %v, want %v", c.name, got, c.want)
		}
	}
}

// The slack is jittered within [50ms, 300ms] so refused clients spread out.
func TestRetryHintSlackIsJitteredWithinBounds(t *testing.T) {
	lo, hi := time.Hour, time.Duration(0)
	for i := 0; i < 2000; i++ {
		s := retryHintSlack()
		if s < 50*time.Millisecond || s > 300*time.Millisecond {
			t.Fatalf("slack %v outside [50ms, 300ms]", s)
		}
		lo, hi = min(lo, s), max(hi, s)
	}
	if hi-lo < 100*time.Millisecond {
		t.Fatalf("slack barely varies: [%v, %v]", lo, hi)
	}
}

// The loop waits hint + slack, not the 1s step, after a refusal with a 100ms hint.
func TestRetryWithBackoffHonoursShorterServerHint(t *testing.T) {
	var waits []time.Duration
	defer func(orig func(time.Duration) *time.Timer) { retryBackoffTimer = orig }(retryBackoffTimer)
	retryBackoffTimer = func(d time.Duration) *time.Timer {
		waits = append(waits, d)
		return time.NewTimer(d)
	}
	start := time.Now()
	var calls int
	err := RetryWithBackoff(context.Background(), "hint-test", 30*time.Second,
		func(err error) bool { return status.Code(err) == codes.Unavailable },
		func() error {
			calls++
			if calls == 1 {
				return unavailableWithDelay(t, durationpb.New(100*time.Millisecond))
			}
			return nil
		})
	if err != nil || calls != 2 {
		t.Fatalf("RetryWithBackoff = %v after %d calls, want success after 2", err, calls)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("returned after %v, want the retry to wait out its timer", elapsed)
	}
	if len(waits) != 1 || waits[0] < 150*time.Millisecond || waits[0] > 400*time.Millisecond {
		t.Fatalf("waits %v, want one wait of hint + slack (150-400ms), not the 1s step", waits)
	}
}
