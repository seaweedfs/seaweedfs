package util

import (
	"context"
	"math"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func unavailableWithHint(t *testing.T, hint time.Duration) error {
	t.Helper()
	st, err := status.New(codes.Unavailable, "warming up").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(hint)})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	return st.Err()
}

func isUnavailable(err error) bool { return status.Code(err) == codes.Unavailable }

// timeSecondAttempt runs RetryWithBackoff with one failing attempt and returns
// how long it waited before the second attempt.
func timeSecondAttempt(t *testing.T, first error) time.Duration {
	t.Helper()
	var calls int
	var firstAt, secondAt time.Time
	err := RetryWithBackoff(context.Background(), "hint-test", 30*time.Second, isUnavailable, func() error {
		calls++
		if calls == 1 {
			firstAt = time.Now()
			return first
		}
		secondAt = time.Now()
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("RetryWithBackoff = %v after %d calls, want success after 2", err, calls)
	}
	return secondAt.Sub(firstAt)
}

// A server hint shorter than the backoff step brings the retry forward.
func TestRetryWithBackoffHonoursShorterServerHint(t *testing.T) {
	wait := timeSecondAttempt(t, unavailableWithHint(t, 200*time.Millisecond))
	// The lower bound pins the slack: timers never fire early, so the retry
	// lands no sooner than hint + 50 ms, just after the server is ready.
	if wait < 250*time.Millisecond || wait > 700*time.Millisecond {
		t.Fatalf("waited %v before the retry, want about 250ms (hint + 50ms slack), not the 1s backoff", wait)
	}
}

// A zero or negative hint from a misbehaving server must not turn the backoff
// into tight polling: it is ignored and the 1s step applies.
func TestRetryWithBackoffIgnoresNonPositiveServerHint(t *testing.T) {
	for _, hint := range []time.Duration{0, -200 * time.Millisecond} {
		wait := timeSecondAttempt(t, unavailableWithHint(t, hint))
		if wait < 900*time.Millisecond || wait > 2*time.Second {
			t.Fatalf("waited %v before the retry after a %v hint, want the 1s backoff step", wait, hint)
		}
	}
}

// A hint longer than the backoff step never makes the client wait longer.
func TestRetryWithBackoffIgnoresLongerServerHint(t *testing.T) {
	wait := timeSecondAttempt(t, unavailableWithHint(t, 10*time.Second))
	if wait < 900*time.Millisecond || wait > 2*time.Second {
		t.Fatalf("waited %v before the retry, want the 1s backoff step", wait)
	}
}

// Control: without a hint the first backoff step is unchanged.
func TestRetryWithBackoffWithoutHintKeepsBackoff(t *testing.T) {
	wait := timeSecondAttempt(t, status.Error(codes.Unavailable, "warming up"))
	if wait < 900*time.Millisecond || wait > 2*time.Second {
		t.Fatalf("waited %v before the retry, want the 1s backoff step", wait)
	}
}

// A valid hint near the Duration maximum must not overflow into an immediate retry.
func TestRetryWithBackoffHugeServerHintKeepsBackoff(t *testing.T) {
	wait := timeSecondAttempt(t, unavailableWithHint(t, time.Duration(math.MaxInt64)))
	if wait < 900*time.Millisecond || wait > 2*time.Second {
		t.Fatalf("waited %v before the retry, want the 1s backoff step", wait)
	}
}

// A malformed protobuf Duration converts to a small positive value, so it must be
// rejected as invalid, not read as a hint.
func TestRetryWithBackoffIgnoresInvalidServerHint(t *testing.T) {
	st, err := status.New(codes.Unavailable, "warming up").WithDetails(&errdetails.RetryInfo{RetryDelay: &durationpb.Duration{Seconds: 1, Nanos: -999999999}})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	wait := timeSecondAttempt(t, st.Err())
	if wait < 900*time.Millisecond || wait > 2*time.Second {
		t.Fatalf("waited %v before the retry, want the 1s backoff step", wait)
	}
}
