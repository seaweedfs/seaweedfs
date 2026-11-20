//go:build tikv

package tikv

import (
	"context"
	"errors"
	"testing"

	tikverr "github.com/tikv/client-go/v2/error"
)

func writeConflict() error {
	return tikverr.NewErrWriteConflictWithArgs(470572673646985949, 470572673646985677, 0, []byte("k"), 0)
}

// A write in a transaction of the store's own runs again after a write
// conflict and succeeds once the other writer is done.
func TestRunOwnTxnRetriesWriteConflict(t *testing.T) {
	runs := 0
	err := runOwnTxn(context.Background(), func() (bool, error) {
		runs++
		if runs < 3 {
			return false, writeConflict()
		}
		return false, nil
	})
	if err != nil || runs != 3 {
		t.Fatalf("runOwnTxn = %v after %d runs, want nil after 3", err, runs)
	}
}

// It gives up after writeConflictRetries retries and returns TiKV's error.
func TestRunOwnTxnGivesUp(t *testing.T) {
	runs := 0
	err := runOwnTxn(context.Background(), func() (bool, error) {
		runs++
		return false, writeConflict()
	})
	if runs != writeConflictRetries+1 {
		t.Fatalf("ran %d times, want %d", runs, writeConflictRetries+1)
	}
	if !tikverr.IsErrWriteConflict(err) {
		t.Fatalf("runOwnTxn = %v, want the write conflict", err)
	}
}

// In the caller's transaction a write runs once: the conflict is reported when
// the caller commits, and only the caller can run the whole transaction again.
// Errors other than a write conflict are not retried.
func TestRunOwnTxnRunsOnceOtherwise(t *testing.T) {
	other := errors.New("region is unavailable")
	for _, tc := range []struct {
		name      string
		inContext bool
		err       error
	}{
		{"in the caller's transaction", true, writeConflict()},
		{"another error", false, other},
		{"success", false, nil},
	} {
		runs := 0
		err := runOwnTxn(context.Background(), func() (bool, error) {
			runs++
			return tc.inContext, tc.err
		})
		if runs != 1 || err != tc.err {
			t.Errorf("%s: runOwnTxn = %v after %d runs, want %v after 1", tc.name, err, runs, tc.err)
		}
	}
}

// A cancelled context stops the retries.
func TestRunOwnTxnStopsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runs := 0
	err := runOwnTxn(ctx, func() (bool, error) {
		runs++
		return false, writeConflict()
	})
	if runs != 1 || !tikverr.IsErrWriteConflict(err) {
		t.Fatalf("runOwnTxn = %v after %d runs, want the write conflict after 1", err, runs)
	}
}
