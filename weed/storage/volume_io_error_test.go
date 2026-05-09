package storage

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestCheckReadWriteErrorTracksConsecutiveEIO(t *testing.T) {
	v := &Volume{}

	// non-EIO errors must not advance the counter.
	v.checkReadWriteError(errors.New("some other error"))
	if got := v.lastIoErrorCount.Load(); got != 0 {
		t.Fatalf("non-EIO error advanced counter: got %d", got)
	}
	if v.lastIoError != nil {
		t.Fatalf("non-EIO error set lastIoError: %v", v.lastIoError)
	}

	// each EIO bumps the counter.
	for i := int32(1); i <= 5; i++ {
		v.checkReadWriteError(fmt.Errorf("disk failed: %w", syscall.EIO))
		if got := v.lastIoErrorCount.Load(); got != i {
			t.Fatalf("after %d EIO(s): counter = %d, want %d", i, got, i)
		}
		if v.lastIoError == nil {
			t.Fatalf("after %d EIO(s): lastIoError is nil", i)
		}
	}

	// a single success resets both fields.
	v.checkReadWriteError(nil)
	if got := v.lastIoErrorCount.Load(); got != 0 {
		t.Fatalf("success did not reset counter: got %d", got)
	}
	if v.lastIoError != nil {
		t.Fatalf("success did not clear lastIoError: %v", v.lastIoError)
	}
}

func TestIoErrorToleranceGate(t *testing.T) {
	v := &Volume{}

	// below tolerance: do not act.
	for i := 0; i < IoErrorTolerance-1; i++ {
		v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	}
	if v.lastIoErrorCount.Load() >= IoErrorTolerance {
		t.Fatalf("counter %d already crossed tolerance %d after %d errors",
			v.lastIoErrorCount.Load(), IoErrorTolerance, IoErrorTolerance-1)
	}

	// one more crosses the threshold.
	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	if got := v.lastIoErrorCount.Load(); got < IoErrorTolerance {
		t.Fatalf("counter %d below tolerance %d after %d errors",
			got, IoErrorTolerance, IoErrorTolerance)
	}
}
