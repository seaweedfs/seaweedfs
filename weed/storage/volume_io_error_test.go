package storage

import (
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
)

func TestCheckReadWriteErrorTracksConsecutiveEIO(t *testing.T) {
	v := &Volume{}

	// each EIO bumps the counter.
	for i := int32(1); i <= 5; i++ {
		v.checkReadWriteError(fmt.Errorf("disk failed: %w", syscall.EIO))
		_, count := v.getIoErrorState()
		if count != i {
			t.Fatalf("after %d EIO(s): counter = %d, want %d", i, count, i)
		}
	}

	// a single success resets both fields.
	v.checkReadWriteError(nil)
	if err, count := v.getIoErrorState(); err != nil || count != 0 {
		t.Fatalf("success did not reset state: err=%v count=%d", err, count)
	}
}

func TestCheckReadWriteErrorNonEIOResetsStreak(t *testing.T) {
	v := &Volume{}

	// build up a 2-EIO streak.
	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	if _, count := v.getIoErrorState(); count != 2 {
		t.Fatalf("expected count=2 after two EIOs, got %d", count)
	}

	// a non-EIO error breaks the streak — only sustained EIOs are
	// diagnostic of a failing disk.
	v.checkReadWriteError(fmt.Errorf("other: %w", syscall.ENOSPC))
	if err, count := v.getIoErrorState(); err != nil || count != 0 {
		t.Fatalf("non-EIO did not reset streak: err=%v count=%d", err, count)
	}

	// a fresh EIO starts the streak from 1, not 3.
	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	if _, count := v.getIoErrorState(); count != 1 {
		t.Fatalf("EIO after non-EIO did not restart streak: count=%d, want 1", count)
	}
}

func TestCheckReadWriteErrorIgnoresPlainError(t *testing.T) {
	v := &Volume{}

	// non-EIO error with no prior streak should be a no-op (count
	// stays 0, no spurious lastIoError).
	v.checkReadWriteError(errors.New("some other error"))
	if err, count := v.getIoErrorState(); err != nil || count != 0 {
		t.Fatalf("non-EIO with no prior streak set state: err=%v count=%d", err, count)
	}
}

func TestIoErrorToleranceGate(t *testing.T) {
	v := &Volume{}

	// below tolerance: do not act.
	for i := 0; i < IoErrorTolerance-1; i++ {
		v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	}
	if _, count := v.getIoErrorState(); count >= IoErrorTolerance {
		t.Fatalf("counter %d already crossed tolerance %d after %d errors",
			count, IoErrorTolerance, IoErrorTolerance-1)
	}

	// one more crosses the threshold.
	v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
	if _, count := v.getIoErrorState(); count < IoErrorTolerance {
		t.Fatalf("counter %d below tolerance %d after %d errors",
			count, IoErrorTolerance, IoErrorTolerance)
	}
}

func TestIoErrorStateIsRaceFree(t *testing.T) {
	// Drives both writers (checkReadWriteError) and a reader
	// (getIoErrorState) concurrently; relies on `go test -race` to
	// detect any unprotected access on lastIoError / lastIoErrorCount.
	v := &Volume{}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				v.checkReadWriteError(fmt.Errorf("eio: %w", syscall.EIO))
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				v.checkReadWriteError(nil)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			v.getIoErrorState()
		}
		close(stop)
	}()
	wg.Wait()
}
