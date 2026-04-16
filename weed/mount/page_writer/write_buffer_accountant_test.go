package page_writer

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteBufferAccountant_Unlimited(t *testing.T) {
	a := NewWriteBufferAccountant(0)
	a.Reserve(1 << 30)
	a.Reserve(1 << 30)
	if got := a.Used(); got != 0 {
		t.Fatalf("unlimited accountant should not track usage, got %d", got)
	}
}

func TestWriteBufferAccountant_Nil(t *testing.T) {
	var a *WriteBufferAccountant
	a.Reserve(100) // must not panic
	a.Release(100)
	if a.Used() != 0 {
		t.Fatal("nil Used should return 0")
	}
}

func TestWriteBufferAccountant_ReserveAndRelease(t *testing.T) {
	a := NewWriteBufferAccountant(300)
	a.Reserve(100)
	a.Reserve(100)
	if got := a.Used(); got != 200 {
		t.Fatalf("expected 200 used, got %d", got)
	}
	a.Release(100)
	if got := a.Used(); got != 100 {
		t.Fatalf("expected 100 used after release, got %d", got)
	}
}

func TestWriteBufferAccountant_BlocksWhenOverCap(t *testing.T) {
	a := NewWriteBufferAccountant(100)
	a.Reserve(100)

	started := make(chan struct{})
	var landed atomic.Bool
	go func() {
		close(started)
		a.Reserve(50) // should block — used=100, cap=100
		landed.Store(true)
	}()

	// Wait until the goroutine is about to enter Reserve. Once it does,
	// Reserve cannot make progress until Release is called (cap is
	// full), so landed is guaranteed to stay false until we release.
	<-started
	if landed.Load() {
		t.Fatal("second reserve should be blocked while cap is full")
	}

	a.Release(100)

	deadline := time.Now().Add(time.Second)
	for !landed.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !landed.Load() {
		t.Fatal("second reserve should unblock after release")
	}
	if got := a.Used(); got != 50 {
		t.Fatalf("expected 50 used after handoff, got %d", got)
	}
}

func TestWriteBufferAccountant_SoftThrottle(t *testing.T) {
	// Cap = 1000, soft threshold = 800. Reserve 850 (85%) then measure
	// that the next Reserve is delayed by at least softThrottleDelay.
	a := NewWriteBufferAccountant(1000)
	a.Reserve(850)

	start := time.Now()
	a.Reserve(10) // should trigger soft throttle (used=850 > 800)
	elapsed := time.Since(start)

	if elapsed < softThrottleDelay {
		t.Fatalf("expected Reserve to take >= %v (soft throttle), took %v", softThrottleDelay, elapsed)
	}
}

func TestWriteBufferAccountant_HardThrottle(t *testing.T) {
	// Cap = 1000, hard threshold = 950. Reserve 960 (96%) then measure
	// that the next Reserve is delayed by at least hardThrottleDelay.
	a := NewWriteBufferAccountant(1000)
	a.Reserve(960)

	start := time.Now()
	a.Reserve(10) // should trigger hard throttle (used=960 > 950)
	elapsed := time.Since(start)

	if elapsed < hardThrottleDelay {
		t.Fatalf("expected Reserve to take >= %v (hard throttle), took %v", hardThrottleDelay, elapsed)
	}
}

func TestWriteBufferAccountant_NoThrottleBelowSoft(t *testing.T) {
	// Cap = 1000, soft threshold = 800. Reserve 700 (70%) -- below soft
	// threshold, so subsequent Reserve should return quickly.
	a := NewWriteBufferAccountant(1000)
	a.Reserve(700)

	start := time.Now()
	a.Reserve(10)
	elapsed := time.Since(start)

	if elapsed >= 5*time.Millisecond {
		t.Fatalf("expected Reserve below soft threshold to be fast (< 5ms), took %v", elapsed)
	}
}

func TestWriteBufferAccountant_ThrottleCounters(t *testing.T) {
	a := NewWriteBufferAccountant(1000)

	// Below soft -- no throttle
	a.Reserve(700)
	a.Reserve(10)
	if a.SoftThrottleCount() != 0 || a.HardThrottleCount() != 0 {
		t.Fatalf("expected no throttle counts below soft threshold, got soft=%d hard=%d",
			a.SoftThrottleCount(), a.HardThrottleCount())
	}

	// Release back down, then fill to soft zone
	a.Release(710)
	a.Reserve(850) // now used=850, above soft (800)
	a.Reserve(10)  // triggers soft throttle
	if a.SoftThrottleCount() != 1 {
		t.Fatalf("expected soft throttle count 1, got %d", a.SoftThrottleCount())
	}
	if a.HardThrottleCount() != 0 {
		t.Fatalf("expected hard throttle count 0, got %d", a.HardThrottleCount())
	}

	// Release and fill to hard zone
	a.Release(860)
	a.Reserve(960) // now used=960, above hard (950)
	a.Reserve(10)  // triggers hard throttle
	if a.HardThrottleCount() != 1 {
		t.Fatalf("expected hard throttle count 1, got %d", a.HardThrottleCount())
	}
}

func TestWriteBufferAccountant_GraduatedRecovery(t *testing.T) {
	// Fill to hard threshold, verify hard throttle. Release to soft zone,
	// verify that throttle level decreases to soft.
	a := NewWriteBufferAccountant(1000)

	// Fill to hard zone (960 > 950)
	a.Reserve(960)
	start := time.Now()
	a.Reserve(10) // hard throttle
	elapsed := time.Since(start)
	if elapsed < hardThrottleDelay {
		t.Fatalf("expected hard throttle delay, got %v", elapsed)
	}
	if a.HardThrottleCount() != 1 {
		t.Fatalf("expected hard throttle count 1, got %d", a.HardThrottleCount())
	}

	// Release down to soft zone: used = 970 - 130 = 840 (> 800 soft, < 950 hard)
	a.Release(130)

	start = time.Now()
	a.Reserve(10) // should now be soft throttle
	elapsed = time.Since(start)
	if elapsed < softThrottleDelay {
		t.Fatalf("expected soft throttle delay after recovery, got %v", elapsed)
	}
	if elapsed >= hardThrottleDelay {
		t.Fatalf("expected soft (not hard) throttle after recovery, got %v", elapsed)
	}
	if a.SoftThrottleCount() != 1 {
		t.Fatalf("expected soft throttle count 1 after recovery, got %d", a.SoftThrottleCount())
	}
}

func TestWriteBufferAccountant_AllowsOversizeWhenEmpty(t *testing.T) {
	// A single request larger than cap must succeed when nothing is in
	// flight, otherwise a single file handle with a large chunk size would
	// deadlock forever.
	a := NewWriteBufferAccountant(100)
	done := make(chan struct{})
	go func() {
		a.Reserve(500)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("oversize reserve on empty accountant should not block")
	}
}
