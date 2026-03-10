package blockvol

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWALAdmission_AcquireRelease_Basic(t *testing.T) {
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 4,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})

	// Acquire and release should work under no pressure.
	for i := 0; i < 4; i++ {
		if err := a.Acquire(100 * time.Millisecond); err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
	}
	// All 4 slots taken — next acquire should timeout.
	err := a.Acquire(10 * time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout with all slots taken")
	}
	if !errors.Is(err, ErrWALFull) {
		t.Fatalf("expected ErrWALFull, got %v", err)
	}

	// Release one and acquire again.
	a.Release()
	if err := a.Acquire(100 * time.Millisecond); err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}

	// Release all.
	for i := 0; i < 4; i++ {
		a.Release()
	}
}

func TestWALAdmission_SoftWatermark_Throttles(t *testing.T) {
	var sleepCalls []time.Duration
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.8 }, // between soft and hard
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})
	a.sleepFn = func(d time.Duration) { sleepCalls = append(sleepCalls, d) }

	if err := a.Acquire(100 * time.Millisecond); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	a.Release()

	// Should have slept once for soft watermark delay.
	if len(sleepCalls) != 1 {
		t.Fatalf("expected 1 sleep call for soft watermark, got %d", len(sleepCalls))
	}
	// Scale: (0.8 - 0.7) / (0.9 - 0.7) = 0.5, delay = 0.5 * 5ms = 2.5ms
	if sleepCalls[0] < 2*time.Millisecond || sleepCalls[0] > 3*time.Millisecond {
		t.Fatalf("soft watermark sleep = %v, want ~2.5ms", sleepCalls[0])
	}
}

func TestWALAdmission_BelowSoft_NoThrottle(t *testing.T) {
	sleepCalled := false
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.5 }, // below soft
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})
	a.sleepFn = func(d time.Duration) { sleepCalled = true }

	if err := a.Acquire(100 * time.Millisecond); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	a.Release()

	if sleepCalled {
		t.Fatal("should not sleep below soft watermark")
	}
}

func TestWALAdmission_HardWatermark_BlocksUntilDrain(t *testing.T) {
	var pressure atomic.Int64
	pressure.Store(95) // 0.95

	var notifyCalls atomic.Int64
	var sleepCalls atomic.Int64

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() { notifyCalls.Add(1) },
		ClosedFn:      func() bool { return false },
	})
	a.sleepFn = func(d time.Duration) {
		count := sleepCalls.Add(1)
		// Simulate flusher drain: after 3 sleeps, pressure drops.
		if count >= 3 {
			pressure.Store(50)
		}
	}

	if err := a.Acquire(1 * time.Second); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	a.Release()

	if sleepCalls.Load() < 3 {
		t.Fatalf("expected >= 3 sleep calls in hard watermark wait, got %d", sleepCalls.Load())
	}
	if notifyCalls.Load() < 2 {
		t.Fatalf("expected >= 2 flusher notifications, got %d", notifyCalls.Load())
	}
}

func TestWALAdmission_HardWatermark_Timeout(t *testing.T) {
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.95 }, // always above hard
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})
	a.sleepFn = func(d time.Duration) {} // no-op sleep

	err := a.Acquire(10 * time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout under persistent hard watermark pressure")
	}
	if !errors.Is(err, ErrWALFull) {
		t.Fatalf("expected ErrWALFull, got %v", err)
	}
}

func TestWALAdmission_ClosedDuringHardWait(t *testing.T) {
	var closed atomic.Bool

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.95 },
		NotifyFn:      func() {},
		ClosedFn:      closed.Load,
	})
	a.sleepFn = func(d time.Duration) {
		closed.Store(true) // simulate volume closing during wait
	}

	err := a.Acquire(1 * time.Second)
	if !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("expected ErrVolumeClosed, got %v", err)
	}
}

func TestWALAdmission_Concurrent_BoundedWriters(t *testing.T) {
	const maxConcurrent = 4
	var active atomic.Int64
	var maxSeen atomic.Int64

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: maxConcurrent,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})

	var wg sync.WaitGroup
	const goroutines = 32

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := a.Acquire(5 * time.Second); err != nil {
					return
				}
				cur := active.Add(1)
				// Track max concurrency observed.
				for {
					old := maxSeen.Load()
					if cur <= old || maxSeen.CompareAndSwap(old, cur) {
						break
					}
				}
				// Simulate work.
				time.Sleep(100 * time.Microsecond)
				active.Add(-1)
				a.Release()
			}
		}()
	}
	wg.Wait()

	if maxSeen.Load() > maxConcurrent {
		t.Fatalf("max concurrent = %d, want <= %d", maxSeen.Load(), maxConcurrent)
	}
}

func TestWALAdmission_FlusherNotified_OnSoftAndHard(t *testing.T) {
	var notifyCount atomic.Int64
	var callNum atomic.Int64

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn: func() float64 {
			// First call returns soft pressure, second returns below soft.
			n := callNum.Add(1)
			if n == 1 {
				return 0.8 // soft watermark
			}
			return 0.3 // safe
		},
		NotifyFn: func() { notifyCount.Add(1) },
		ClosedFn: func() bool { return false },
	})
	a.sleepFn = func(d time.Duration) {}

	// First acquire: soft watermark should trigger notify.
	if err := a.Acquire(100 * time.Millisecond); err != nil {
		t.Fatalf("Acquire 1: %v", err)
	}
	a.Release()

	if notifyCount.Load() < 1 {
		t.Fatal("expected flusher notification at soft watermark")
	}

	// Second acquire: below soft, no additional notify.
	before := notifyCount.Load()
	if err := a.Acquire(100 * time.Millisecond); err != nil {
		t.Fatalf("Acquire 2: %v", err)
	}
	a.Release()

	if notifyCount.Load() != before {
		t.Fatal("should not notify flusher below soft watermark")
	}
}

// TestWALAdmission_SingleBudget_HardThenSemaphore verifies that the hard
// watermark wait and semaphore wait share a single timeout budget.
// If the hard watermark consumes most of the budget, the semaphore wait
// must use only the remaining time (not a fresh timeout).
func TestWALAdmission_SingleBudget_HardThenSemaphore(t *testing.T) {
	var pressure atomic.Int64
	pressure.Store(95) // above hard watermark

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 1,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})

	var sleepTotal atomic.Int64
	a.sleepFn = func(d time.Duration) {
		sleepTotal.Add(int64(d))
		// After some sleep cycles, drop pressure below hard mark.
		if sleepTotal.Load() > int64(10*time.Millisecond) {
			pressure.Store(50)
		}
	}

	// Fill the semaphore so semaphore wait also blocks.
	a.sem <- struct{}{}

	// Total budget: 50ms. Hard watermark will consume ~10ms of it.
	// Semaphore wait must timeout with the remaining ~40ms, NOT a fresh 50ms.
	start := time.Now()
	err := a.Acquire(50 * time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		a.Release()
		t.Fatal("expected timeout (semaphore full)")
	}
	if !errors.Is(err, ErrWALFull) {
		t.Fatalf("expected ErrWALFull, got %v", err)
	}
	// Total elapsed must be well under 2x the budget (100ms).
	// With single budget, it should be ~50ms. With double budget it would be ~100ms.
	if elapsed > 80*time.Millisecond {
		t.Fatalf("elapsed %v exceeds single-budget expectation (~50ms), suggests double timeout", elapsed)
	}

	// Drain the semaphore.
	<-a.sem
}

// TestWALAdmission_CloseDuringSemaphoreWait verifies that volume close is
// detected while waiting for a full semaphore, not only during the hard
// watermark loop.
func TestWALAdmission_CloseDuringSemaphoreWait(t *testing.T) {
	var closed atomic.Bool

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 1,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.0 }, // no pressure
		NotifyFn:      func() {},
		ClosedFn:      closed.Load,
	})

	// Fill semaphore.
	a.sem <- struct{}{}

	// Close after a short delay.
	go func() {
		time.Sleep(15 * time.Millisecond)
		closed.Store(true)
	}()

	start := time.Now()
	err := a.Acquire(2 * time.Second) // long timeout — should not wait that long
	elapsed := time.Since(start)

	if !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("expected ErrVolumeClosed, got %v", err)
	}
	// Should detect close quickly (within ~20ms), not wait 2s.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("close detection took %v, expected < 200ms", elapsed)
	}

	// Drain.
	<-a.sem
}
