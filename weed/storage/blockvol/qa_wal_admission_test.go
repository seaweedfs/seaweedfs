package blockvol

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// QA Adversarial Tests for WALAdmission (BUG-CP103-2)
//
// These tests exercise race conditions, starvation scenarios, and edge cases
// that go beyond the dev-test coverage. All tests are deterministic where
// possible (injectable sleepFn) and use real concurrency where needed.
// =============================================================================

// TestQA_Admission_PressureOscillation rapidly cycles pressure between all
// three zones (below-soft, soft-to-hard, above-hard) while concurrent writers
// attempt to acquire. No writer should panic or deadlock.
func TestQA_Admission_PressureOscillation(t *testing.T) {
	var pressure atomic.Int64
	pressure.Store(50) // start below soft

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 8,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})

	// Oscillator: cycles pressure through all zones every 2ms.
	stopOsc := make(chan struct{})
	go func() {
		zones := []int64{30, 80, 95, 50, 75, 92, 40, 85, 98, 20}
		i := 0
		for {
			select {
			case <-stopOsc:
				return
			default:
				pressure.Store(zones[i%len(zones)])
				i++
				time.Sleep(500 * time.Microsecond)
			}
		}
	}()

	// 16 writers doing rapid acquire/release cycles.
	var wg sync.WaitGroup
	var successes, failures atomic.Int64
	const writers = 16
	const iterations = 50

	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				err := a.Acquire(50 * time.Millisecond)
				if err == nil {
					successes.Add(1)
					time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)
					a.Release()
				} else {
					failures.Add(1)
					if !errors.Is(err, ErrWALFull) {
						t.Errorf("unexpected error: %v", err)
					}
				}
			}
		}()
	}

	wg.Wait()
	close(stopOsc)

	total := successes.Load() + failures.Load()
	if total != writers*iterations {
		t.Fatalf("expected %d total operations, got %d", writers*iterations, total)
	}
	// With oscillating pressure and 50ms timeout, most should succeed.
	if successes.Load() == 0 {
		t.Fatal("all writers failed — admission too aggressive")
	}
	t.Logf("successes=%d failures=%d (of %d)", successes.Load(), failures.Load(), total)
}

// TestQA_Admission_StarvationUnderSoftPressure verifies that soft-watermark
// throttling doesn't cause starvation. Even at pressure just below hard mark,
// all writers should eventually complete (with delay, not rejection).
func TestQA_Admission_StarvationUnderSoftPressure(t *testing.T) {
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 4,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.89 }, // just below hard
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})
	// Soft watermark delay is real (not replaced) but max ~5ms, so this
	// should complete in reasonable time.

	var wg sync.WaitGroup
	const writers = 20

	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			if err := a.Acquire(5 * time.Second); err != nil {
				t.Errorf("writer %d starved: %v", id, err)
			} else {
				time.Sleep(100 * time.Microsecond)
				a.Release()
			}
		}(i)
	}
	wg.Wait()
}

// TestQA_Admission_HardToSoftTransitionNoDeadlock verifies that writers
// blocked in the hard-watermark loop properly transition when pressure drops
// to the soft zone (not below soft). They should proceed to semaphore
// acquisition, not re-enter the hard loop.
func TestQA_Admission_HardToSoftTransitionNoDeadlock(t *testing.T) {
	var pressure atomic.Int64
	pressure.Store(95) // above hard

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})

	var sleepCount atomic.Int64
	a.sleepFn = func(d time.Duration) {
		n := sleepCount.Add(1)
		// After 3 polls in hard loop, drop pressure to soft zone (not below soft).
		if n == 3 {
			pressure.Store(80) // between soft and hard
		}
	}

	if err := a.Acquire(1 * time.Second); err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	a.Release()

	if sleepCount.Load() < 3 {
		t.Fatalf("expected >= 3 hard-loop sleeps, got %d", sleepCount.Load())
	}
}

// TestQA_Admission_SemaphoreFullWithHardPressureDrain tests the combined
// scenario: hard pressure AND full semaphore. The writer should wait for
// pressure to drop, then wait for a semaphore slot, all within a single
// timeout budget.
func TestQA_Admission_SemaphoreFullWithHardPressureDrain(t *testing.T) {
	var pressure atomic.Int64
	pressure.Store(95)

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 1,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})

	// Fill semaphore.
	a.sem <- struct{}{}

	// Drop pressure after 10ms, release semaphore after 30ms.
	go func() {
		time.Sleep(10 * time.Millisecond)
		pressure.Store(50)
		time.Sleep(20 * time.Millisecond)
		<-a.sem
	}()

	start := time.Now()
	err := a.Acquire(500 * time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected success after pressure+semaphore drain, got: %v", err)
	}
	a.Release()

	// Should complete in ~30-50ms, not 500ms.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("elapsed %v, expected < 200ms", elapsed)
	}
	t.Logf("combined hard+semaphore wait: %v", elapsed)
}

// TestQA_Admission_ReleaseWithoutAcquire verifies that an unpaired Release
// panics with a channel receive on empty channel (tests the invariant, not
// the behavior — this is a programmer error). We verify the semaphore can
// still be used correctly after proper acquire/release cycles.
func TestQA_Admission_DoubleReleaseSafety(t *testing.T) {
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 2,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})

	// Normal acquire/release cycle should work.
	if err := a.Acquire(100 * time.Millisecond); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	a.Release()

	// Verify semaphore is clean: can acquire maxConcurrent times.
	for i := 0; i < 2; i++ {
		if err := a.Acquire(100 * time.Millisecond); err != nil {
			t.Fatalf("Acquire %d after release: %v", i, err)
		}
	}
	// Should be full now.
	err := a.Acquire(5 * time.Millisecond)
	if !errors.Is(err, ErrWALFull) {
		t.Fatalf("expected ErrWALFull with full semaphore, got %v", err)
	}
	// Clean up.
	a.Release()
	a.Release()
}

// TestQA_Admission_SoftDelayScalingBoundary checks delay calculation at
// exact boundary values: exactly soft, exactly (hard-epsilon), mid-point.
func TestQA_Admission_SoftDelayScalingBoundary(t *testing.T) {
	cases := []struct {
		name     string
		pressure float64
		minDelay time.Duration
		maxDelay time.Duration
	}{
		{"at_soft", 0.70, 0, 100 * time.Microsecond},           // scale=0, delay≈0
		{"mid", 0.80, 2 * time.Millisecond, 3 * time.Millisecond}, // scale=0.5, delay=2.5ms
		{"near_hard", 0.899, 4 * time.Millisecond, 5500 * time.Microsecond}, // scale≈0.995, delay≈4.98ms
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sleepDur time.Duration
			a := NewWALAdmission(WALAdmissionConfig{
				MaxConcurrent: 16,
				SoftWatermark: 0.7,
				HardWatermark: 0.9,
				WALUsedFn:     func() float64 { return tc.pressure },
				NotifyFn:      func() {},
				ClosedFn:      func() bool { return false },
			})
			a.sleepFn = func(d time.Duration) { sleepDur = d }

			if err := a.Acquire(100 * time.Millisecond); err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			a.Release()

			if sleepDur < tc.minDelay || sleepDur > tc.maxDelay {
				t.Fatalf("pressure=%.3f: delay=%v, want [%v, %v]",
					tc.pressure, sleepDur, tc.minDelay, tc.maxDelay)
			}
		})
	}
}

// TestQA_Admission_CloseRaceBothPaths starts many goroutines that will hit
// both the hard-watermark path and the semaphore-wait path, then closes the
// volume. All goroutines must return ErrVolumeClosed or nil (success before
// close), never hang.
func TestQA_Admission_CloseRaceBothPaths(t *testing.T) {
	var closed atomic.Bool
	var pressure atomic.Int64
	pressure.Store(95) // start above hard

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 2,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      closed.Load,
	})

	var wg sync.WaitGroup
	const writers = 20

	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			err := a.Acquire(5 * time.Second)
			if err == nil {
				a.Release()
				return
			}
			if !errors.Is(err, ErrVolumeClosed) && !errors.Is(err, ErrWALFull) {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	// Let writers enter the hard-watermark loop, then close.
	time.Sleep(10 * time.Millisecond)
	closed.Store(true)

	// Wait with a hard deadline — if any goroutine hangs, this test hangs
	// and the test framework's timeout will catch it.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All writers returned — good.
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: some writers did not return after close")
	}
}

// TestQA_Admission_ZeroPressureThroughput verifies that under zero WAL
// pressure, admission adds negligible overhead. 1000 acquire/release cycles
// should complete in under 100ms (no sleeps, no waits).
func TestQA_Admission_ZeroPressureThroughput(t *testing.T) {
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 64,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})

	start := time.Now()
	const iterations = 1000
	for i := 0; i < iterations; i++ {
		if err := a.Acquire(100 * time.Millisecond); err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		a.Release()
	}
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Fatalf("zero-pressure throughput too slow: %d ops in %v (expected < 100ms)", iterations, elapsed)
	}
	t.Logf("zero-pressure: %d acquire/release cycles in %v", iterations, elapsed)
}

// TestQA_Admission_NotifyFnPanicRecovery verifies that if notifyFn panics
// (flusher bug), the panic propagates — we do NOT silently swallow it.
// This test documents the contract: notifyFn must not panic.
func TestQA_Admission_NotifyFnPanicPropagates(t *testing.T) {
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.8 }, // soft zone triggers notify
		NotifyFn:      func() { panic("flusher bug") },
		ClosedFn:      func() bool { return false },
	})
	a.sleepFn = func(d time.Duration) {}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from notifyFn to propagate")
		}
		if r != "flusher bug" {
			t.Fatalf("unexpected panic value: %v", r)
		}
	}()

	a.Acquire(100 * time.Millisecond)
}

// TestQA_Admission_WALUsedFnReturnsAboveOne tests edge case where WALUsedFn
// returns > 1.0 (shouldn't happen, but defensive). Should be treated as
// above hard watermark.
func TestQA_Admission_WALUsedFnReturnsAboveOne(t *testing.T) {
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 1.5 }, // bogus value > 1.0
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})
	a.sleepFn = func(d time.Duration) {} // no-op to speed up

	err := a.Acquire(10 * time.Millisecond)
	if !errors.Is(err, ErrWALFull) {
		t.Fatalf("expected ErrWALFull for pressure > 1.0, got %v", err)
	}
}

// TestQA_Admission_WriteLBAIntegration creates a real BlockVol and verifies
// that concurrent writes at maximum concurrency all succeed without ErrWALFull
// when the flusher is active and WAL is adequately sized.
func TestQA_Admission_WriteLBAIntegration(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WALMaxConcurrentWrites = 4
	cfg.FlushInterval = 5 * time.Millisecond
	cfg.WALFullTimeout = 2 * time.Second

	vol, err := CreateBlockVol(dir+"/test.blk", CreateOptions{
		VolumeSize: 256 * 1024,  // 256KB
		BlockSize:  4096,
		WALSize:    128 * 1024,  // 128KB — enough for concurrent writes
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()

	// 16 goroutines, each writing 10 blocks concurrently.
	// Admission control should bound to 4 concurrent, preventing WAL overflow.
	var wg sync.WaitGroup
	var writeErrors atomic.Int64
	const writers = 16
	const writesPerWriter = 10

	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			data := make([]byte, 4096)
			data[0] = byte(id)
			for j := 0; j < writesPerWriter; j++ {
				lba := uint64((id*writesPerWriter + j) % 64) // 64 blocks in 256KB
				if err := vol.WriteLBA(lba, data); err != nil {
					writeErrors.Add(1)
					t.Errorf("writer %d write %d: %v", id, j, err)
				}
			}
		}(i)
	}
	wg.Wait()

	if writeErrors.Load() > 0 {
		t.Fatalf("%d writes failed — admission control should have prevented WAL overflow", writeErrors.Load())
	}
	t.Logf("all %d writes succeeded with maxConcurrent=4", writers*writesPerWriter)
}

// =============================================================================
// QA Adversarial Tests for WAL Admission Metrics (Item 2: WAL Visibility Hooks)
//
// These tests verify counter correctness under concurrent pressure, metrics
// consistency across all code paths, and integration with EngineMetrics.
// =============================================================================

// TestQA_Admission_Metrics_ConcurrentCountersConsistent verifies that under
// heavy concurrent load, Total >= Soft + Hard and Timeout <= Hard.
func TestQA_Admission_Metrics_ConcurrentCountersConsistent(t *testing.T) {
	m := NewEngineMetrics()
	var pressure atomic.Int64
	pressure.Store(50)

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 8,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       m,
	})

	// Oscillate pressure so writers hit all code paths.
	stopOsc := make(chan struct{})
	go func() {
		zones := []int64{30, 80, 95, 50, 75, 92, 40, 85}
		i := 0
		for {
			select {
			case <-stopOsc:
				return
			default:
				pressure.Store(zones[i%len(zones)])
				i++
				time.Sleep(time.Millisecond)
			}
		}
	}()

	var wg sync.WaitGroup
	const writers = 16
	const iters = 30

	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				err := a.Acquire(50 * time.Millisecond)
				if err == nil {
					time.Sleep(time.Duration(rand.Intn(50)) * time.Microsecond)
					a.Release()
				}
			}
		}()
	}
	wg.Wait()
	close(stopOsc)

	total := m.WALAdmitTotal.Load()
	soft := m.WALAdmitSoftTotal.Load()
	hard := m.WALAdmitHardTotal.Load()
	timeout := m.WALAdmitTimeoutTotal.Load()
	waitCount, waitSumNs := m.WALAdmitWaitSnapshot()

	// Every Acquire call records exactly one total count.
	if total != writers*iters {
		t.Fatalf("WALAdmitTotal = %d, want %d", total, writers*iters)
	}

	// soft and hard are mutually exclusive per call.
	// But a call hitting hard also doesn't count as soft.
	// Timeout is a subset of hard path calls.
	if timeout > hard {
		t.Fatalf("Timeout (%d) > Hard (%d): timeouts only happen in hard path", timeout, hard)
	}

	// Wait histogram count must equal total.
	if waitCount != total {
		t.Fatalf("WALAdmitWait count = %d, want %d (same as total)", waitCount, total)
	}

	// Sum of wait times must be non-negative.
	if waitSumNs < 0 {
		t.Fatalf("WALAdmitWait sum = %d, must be >= 0", waitSumNs)
	}

	t.Logf("total=%d soft=%d hard=%d timeout=%d waitCount=%d waitSumNs=%d",
		total, soft, hard, timeout, waitCount, waitSumNs)
}

// TestQA_Admission_Metrics_SemaphoreWaitPathRecords verifies that metrics are
// recorded even when the semaphore-wait path (not hard watermark) is the
// bottleneck. The semaphore-wait code has a separate recordAdmit call.
func TestQA_Admission_Metrics_SemaphoreWaitPathRecords(t *testing.T) {
	m := NewEngineMetrics()
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 1,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.0 }, // no pressure
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       m,
	})

	// Fill semaphore.
	a.sem <- struct{}{}

	// Release after 10ms.
	go func() {
		time.Sleep(10 * time.Millisecond)
		<-a.sem
	}()

	if err := a.Acquire(500 * time.Millisecond); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	a.Release()

	if m.WALAdmitTotal.Load() != 1 {
		t.Fatalf("WALAdmitTotal = %d, want 1", m.WALAdmitTotal.Load())
	}
	// No pressure → no soft/hard flags.
	if m.WALAdmitSoftTotal.Load() != 0 {
		t.Fatalf("WALAdmitSoftTotal = %d, want 0", m.WALAdmitSoftTotal.Load())
	}
	if m.WALAdmitHardTotal.Load() != 0 {
		t.Fatalf("WALAdmitHardTotal = %d, want 0", m.WALAdmitHardTotal.Load())
	}

	// Wait time should be >10ms (semaphore wait).
	_, waitSum := m.WALAdmitWaitSnapshot()
	if waitSum < int64(5*time.Millisecond) {
		t.Fatalf("WALAdmitWait sum = %dns, expected >= 5ms from semaphore wait", waitSum)
	}
}

// TestQA_Admission_Metrics_SemaphoreTimeoutRecords verifies that a timeout
// during semaphore wait (not hard watermark) still records timedOut=true.
func TestQA_Admission_Metrics_SemaphoreTimeoutRecords(t *testing.T) {
	m := NewEngineMetrics()
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 1,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.0 }, // no pressure
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       m,
	})

	// Fill semaphore permanently.
	a.sem <- struct{}{}

	err := a.Acquire(10 * time.Millisecond)
	if !errors.Is(err, ErrWALFull) {
		t.Fatalf("expected ErrWALFull, got %v", err)
	}

	if m.WALAdmitTotal.Load() != 1 {
		t.Fatalf("WALAdmitTotal = %d, want 1", m.WALAdmitTotal.Load())
	}
	if m.WALAdmitTimeoutTotal.Load() != 1 {
		t.Fatalf("WALAdmitTimeoutTotal = %d, want 1 (semaphore timeout)", m.WALAdmitTimeoutTotal.Load())
	}
	// Hard should NOT be set — timeout was in semaphore path, not hard pressure path.
	if m.WALAdmitHardTotal.Load() != 0 {
		t.Fatalf("WALAdmitHardTotal = %d, want 0", m.WALAdmitHardTotal.Load())
	}

	<-a.sem // cleanup
}

// TestQA_Admission_Metrics_CloseDuringSemaphoreRecords verifies that volume
// close during semaphore wait records metrics correctly (no timeout flag).
func TestQA_Admission_Metrics_CloseDuringSemaphoreRecords(t *testing.T) {
	m := NewEngineMetrics()
	var closed atomic.Bool

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 1,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.0 },
		NotifyFn:      func() {},
		ClosedFn:      closed.Load,
		Metrics:       m,
	})

	a.sem <- struct{}{}

	go func() {
		time.Sleep(10 * time.Millisecond)
		closed.Store(true)
	}()

	err := a.Acquire(2 * time.Second)
	if !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("expected ErrVolumeClosed, got %v", err)
	}

	if m.WALAdmitTotal.Load() != 1 {
		t.Fatalf("WALAdmitTotal = %d, want 1", m.WALAdmitTotal.Load())
	}
	// Close is NOT a timeout.
	if m.WALAdmitTimeoutTotal.Load() != 0 {
		t.Fatalf("WALAdmitTimeoutTotal = %d, want 0 (close, not timeout)", m.WALAdmitTimeoutTotal.Load())
	}

	<-a.sem
}

// TestQA_Admission_Metrics_Integration_WriteLBA verifies that real WriteLBA
// calls produce correct admission metrics in the engine.
func TestQA_Admission_Metrics_Integration_WriteLBA(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.WALMaxConcurrentWrites = 4
	cfg.FlushInterval = 5 * time.Millisecond
	cfg.WALFullTimeout = 2 * time.Second

	vol, err := CreateBlockVol(dir+"/test.blk", CreateOptions{
		VolumeSize: 256 * 1024,
		BlockSize:  4096,
		WALSize:    128 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()

	// Write 20 blocks.
	data := make([]byte, 4096)
	for i := 0; i < 20; i++ {
		data[0] = byte(i)
		if err := vol.WriteLBA(uint64(i%64), data); err != nil {
			t.Fatalf("WriteLBA %d: %v", i, err)
		}
	}

	// Engine metrics should show 20 admits with 0 timeouts.
	m := vol.Metrics
	if m == nil {
		t.Skip("volume has no metrics (nil)")
	}

	total := m.WALAdmitTotal.Load()
	if total != 20 {
		t.Fatalf("WALAdmitTotal = %d, want 20", total)
	}
	if m.WALAdmitTimeoutTotal.Load() != 0 {
		t.Fatalf("WALAdmitTimeoutTotal = %d, want 0", m.WALAdmitTimeoutTotal.Load())
	}
}
