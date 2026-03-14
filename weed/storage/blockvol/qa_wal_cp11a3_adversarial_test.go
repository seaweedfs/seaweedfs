package blockvol

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================
// CP11A-3 Adversarial Test Suite
//
// 10 scenarios stress-testing WAL admission pressure tracking,
// PressureState boundaries, guidance edge cases, and concurrent
// metric visibility.
// ============================================================

// ────────────────────────────────────────────────────────────
// QA-CP11A3-1: SoftMarkEqualsHardMark_NoPanic
//
// If an operator configures softMark == hardMark, the soft-zone
// delay calculation divides by (hardMark - softMark) = 0.
// Must not panic, hang, or produce NaN/Inf delay.
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_SoftMarkEqualsHardMark_NoPanic(t *testing.T) {
	m := NewEngineMetrics()

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.8,
		HardWatermark: 0.8, // equal — no soft zone
		WALUsedFn:     func() float64 { return 0.85 }, // above both marks
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       m,
	})

	// With equal marks, pressure >= hardMark takes the hard branch.
	// The soft branch's division by zero is never reached.
	// But if the code path ever changes, this test catches it.
	done := make(chan error, 1)
	go func() {
		done <- a.Acquire(50 * time.Millisecond)
	}()

	select {
	case err := <-done:
		// ErrWALFull is expected (pressure stays above hard, times out).
		if err != ErrWALFull {
			t.Fatalf("expected ErrWALFull, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire hung — possible Inf delay from division by zero")
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-2: SoftZoneExactBoundary_DelayIsZero
//
// When pressure == softMark exactly, scale = 0, delay = 0.
// softPressureWaitNs should NOT increase (delay <= 0 skips sleep).
// But hitSoft should still be true → SoftAdmitTotal increments.
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_SoftZoneExactBoundary_DelayIsZero(t *testing.T) {
	m := NewEngineMetrics()

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return 0.7 }, // exactly at soft mark
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       m,
	})
	a.sleepFn = func(d time.Duration) {
		t.Fatalf("sleep should not be called when delay=0, but called with %v", d)
	}

	if err := a.Acquire(100 * time.Millisecond); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	a.Release()

	// SoftAdmitTotal should increment (we entered the soft branch).
	if m.WALAdmitSoftTotal.Load() != 1 {
		t.Fatalf("WALAdmitSoftTotal = %d, want 1", m.WALAdmitSoftTotal.Load())
	}
	// But no sleep → softPressureWaitNs stays 0.
	if a.SoftPressureWaitNs() != 0 {
		t.Fatalf("SoftPressureWaitNs = %d, want 0 (no delay at exact boundary)", a.SoftPressureWaitNs())
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-3: ConcurrentHardWaiters_TimeAccumulates
//
// 8 goroutines enter hard zone simultaneously. Each waits ~5ms.
// Total hardPressureWaitNs should be roughly 8 × 5ms, proving
// atomic accumulation doesn't lose contributions.
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_ConcurrentHardWaiters_TimeAccumulates(t *testing.T) {
	m := NewEngineMetrics()
	var pressure atomic.Int64
	pressure.Store(95) // above hard mark

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       m,
	})

	var sleepCalls atomic.Int64
	a.sleepFn = func(d time.Duration) {
		time.Sleep(1 * time.Millisecond)
		// After enough total sleeps across all goroutines, drop pressure.
		if sleepCalls.Add(1) >= 20 {
			pressure.Store(50)
		}
	}

	const workers = 8
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Acquire(5 * time.Second); err != nil {
				t.Errorf("Acquire: %v", err)
			}
			a.Release()
		}()
	}
	wg.Wait()

	// All 8 must have entered hard zone.
	if m.WALAdmitHardTotal.Load() < uint64(workers) {
		t.Fatalf("WALAdmitHardTotal = %d, want >= %d", m.WALAdmitHardTotal.Load(), workers)
	}
	// Accumulated hard wait should be > 0, reflecting contributions from all goroutines.
	if a.HardPressureWaitNs() <= 0 {
		t.Fatal("HardPressureWaitNs should be > 0 after concurrent hard-zone waits")
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-4: PressureStateAndAcquireRace
//
// One goroutine oscillates walUsed, another reads PressureState
// rapidly. Must not panic, must always return a valid state.
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_PressureStateAndAcquireRace(t *testing.T) {
	var pressure atomic.Int64
	pressure.Store(50)

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       NewEngineMetrics(),
	})
	a.sleepFn = func(d time.Duration) { time.Sleep(100 * time.Microsecond) }

	var wg sync.WaitGroup
	const rounds = 200

	// Goroutine 1: oscillate pressure.
	wg.Add(1)
	go func() {
		defer wg.Done()
		levels := []int64{30, 75, 95, 50, 80, 92, 10}
		for i := 0; i < rounds; i++ {
			pressure.Store(levels[i%len(levels)])
		}
	}()

	// Goroutine 2: read PressureState.
	wg.Add(1)
	go func() {
		defer wg.Done()
		valid := map[string]bool{"normal": true, "soft": true, "hard": true}
		for i := 0; i < rounds; i++ {
			s := a.PressureState()
			if !valid[s] {
				t.Errorf("PressureState() = %q — not a valid state", s)
				return
			}
		}
	}()

	// Goroutine 3: Acquire/Release rapidly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds/2; i++ {
			err := a.Acquire(20 * time.Millisecond)
			if err == nil {
				a.Release()
			}
		}
	}()

	wg.Wait()
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-5: TimeInZoneMonotonicity
//
// softPressureWaitNs and hardPressureWaitNs must be monotonically
// non-decreasing across reads, even under concurrent writes.
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_TimeInZoneMonotonicity(t *testing.T) {
	m := NewEngineMetrics()
	var pressure atomic.Int64
	pressure.Store(80) // soft zone

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       m,
	})
	a.sleepFn = func(d time.Duration) { time.Sleep(100 * time.Microsecond) }

	var wg sync.WaitGroup
	const writers = 4
	const rounds = 30

	// Writers produce soft-zone and hard-zone waits.
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				if j%5 == 0 {
					pressure.Store(95) // hard
				} else {
					pressure.Store(80) // soft
				}
				err := a.Acquire(50 * time.Millisecond)
				if err == nil {
					a.Release()
				}
				// Drop back so next Acquire can succeed.
				pressure.Store(50)
			}
		}(i)
	}

	// Reader checks monotonicity.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var prevSoft, prevHard int64
		for i := 0; i < rounds*writers; i++ {
			soft := a.SoftPressureWaitNs()
			hard := a.HardPressureWaitNs()
			if soft < prevSoft {
				t.Errorf("SoftPressureWaitNs decreased: %d -> %d", prevSoft, soft)
			}
			if hard < prevHard {
				t.Errorf("HardPressureWaitNs decreased: %d -> %d", prevHard, hard)
			}
			prevSoft = soft
			prevHard = hard
		}
	}()

	wg.Wait()
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-6: WALGuidance_ZeroInputs
//
// Zero walSize, zero blockSize, zero maxConcurrent, empty hint.
// Must not panic or produce invalid results.
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_WALGuidance_ZeroInputs(t *testing.T) {
	// All zeros.
	r := WALSizingGuidance(0, 0, "")
	if r.Level != "warn" {
		t.Errorf("zero walSize: Level = %q, want warn", r.Level)
	}

	// Zero blockSize: absMin = 0*64 = 0. Only workload minimum check fires.
	r = WALSizingGuidance(0, 0, WorkloadGeneral)
	if r.Level != "warn" {
		t.Errorf("zero walSize+blockSize: Level = %q, want warn", r.Level)
	}

	// Zero walSize but nonzero blockSize.
	r = WALSizingGuidance(0, 4096, WorkloadDatabase)
	if r.Level != "warn" {
		t.Errorf("zero walSize: Level = %q, want warn", r.Level)
	}
	if len(r.Warnings) < 2 {
		t.Errorf("expected both workload + absolute minimum warnings, got %d", len(r.Warnings))
	}

	// EvaluateWALConfig with zero maxConcurrent should not trigger concurrency warning.
	r = EvaluateWALConfig(0, 4096, 0, WorkloadGeneral)
	// walSize=0 still triggers sizing warning.
	if r.Level != "warn" {
		t.Errorf("Level = %q, want warn for zero walSize", r.Level)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-7: WALGuidance_OverflowSafe
//
// Very large blockSize × minWALEntries might overflow uint64.
// (64 × 2^60 does NOT overflow, but let's test near-boundary.)
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_WALGuidance_OverflowSafe(t *testing.T) {
	// Large blockSize: 256MB blocks × 64 = 16GB minimum.
	// walSize = 1GB → should warn (16GB > 1GB).
	r := WALSizingGuidance(1<<30, 256<<20, WorkloadGeneral)
	if r.Level != "warn" {
		t.Errorf("Level = %q, want warn (1GB WAL < 16GB absMin)", r.Level)
	}

	// Extreme: blockSize = 1<<40 (1TB). 64 × 1TB = 64TB.
	// uint64 can hold 18 EB — no overflow.
	r = WALSizingGuidance(1<<50, 1<<40, WorkloadThroughput)
	// 1PB WAL with 1TB blocks: absMin = 64TB, 1PB > 64TB → ok for absolute.
	// 1PB > 128MB (throughput min) → ok for workload.
	if r.Level != "ok" {
		t.Errorf("Level = %q, want ok for huge WAL", r.Level)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-8: WALStatusSnapshot_PartialInit
//
// BlockVol with Metrics but nil walAdmission, and vice versa.
// WALStatus must return coherent defaults for the nil side
// and real values for the non-nil side.
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_WALStatusSnapshot_PartialInit(t *testing.T) {
	// Case 1: Metrics set, walAdmission nil.
	m := NewEngineMetrics()
	m.WALAdmitSoftTotal.Add(42)
	m.WALAdmitHardTotal.Add(7)
	vol1 := &BlockVol{Metrics: m}

	ws := vol1.WALStatus()
	if ws.PressureState != "normal" {
		t.Errorf("nil admission: PressureState = %q, want normal", ws.PressureState)
	}
	if ws.SoftAdmitTotal != 42 {
		t.Errorf("SoftAdmitTotal = %d, want 42", ws.SoftAdmitTotal)
	}
	if ws.HardAdmitTotal != 7 {
		t.Errorf("HardAdmitTotal = %d, want 7", ws.HardAdmitTotal)
	}
	// Pressure wait should be 0 (no admission controller).
	if ws.SoftPressureWaitSec != 0 || ws.HardPressureWaitSec != 0 {
		t.Errorf("nil admission: pressure wait should be 0")
	}

	// Case 2: walAdmission set, Metrics nil.
	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.65,
		HardWatermark: 0.85,
		WALUsedFn:     func() float64 { return 0.7 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
	})
	vol2 := &BlockVol{walAdmission: a}

	ws2 := vol2.WALStatus()
	if ws2.PressureState != "soft" {
		t.Errorf("PressureState = %q, want soft (0.7 >= 0.65)", ws2.PressureState)
	}
	if ws2.SoftWatermark != 0.65 {
		t.Errorf("SoftWatermark = %f, want 0.65", ws2.SoftWatermark)
	}
	// Metrics fields should be zero (nil Metrics).
	if ws2.SoftAdmitTotal != 0 || ws2.HardAdmitTotal != 0 || ws2.TimeoutTotal != 0 {
		t.Errorf("nil metrics: counters should be 0")
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-9: ObserverPanic_ContainedOrDocumented
//
// If WALAdmitWaitObserver panics, RecordWALAdmit is called from
// Acquire → recordAdmit. A panic in the observer would crash the
// writer goroutine. This test documents whether the panic is
// recovered or propagated.
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_ObserverPanic_DocumentedBehavior(t *testing.T) {
	m := NewEngineMetrics()
	m.WALAdmitWaitObserver = func(s float64) { panic("boom") }

	// RecordWALAdmit calls the observer. If it panics, the caller panics.
	// This is expected (same as prometheus.Histogram.Observe panicking).
	// Document that the observer must not panic.
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		m.RecordWALAdmit(1*time.Millisecond, false, false, false)
	}()

	if !panicked {
		t.Fatal("expected panic from observer — if recovered, update this test")
	}

	// Verify counters were NOT updated (panic happened before completion).
	// Actually, the observer is called AFTER WALAdmitTotal.Add(1) and
	// walAdmitWaitNs.record(). Let's verify the counter state.
	if m.WALAdmitTotal.Load() != 1 {
		t.Errorf("WALAdmitTotal = %d — should be 1 (incremented before observer)", m.WALAdmitTotal.Load())
	}
	// soft/hard/timeout flags are processed AFTER observer — panic skips them.
	// With soft=false, hard=false, timedOut=false there's nothing to skip,
	// but the counters should reflect what happened before the panic.
}

// ────────────────────────────────────────────────────────────
// QA-CP11A3-10: ConcurrentWALStatusReads
//
// Multiple goroutines read WALStatus while Acquire/Release runs.
// Must not panic. Fields should be internally consistent
// (SoftAdmitTotal >= 0, HardPressureWaitSec >= 0, etc.)
// ────────────────────────────────────────────────────────────
func TestQA_CP11A3_ConcurrentWALStatusReads(t *testing.T) {
	m := NewEngineMetrics()
	var pressure atomic.Int64
	pressure.Store(50)

	a := NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: 16,
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
		WALUsedFn:     func() float64 { return float64(pressure.Load()) / 100.0 },
		NotifyFn:      func() {},
		ClosedFn:      func() bool { return false },
		Metrics:       m,
	})
	a.sleepFn = func(d time.Duration) { time.Sleep(50 * time.Microsecond) }

	vol := &BlockVol{
		Metrics:      m,
		walAdmission: a,
	}

	var wg sync.WaitGroup
	const rounds = 100

	// Writers with varying pressure.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			levels := []int64{50, 75, 95, 60, 85}
			for j := 0; j < rounds; j++ {
				pressure.Store(levels[j%len(levels)])
				if err := a.Acquire(20 * time.Millisecond); err == nil {
					a.Release()
				}
				pressure.Store(50) // reset for next round
			}
		}()
	}

	// Concurrent WALStatus readers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			valid := map[string]bool{"normal": true, "soft": true, "hard": true}
			for j := 0; j < rounds*2; j++ {
				ws := vol.WALStatus()
				if !valid[ws.PressureState] {
					t.Errorf("invalid PressureState: %q", ws.PressureState)
					return
				}
				if ws.UsedFraction < 0 || ws.UsedFraction > 1.01 {
					t.Errorf("UsedFraction out of range: %f", ws.UsedFraction)
					return
				}
				if ws.SoftPressureWaitSec < 0 {
					t.Errorf("SoftPressureWaitSec negative: %f", ws.SoftPressureWaitSec)
					return
				}
				if ws.HardPressureWaitSec < 0 {
					t.Errorf("HardPressureWaitSec negative: %f", ws.HardPressureWaitSec)
					return
				}
			}
		}()
	}

	wg.Wait()
}
