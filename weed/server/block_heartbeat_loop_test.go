package weed_server

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ─── QA adversarial tests (CP4b-2) ───────────────────────────────────────────
// 9 tests exercising edge cases and concurrency in BlockVolumeHeartbeatCollector.
// Bugs found: BUG-CP4B2-1 (Stop before Run), BUG-CP4B2-2 (zero interval),
// BUG-CP4B2-3 (callback panic). All fixed.

// --- Stop lifecycle (3 tests) ---

// TestBlockQA_StopBeforeRun_NoPanic verifies Stop() returns promptly
// when Run() was never called (BUG-CP4B2-1 regression).
func TestBlockQA_StopBeforeRun_NoPanic(t *testing.T) {
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	// Never call Run(). Stop must not deadlock.
	done := make(chan struct{})
	go func() {
		collector.Stop()
		close(done)
	}()
	select {
	case <-done:
		// ok — Stop returned without deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("BUG-CP4B2-1: Stop() before Run() deadlocked (blocked >2s on <-c.done)")
	}
}

// TestBlockQA_DoubleStop verifies calling Stop() twice doesn't panic
// or deadlock (sync.Once + closed done channel).
func TestBlockQA_DoubleStop(t *testing.T) {
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	go collector.Run()
	time.Sleep(30 * time.Millisecond)
	collector.Stop()
	// Second Stop must return immediately (done already closed).
	done := make(chan struct{})
	go func() {
		collector.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second Stop() deadlocked")
	}
}

// TestBlockQA_StopDuringCallback verifies Stop() waits for an in-flight
// callback to finish before returning.
func TestBlockQA_StopDuringCallback(t *testing.T) {
	bs := newTestBlockService(t)
	var entered atomic.Bool
	var finished atomic.Bool

	collector := NewBlockVolumeHeartbeatCollector(bs, 5*time.Millisecond)
	collector.StatusCallback = func(msgs []blockvol.BlockVolumeInfoMessage) {
		entered.Store(true)
		time.Sleep(50 * time.Millisecond) // slow callback
		finished.Store(true)
	}
	go collector.Run()

	// Wait for callback to enter.
	deadline := time.After(2 * time.Second)
	for !entered.Load() {
		select {
		case <-deadline:
			t.Fatal("callback never entered")
		case <-time.After(time.Millisecond):
		}
	}

	collector.Stop()
	// After Stop returns, the callback must have finished.
	if !finished.Load() {
		t.Fatal("Stop() returned before callback finished")
	}
}

// --- Interval edge cases (2 tests) ---

// TestBlockQA_ZeroInterval_Clamped verifies zero interval doesn't panic
// (BUG-CP4B2-2 regression) and the collector still ticks.
func TestBlockQA_ZeroInterval_Clamped(t *testing.T) {
	bs := newTestBlockService(t)
	var count atomic.Int64
	collector := NewBlockVolumeHeartbeatCollector(bs, 0)
	collector.StatusCallback = func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
	}
	go collector.Run()
	defer collector.Stop()
	time.Sleep(50 * time.Millisecond)
	if count.Load() < 1 {
		t.Fatal("expected at least 1 callback with clamped zero interval")
	}
}

// TestBlockQA_NegativeInterval_Clamped verifies negative interval doesn't
// panic and is clamped to minimum.
func TestBlockQA_NegativeInterval_Clamped(t *testing.T) {
	bs := newTestBlockService(t)
	var count atomic.Int64
	collector := NewBlockVolumeHeartbeatCollector(bs, -5*time.Second)
	collector.StatusCallback = func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
	}
	go collector.Run()
	defer collector.Stop()
	time.Sleep(50 * time.Millisecond)
	if count.Load() < 1 {
		t.Fatal("expected at least 1 callback with clamped negative interval")
	}
}

// --- Callback edge cases (3 tests) ---

// TestBlockQA_CallbackPanic_Survives verifies a panicking callback doesn't
// kill the collector goroutine (BUG-CP4B2-3 regression).
func TestBlockQA_CallbackPanic_Survives(t *testing.T) {
	bs := newTestBlockService(t)
	var panicked atomic.Bool
	var afterPanic atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.StatusCallback = func(msgs []blockvol.BlockVolumeInfoMessage) {
		if !panicked.Load() {
			panicked.Store(true)
			panic("test panic in callback")
		}
		afterPanic.Add(1)
	}
	go collector.Run()
	defer collector.Stop()

	// Wait for post-panic callbacks.
	deadline := time.After(500 * time.Millisecond)
	for afterPanic.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("BUG-CP4B2-3: collector goroutine died after callback panic")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestBlockQA_SlowCallback_NoAccumulation verifies that a slow callback
// doesn't cause tick accumulation — ticks are dropped while callback runs.
func TestBlockQA_SlowCallback_NoAccumulation(t *testing.T) {
	bs := newTestBlockService(t)
	var count atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 5*time.Millisecond)
	collector.StatusCallback = func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
		time.Sleep(50 * time.Millisecond) // 10× the tick interval
	}
	go collector.Run()

	time.Sleep(200 * time.Millisecond)
	collector.Stop()
	// With 50ms sleep per callback over 200ms, expect ~4 callbacks, not 40.
	n := count.Load()
	if n > 10 {
		t.Fatalf("expected ≤10 callbacks (slow callback), got %d — tick accumulation?", n)
	}
	if n < 1 {
		t.Fatal("expected at least 1 callback")
	}
}

// TestBlockQA_CallbackSetAfterRun verifies setting StatusCallback after
// Run() has started still works on the next tick.
func TestBlockQA_CallbackSetAfterRun(t *testing.T) {
	bs := newTestBlockService(t)

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	// StatusCallback left nil initially.
	go collector.Run()
	defer collector.Stop()

	// Let a few nil-callback ticks fire.
	time.Sleep(30 * time.Millisecond)

	// Now set the callback.
	var called atomic.Bool
	collector.StatusCallback = func(msgs []blockvol.BlockVolumeInfoMessage) {
		called.Store(true)
	}

	deadline := time.After(200 * time.Millisecond)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("callback set after Run() was never called")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// --- Concurrency (1 test) ---

// TestBlockQA_ConcurrentStop verifies multiple goroutines calling Stop()
// simultaneously all return cleanly without panic or deadlock.
func TestBlockQA_ConcurrentStop(t *testing.T) {
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	go collector.Run()
	time.Sleep(30 * time.Millisecond)

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			collector.Stop()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stop() deadlocked")
	}
}

// newTestBlockService creates a BlockService with one block volume for testing.
func newTestBlockService(t *testing.T) *BlockService {
	t.Helper()
	dir := t.TempDir()
	createTestBlockVolFile(t, dir, "hb-test.blk")
	bs := StartBlockService("127.0.0.1:0", dir, "iqn.2024-01.com.test:vol.")
	if bs == nil {
		t.Fatal("expected non-nil BlockService")
	}
	t.Cleanup(bs.Shutdown)
	return bs
}

func TestBlockCollectorPeriodicTick(t *testing.T) {
	bs := newTestBlockService(t)

	var mu sync.Mutex
	var calls [][]blockvol.BlockVolumeInfoMessage

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.StatusCallback = func(msgs []blockvol.BlockVolumeInfoMessage) {
		mu.Lock()
		calls = append(calls, msgs)
		mu.Unlock()
	}

	go collector.Run()
	defer collector.Stop()

	// Wait up to 3× interval for ≥2 callbacks.
	deadline := time.After(200 * time.Millisecond)
	for {
		mu.Lock()
		n := len(calls)
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			n = len(calls)
			mu.Unlock()
			t.Fatalf("expected ≥2 callbacks, got %d", n)
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Verify at least one callback delivered volume info.
	mu.Lock()
	first := calls[0]
	mu.Unlock()
	if len(first) != 1 {
		t.Fatalf("expected 1 volume info, got %d", len(first))
	}
	if first[0].Path == "" {
		t.Fatal("expected non-empty path in volume info")
	}
}

func TestBlockCollectorStopNoLeak(t *testing.T) {
	bs := newTestBlockService(t)

	var count atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.StatusCallback = func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
	}

	go collector.Run()

	// Let it tick a few times.
	time.Sleep(50 * time.Millisecond)

	// Stop must return promptly (Run goroutine exits).
	done := make(chan struct{})
	go func() {
		collector.Stop()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2s — goroutine leak")
	}

	// After stop, no more callbacks should fire.
	snapshot := count.Load()
	time.Sleep(30 * time.Millisecond)
	if count.Load() != snapshot {
		t.Fatal("callback fired after Stop()")
	}
}

func TestBlockCollectorNilCallback(t *testing.T) {
	bs := newTestBlockService(t)

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	// StatusCallback intentionally left nil.

	go collector.Run()

	// Let it tick — should not panic.
	time.Sleep(50 * time.Millisecond)

	collector.Stop()
}
