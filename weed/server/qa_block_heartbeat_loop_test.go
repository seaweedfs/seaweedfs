package weed_server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func TestQABlockHeartbeatCollector(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// --- Stop lifecycle ---
		{name: "stop_before_run_deadlocks", run: testStopBeforeRunDeadlocks},
		{name: "double_stop_no_panic", run: testDoubleStopNoPanic},
		{name: "stop_during_callback", run: testStopDuringCallback},

		// --- Interval edge cases ---
		{name: "zero_interval_panics", run: testZeroIntervalPanics},
		{name: "very_short_interval", run: testVeryShortInterval},

		// --- Callback edge cases ---
		{name: "callback_panic_crashes_goroutine", run: testCallbackPanicCrashesGoroutine},
		{name: "callback_slow_blocks_next_tick", run: testCallbackSlowBlocksNextTick},
		{name: "callback_set_after_run", run: testCallbackSetAfterRun},

		// --- Concurrency ---
		{name: "concurrent_stop_calls", run: testConcurrentStopCalls},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// ---------------------------------------------------------------------------
// Stop lifecycle
// ---------------------------------------------------------------------------

func testStopBeforeRunDeadlocks(t *testing.T) {
	// BUG-CP4B2-1: Stop() before Run() blocks forever on <-c.done.
	// done channel is unbuffered and only closed by Run()'s defer.
	// If Run() never starts, Stop() hangs.
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 100*time.Millisecond)

	stopped := make(chan struct{})
	go func() {
		collector.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		// Good: Stop() returned. Bug is fixed.
	case <-time.After(2 * time.Second):
		t.Error("BUG-CP4B2-1: Stop() before Run() deadlocked (blocked >2s on <-c.done)")
		// We can't recover from this -- the goroutine is leaked.
		// Start Run() to unblock.
		go collector.Run()
		<-stopped
	}
}

func testDoubleStopNoPanic(t *testing.T) {
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {})

	go collector.Run()
	time.Sleep(30 * time.Millisecond)

	// First stop.
	collector.Stop()

	// Second stop should not panic (sync.Once + closed done channel).
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("double Stop() panicked: %v", r)
			}
			close(done)
		}()
		collector.Stop()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("double Stop() blocked >2s")
	}
}

func testStopDuringCallback(t *testing.T) {
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)

	callbackStarted := make(chan struct{})
	callbackRelease := make(chan struct{})

	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		select {
		case callbackStarted <- struct{}{}:
		default:
		}
		<-callbackRelease
	})

	go collector.Run()

	// Wait for callback to start.
	select {
	case <-callbackStarted:
	case <-time.After(2 * time.Second):
		close(callbackRelease)
		t.Fatal("callback never started")
		return
	}

	// Stop while callback is blocked. Stop should block until Run() exits.
	stopDone := make(chan struct{})
	go func() {
		collector.Stop()
		close(stopDone)
	}()

	// Stop should be blocked because callback is still running.
	select {
	case <-stopDone:
		close(callbackRelease)
		// Stop returned while callback was blocked -- this means Run()
		// exited mid-callback? Let's see...
		t.Log("Stop() returned while callback was blocked (Run exited between ticks)")
	case <-time.After(100 * time.Millisecond):
		// Expected: Stop is blocked because Run() is in the callback.
		// Actually, Run()'s select will pick stopCh on the NEXT iteration,
		// not mid-callback. So callback must complete first.
		close(callbackRelease)
		<-stopDone
		t.Log("Stop() waited for callback to finish before returning (correct)")
	}
}

// ---------------------------------------------------------------------------
// Interval edge cases
// ---------------------------------------------------------------------------

func testZeroIntervalPanics(t *testing.T) {
	// BUG-CP4B2-2 (fixed): zero interval is clamped to minHeartbeatInterval.
	// Verify: no panic, collector runs normally, callbacks fire.
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 0)

	var count atomic.Int64
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
	})

	go collector.Run()
	time.Sleep(30 * time.Millisecond)
	collector.Stop()

	n := count.Load()
	if n < 1 {
		t.Errorf("expected at least 1 callback with clamped interval, got %d", n)
	}
	t.Logf("zero interval (clamped): %d callbacks in 30ms", n)
}

func testVeryShortInterval(t *testing.T) {
	bs := newTestBlockService(t)
	var count atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 1*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
	})

	go collector.Run()
	time.Sleep(50 * time.Millisecond)
	collector.Stop()

	n := count.Load()
	if n < 5 {
		t.Errorf("expected >= 5 callbacks at 1ms interval over 50ms, got %d", n)
	}
	t.Logf("1ms interval: %d callbacks in 50ms", n)
}

// ---------------------------------------------------------------------------
// Callback edge cases
// ---------------------------------------------------------------------------

func testCallbackPanicCrashesGoroutine(t *testing.T) {
	// BUG-CP4B2-3 (fixed): safeCallback recovers panics. Run() continues.
	// Verify: panic is logged, collector keeps running, subsequent callbacks fire.
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)

	var callCount atomic.Int64
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		n := callCount.Add(1)
		if n == 1 {
			panic("deliberate test panic in callback")
		}
	})

	go collector.Run()

	// Wait for multiple callbacks (first panics, subsequent should still fire).
	time.Sleep(100 * time.Millisecond)
	collector.Stop()

	n := callCount.Load()
	if n < 2 {
		t.Errorf("expected >= 2 callbacks (first panics, rest recover), got %d", n)
	}
	t.Logf("callback panic recovery: %d callbacks total (first panicked, rest recovered)", n)
}

func testCallbackSlowBlocksNextTick(t *testing.T) {
	bs := newTestBlockService(t)
	var count atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
		time.Sleep(50 * time.Millisecond) // 5x the interval
	})

	go collector.Run()
	time.Sleep(200 * time.Millisecond)
	collector.Stop()

	n := count.Load()
	// With 50ms callback sleep and 10ms interval, we should get ~4 callbacks
	// (200ms / 50ms), not 20 (200ms / 10ms). Slow callback blocks the loop.
	if n > 8 {
		t.Errorf("expected slow callback to throttle ticks, got %d callbacks", n)
	}
	t.Logf("slow callback: %d callbacks in 200ms (10ms interval, 50ms callback)", n)
}

func testCallbackSetAfterRun(t *testing.T) {
	// Setting SetStatusCallback after Run() starts -- now safe with cbMu
	// (BUG-CP4B3-2 fix).
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	// Start with nil callback.

	go collector.Run()

	// Set callback after Run started. With cbMu, this is race-free.
	time.Sleep(5 * time.Millisecond)
	var called atomic.Bool
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		called.Store(true)
	})

	time.Sleep(50 * time.Millisecond)
	collector.Stop()

	t.Logf("callback set after Run: called=%v", called.Load())
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func testConcurrentStopCalls(t *testing.T) {
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {})

	go collector.Run()
	time.Sleep(30 * time.Millisecond)

	// 10 goroutines all calling Stop concurrently.
	var wg atomic.Int64
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)
			collector.Stop()
		}()
	}

	go func() {
		for wg.Load() > 0 {
			time.Sleep(1 * time.Millisecond)
		}
		close(done)
	}()

	select {
	case <-done:
		// All returned.
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stop() calls blocked >5s")
	}
}
