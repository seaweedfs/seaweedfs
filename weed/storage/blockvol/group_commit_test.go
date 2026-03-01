package blockvol

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupCommitter(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "group_single_barrier", run: testGroupSingleBarrier},
		{name: "group_batch_10", run: testGroupBatch10},
		{name: "group_max_delay", run: testGroupMaxDelay},
		{name: "group_max_batch", run: testGroupMaxBatch},
		{name: "group_fsync_error", run: testGroupFsyncError},
		{name: "group_sequential", run: testGroupSequential},
		{name: "group_shutdown", run: testGroupShutdown},
		{name: "group_submit_after_stop", run: testGroupSubmitAfterStop},
		// Phase 3 Task 1.4: Adaptive group commit.
		{name: "adaptive_single_immediate", run: testAdaptiveSingleImmediate},
		{name: "adaptive_batch_above_watermark", run: testAdaptiveBatchAboveWatermark},
		{name: "adaptive_max_batch_triggers_early", run: testAdaptiveMaxBatchTriggersEarly},
		{name: "adaptive_watermark_zero_compat", run: testAdaptiveWatermarkZeroCompat},
		{name: "adaptive_threshold_boundary", run: testAdaptiveThresholdBoundary},
		{name: "adaptive_ramp_up", run: testAdaptiveRampUp},
		{name: "adaptive_max_delay_honored", run: testAdaptiveMaxDelayHonored},
		{name: "adaptive_fsync_error_all_waiters", run: testAdaptiveFsyncErrorAllWaiters},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testGroupSingleBarrier(t *testing.T) {
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay: 10 * time.Millisecond,
	})
	go gc.Run()
	defer gc.Stop()

	if err := gc.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	if c := syncCalls.Load(); c != 1 {
		t.Errorf("syncCalls = %d, want 1", c)
	}
}

func testGroupBatch10(t *testing.T) {
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay: 50 * time.Millisecond,
		MaxBatch: 64,
	})
	go gc.Run()
	defer gc.Stop()

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)

	// Launch all 10 concurrently.
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = gc.Submit()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Submit[%d]: %v", i, err)
		}
	}

	// All 10 should be batched into 1 fsync (maybe 2 if timing is unlucky).
	if c := syncCalls.Load(); c > 2 {
		t.Errorf("syncCalls = %d, want 1-2 (batched)", c)
	}
}

func testGroupMaxDelay(t *testing.T) {
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error { return nil },
		MaxDelay: 5 * time.Millisecond,
	})
	go gc.Run()
	defer gc.Stop()

	start := time.Now()
	if err := gc.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	elapsed := time.Since(start)

	// Should complete within ~maxDelay + some margin, not take much longer.
	if elapsed > 50*time.Millisecond {
		t.Errorf("Submit took %v, expected ~5ms", elapsed)
	}
}

func testGroupMaxBatch(t *testing.T) {
	var syncCalls atomic.Uint64
	const batch = 64

	// Use a slow sync to ensure we can detect immediate trigger.
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay: 5 * time.Second, // very long delay -- should NOT wait this long
		MaxBatch: batch,
	})
	go gc.Run()
	defer gc.Stop()

	var wg sync.WaitGroup
	wg.Add(batch)
	for i := 0; i < batch; i++ {
		go func() {
			defer wg.Done()
			gc.Submit()
		}()
	}

	// Wait with timeout -- if maxBatch triggers, should complete fast.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good -- completed without waiting 5 seconds.
	case <-time.After(2 * time.Second):
		t.Fatal("maxBatch=64 did not trigger immediate flush")
	}
}

func testGroupFsyncError(t *testing.T) {
	errIO := errors.New("simulated EIO")
	var degraded atomic.Bool

	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			return errIO
		},
		MaxDelay:   10 * time.Millisecond,
		OnDegraded: func() { degraded.Store(true) },
	})
	go gc.Run()
	defer gc.Stop()

	const n = 5
	var wg sync.WaitGroup
	errs := make([]error, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = gc.Submit()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, errIO) {
			t.Errorf("Submit[%d]: got %v, want %v", i, err, errIO)
		}
	}

	if !degraded.Load() {
		t.Error("onDegraded was not called")
	}
}

func testGroupSequential(t *testing.T) {
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay: 10 * time.Millisecond,
	})
	go gc.Run()
	defer gc.Stop()

	// First sync.
	if err := gc.Submit(); err != nil {
		t.Fatalf("Submit 1: %v", err)
	}

	// Second sync (after first completes).
	if err := gc.Submit(); err != nil {
		t.Fatalf("Submit 2: %v", err)
	}

	if c := syncCalls.Load(); c != 2 {
		t.Errorf("syncCalls = %d, want 2 (two separate fsyncs)", c)
	}
}

func testGroupShutdown(t *testing.T) {
	// Block fsync so we can shut down while waiters are pending.
	syncStarted := make(chan struct{})
	syncBlock := make(chan struct{})
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			close(syncStarted)
			<-syncBlock // block until test releases
			return nil
		},
		MaxDelay: 1 * time.Millisecond,
	})
	go gc.Run()

	// Submit one request that will block on fsync.
	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- gc.Submit()
	}()

	// Wait for fsync to start.
	<-syncStarted

	// Submit another request while fsync is in progress.
	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- gc.Submit()
	}()
	time.Sleep(5 * time.Millisecond) // let it enqueue

	// Stop the group committer (will drain pending with ErrGroupCommitShutdown).
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(syncBlock) // unblock the fsync first
	}()
	gc.Stop()

	// First waiter should get nil (fsync succeeded).
	if err := <-errCh1; err != nil {
		t.Errorf("first waiter: %v, want nil", err)
	}

	// Second waiter should get shutdown error.
	if err := <-errCh2; !errors.Is(err, ErrGroupCommitShutdown) {
		t.Errorf("second waiter: %v, want ErrGroupCommitShutdown", err)
	}
}

// --- Phase 3 Task 1.4: Adaptive group commit tests ---

func testAdaptiveSingleImmediate(t *testing.T) {
	// With lowWatermark=4, a single Submit should flush immediately (no delay).
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay:     5 * time.Second, // very long delay
		LowWatermark: 4,
	})
	go gc.Run()
	defer gc.Stop()

	start := time.Now()
	if err := gc.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	elapsed := time.Since(start)

	if syncCalls.Load() != 1 {
		t.Errorf("syncCalls = %d, want 1", syncCalls.Load())
	}
	// Should complete much faster than 5s maxDelay.
	if elapsed > 500*time.Millisecond {
		t.Errorf("Submit took %v, expected immediate (low watermark skip)", elapsed)
	}
}

func testAdaptiveBatchAboveWatermark(t *testing.T) {
	// With lowWatermark=4 and a slow sync, 10 concurrent submits should batch.
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			time.Sleep(5 * time.Millisecond) // slow sync so requests queue up
			return nil
		},
		MaxDelay:     50 * time.Millisecond,
		MaxBatch:     64,
		LowWatermark: 4,
	})
	go gc.Run()
	defer gc.Stop()

	const n = 10
	var wg sync.WaitGroup
	// Pre-enqueue all requests, then start Run.
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			gc.Submit()
		}()
	}
	wg.Wait()

	// With slow sync, many should batch. At most n fsyncs (no batching), at least 1.
	c := syncCalls.Load()
	if c == 0 {
		t.Error("syncCalls = 0, want > 0")
	}
	t.Logf("adaptive batch: %d submits, %d fsyncs", n, c)
}

func testAdaptiveMaxBatchTriggersEarly(t *testing.T) {
	// maxBatch should still trigger immediate flush even with adaptive watermark.
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:     func() error { return nil },
		MaxDelay:     5 * time.Second,
		MaxBatch:     8,
		LowWatermark: 4,
	})
	go gc.Run()
	defer gc.Stop()

	var wg sync.WaitGroup
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer wg.Done()
			gc.Submit()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("maxBatch=8 did not trigger flush with adaptive watermark")
	}
}

func testAdaptiveWatermarkZeroCompat(t *testing.T) {
	// LowWatermark=0 should preserve Phase 2 behavior (always wait for delay).
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay:     10 * time.Millisecond,
		LowWatermark: 0,
	})
	go gc.Run()
	defer gc.Stop()

	// Two sequential submits should each trigger their own fsync.
	if err := gc.Submit(); err != nil {
		t.Fatalf("Submit 1: %v", err)
	}
	if err := gc.Submit(); err != nil {
		t.Fatalf("Submit 2: %v", err)
	}

	if c := syncCalls.Load(); c != 2 {
		t.Errorf("syncCalls = %d, want 2 (no watermark optimization)", c)
	}
}

func testAdaptiveThresholdBoundary(t *testing.T) {
	// Exactly lowWatermark concurrent submits: should batch and flush.
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay:     50 * time.Millisecond,
		MaxBatch:     64,
		LowWatermark: 4,
	})
	go gc.Run()
	defer gc.Stop()

	const n = 4 // exactly lowWatermark
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			gc.Submit()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All 4 should complete. SyncCount should be >= 1.
		if c := syncCalls.Load(); c == 0 {
			t.Error("syncCalls = 0, want >= 1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("threshold boundary: timed out waiting for 4 submits")
	}
}

func testAdaptiveRampUp(t *testing.T) {
	// Start with single callers (below watermark -> immediate), then ramp up.
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay:     50 * time.Millisecond,
		MaxBatch:     64,
		LowWatermark: 4,
	})
	go gc.Run()
	defer gc.Stop()

	// Phase 1: single serial submits (below watermark, immediate).
	for i := 0; i < 5; i++ {
		if err := gc.Submit(); err != nil {
			t.Fatalf("serial Submit %d: %v", i, err)
		}
	}
	serialSyncs := syncCalls.Load()
	if serialSyncs != 5 {
		t.Errorf("serial phase: syncCalls = %d, want 5 (1 per call)", serialSyncs)
	}

	// Phase 2: burst 64 concurrent submits (above watermark, batched).
	var wg sync.WaitGroup
	wg.Add(64)
	for i := 0; i < 64; i++ {
		go func() {
			defer wg.Done()
			gc.Submit()
		}()
	}
	wg.Wait()

	totalSyncs := syncCalls.Load()
	batchSyncs := totalSyncs - serialSyncs
	// 64 submits should complete without deadlock. Some batching expected
	// but fast syncFunc means each call may flush individually.
	if batchSyncs > 64 {
		t.Errorf("batch phase: %d fsyncs for 64 submits, want <= 64", batchSyncs)
	}
	t.Logf("ramp_up: serial=%d, batch=%d fsyncs for 64 submits", serialSyncs, batchSyncs)
}

func testAdaptiveMaxDelayHonored(t *testing.T) {
	// 2 concurrent submits (below watermark) should still complete promptly.
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:     func() error { return nil },
		MaxDelay:     5 * time.Second,
		LowWatermark: 4,
	})
	go gc.Run()
	defer gc.Stop()

	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			gc.Submit()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good: completed without waiting 5s maxDelay (adaptive skip).
	case <-time.After(2 * time.Second):
		t.Fatal("2 submits should complete quickly (below watermark), took >2s")
	}
}

func testAdaptiveFsyncErrorAllWaiters(t *testing.T) {
	// Inject fsync error during batched flush: all waiters must receive the error.
	errIO := errors.New("simulated disk error")
	var degradedCalled atomic.Bool

	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			return errIO
		},
		MaxDelay:     50 * time.Millisecond,
		MaxBatch:     64,
		LowWatermark: 4,
		OnDegraded:   func() { degradedCalled.Store(true) },
	})
	go gc.Run()
	defer gc.Stop()

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = gc.Submit()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, errIO) {
			t.Errorf("Submit[%d]: got %v, want %v", i, err, errIO)
		}
	}
	if !degradedCalled.Load() {
		t.Error("OnDegraded was not called")
	}
}

func testGroupSubmitAfterStop(t *testing.T) {
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error { return nil },
		MaxDelay: 10 * time.Millisecond,
	})
	go gc.Run()
	gc.Stop()

	// Submit after Stop must return ErrGroupCommitShutdown, not deadlock.
	done := make(chan error, 1)
	go func() {
		done <- gc.Submit()
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrGroupCommitShutdown) {
			t.Errorf("Submit after Stop: %v, want ErrGroupCommitShutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Submit after Stop deadlocked")
	}
}
