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
