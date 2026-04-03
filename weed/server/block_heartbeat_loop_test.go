package weed_server

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// --- QA adversarial tests (CP4b-2) ----------------------------------------
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
		// ok -- Stop returned without deadlock
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
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		entered.Store(true)
		time.Sleep(50 * time.Millisecond) // slow callback
		finished.Store(true)
	})
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
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
	})
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
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
	})
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
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		if !panicked.Load() {
			panicked.Store(true)
			panic("test panic in callback")
		}
		afterPanic.Add(1)
	})
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
// doesn't cause tick accumulation -- ticks are dropped while callback runs.
func TestBlockQA_SlowCallback_NoAccumulation(t *testing.T) {
	bs := newTestBlockService(t)
	var count atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 5*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
		time.Sleep(50 * time.Millisecond) // 10x the tick interval
	})
	go collector.Run()

	time.Sleep(200 * time.Millisecond)
	collector.Stop()
	// With 50ms sleep per callback over 200ms, expect ~4 callbacks, not 40.
	n := count.Load()
	if n > 10 {
		t.Fatalf("expected <=10 callbacks (slow callback), got %d -- tick accumulation?", n)
	}
	if n < 1 {
		t.Fatal("expected at least 1 callback")
	}
}

// TestBlockQA_CallbackSetAfterRun verifies setting SetStatusCallback after
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
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		called.Store(true)
	})

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
	bs := StartBlockService("127.0.0.1:0", dir, "iqn.2024-01.com.test:vol.", "127.0.0.1:3260,1", NVMeConfig{})
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
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		mu.Lock()
		calls = append(calls, msgs)
		mu.Unlock()
	})

	go collector.Run()
	defer collector.Stop()

	// Wait up to 3x interval for >=2 callbacks.
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
			t.Fatalf("expected >=2 callbacks, got %d", n)
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
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		count.Add(1)
	})

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
		t.Fatal("Stop() did not return within 2s -- goroutine leak")
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

	// Let it tick -- should not panic.
	time.Sleep(50 * time.Millisecond)

	collector.Stop()
}

// --- Dev tests: Assignment Processing (CP4b-3) ----------------------------
// 7 tests verifying ProcessBlockVolumeAssignments and AssignmentSource wiring.

// testBlockVolPath returns the path of the single block volume in the test service.
func testBlockVolPath(t *testing.T, bs *BlockService) string {
	t.Helper()
	paths := bs.Store().ListBlockVolumes()
	if len(paths) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(paths))
	}
	return paths[0]
}

// TestBlockAssign_Success verifies a valid assignment (RoleNone -> Primary)
// changes the volume's role.
func TestBlockAssign_Success(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	assignments := []blockvol.BlockVolumeAssignment{{
		Path:       path,
		Epoch:      1,
		Role:       uint32(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
	}}
	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	if errs[0] != nil {
		t.Fatalf("expected nil error, got %v", errs[0])
	}

	vol, _ := bs.Store().GetBlockVolume(path)
	if vol.Role() != blockvol.RolePrimary {
		t.Fatalf("expected Primary role, got %s", vol.Role())
	}
}

// TestBlockAssign_UnknownVolume verifies an assignment for a non-existent
// volume returns an error without stopping processing.
func TestBlockAssign_UnknownVolume(t *testing.T) {
	bs := newTestBlockService(t)

	assignments := []blockvol.BlockVolumeAssignment{{
		Path:  "/nonexistent/vol.blk",
		Epoch: 1,
		Role:  uint32(blockvol.RolePrimary),
	}}
	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	if errs[0] == nil {
		t.Fatal("expected error for unknown volume")
	}
	if !strings.Contains(errs[0].Error(), "not found") {
		t.Fatalf("expected 'not found' error, got: %v", errs[0])
	}
}

// TestBlockAssign_InvalidTransition verifies an invalid role transition
// (RoleNone -> Stale) returns an error.
func TestBlockAssign_InvalidTransition(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	assignments := []blockvol.BlockVolumeAssignment{{
		Path:  path,
		Epoch: 1,
		Role:  uint32(blockvol.RoleStale),
	}}
	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	if errs[0] == nil {
		t.Fatal("expected error for invalid transition")
	}
}

// TestBlockAssign_EmptyAssignments verifies an empty slice produces no errors.
func TestBlockAssign_EmptyAssignments(t *testing.T) {
	bs := newTestBlockService(t)

	errs := bs.Store().ProcessBlockVolumeAssignments(nil)
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors for nil input, got %d", len(errs))
	}

	errs = bs.Store().ProcessBlockVolumeAssignments([]blockvol.BlockVolumeAssignment{})
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors for empty input, got %d", len(errs))
	}
}

// TestBlockAssign_NilSource verifies that a nil AssignmentSource doesn't
// cause panics and status collection still works.
func TestBlockAssign_NilSource(t *testing.T) {
	bs := newTestBlockService(t)
	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	// AssignmentSource left nil intentionally.
	var statusCalled atomic.Bool
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		statusCalled.Store(true)
	})
	go collector.Run()
	defer collector.Stop()

	deadline := time.After(200 * time.Millisecond)
	for !statusCalled.Load() {
		select {
		case <-deadline:
			t.Fatal("status callback never fired with nil AssignmentSource")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestBlockAssign_CollectorUsesAuthoritativeLifecycle verifies the heartbeat
// collector now drives the full BlockService assignment path, not the store-only
// role path. A replica assignment must start the receiver and close publish
// readiness.
func TestBlockAssign_CollectorUsesAuthoritativeLifecycle(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	collector := NewBlockVolumeHeartbeatCollector(bs, 5*time.Millisecond)
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		return []blockvol.BlockVolumeAssignment{{
			Path:            path,
			Epoch:           1,
			Role:            uint32(blockvol.RoleReplica),
			ReplicaDataAddr: ":0",
			ReplicaCtrlAddr: ":0",
		}}
	})
	go collector.Run()
	defer collector.Stop()

	deadline := time.After(500 * time.Millisecond)
	for {
		dataAddr, ctrlAddr := bs.GetReplState(path)
		readiness := bs.ReadinessSnapshot(path)
		if dataAddr != "" && ctrlAddr != "" && readiness.ReceiverReady && readiness.PublishHealthy {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("collector did not start replica receiver: data=%q ctrl=%q readiness=%+v", dataAddr, ctrlAddr, readiness)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestBlockAssign_MixedBatch verifies a batch with 1 success, 1 unknown volume,
// and 1 invalid transition returns parallel errors correctly.
func TestBlockAssign_MixedBatch(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	assignments := []blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: uint32(blockvol.RolePrimary), LeaseTtlMs: 30000},
		{Path: "/nonexistent/vol.blk", Epoch: 1, Role: uint32(blockvol.RolePrimary)},
		{Path: path, Epoch: 2, Role: uint32(blockvol.RoleReplica)}, // Primary->Replica is invalid
	}
	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	if len(errs) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(errs))
	}
	if errs[0] != nil {
		t.Fatalf("assignment 0: expected nil error, got %v", errs[0])
	}
	if errs[1] == nil {
		t.Fatal("assignment 1: expected error for unknown volume")
	}
	if errs[2] == nil {
		t.Fatal("assignment 2: expected error for invalid transition")
	}

	// Volume should still be Primary from assignment 0.
	vol, _ := bs.Store().GetBlockVolume(path)
	if vol.Role() != blockvol.RolePrimary {
		t.Fatalf("expected Primary role after mixed batch, got %s", vol.Role())
	}
}

// TestBlockAssign_RoleNoneIgnored verifies a RoleNone (0) assignment is a
// no-op same-role refresh -- no crash, no write gate opened.
func TestBlockAssign_RoleNoneIgnored(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	assignments := []blockvol.BlockVolumeAssignment{{
		Path:  path,
		Epoch: 0,
		Role:  uint32(blockvol.RoleNone),
	}}
	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	if errs[0] != nil {
		t.Fatalf("expected nil error for RoleNone assignment, got %v", errs[0])
	}

	// Volume should still be RoleNone.
	vol, _ := bs.Store().GetBlockVolume(path)
	if vol.Role() != blockvol.RoleNone {
		t.Fatalf("expected RoleNone, got %s", vol.Role())
	}
}
