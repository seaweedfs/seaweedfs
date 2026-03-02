package weed_server

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ---------------------------------------------------------------------------
// QA adversarial tests for CP4b-3: Assignment Processing
// Targets: ProcessBlockVolumeAssignments, processAssignments, safe wrappers
//
// Bugs found:
// BUG-CP4B3-1 [Medium]: TOCTOU in ProcessBlockVolumeAssignments -- volume
//   can be removed between GetBlockVolume and HandleAssignment.
// BUG-CP4B3-2 [Low]: Data race on callback fields -- fixed with cbMu +
//   SetXxx methods.
// ---------------------------------------------------------------------------

func TestQABlockAssignmentProcessing(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// --- Source / callback panic recovery ---
		{name: "source_panic_recovery", run: testSourcePanicRecovery},
		{name: "callback_panic_recovery", run: testAssignCallbackPanicRecovery},

		// --- Source edge cases ---
		{name: "source_returns_nil", run: testSourceReturnsNil},
		{name: "source_returns_empty", run: testSourceReturnsEmpty},
		{name: "slow_source_blocks_tick", run: testSlowSourceBlocksTick},
		{name: "source_set_after_run", run: testAssignSourceSetAfterRun},

		// --- Batch processing ---
		{name: "same_volume_batch_ordering", run: testSameVolumeBatchOrdering},
		{name: "unknown_role_in_assignment", run: testUnknownRoleInAssignment},
		{name: "large_batch_100_assignments", run: testLargeBatch100},
		{name: "batch_all_unknown_volumes", run: testBatchAllUnknownVolumes},

		// --- Integration: collector + assignments ---
		{name: "stop_during_slow_assignment", run: testStopDuringSlowAssignment},
		{name: "assignment_and_status_both_fire", run: testAssignmentAndStatusBothFire},
		{name: "assignment_callback_receives_errs", run: testAssignmentCallbackReceivesErrs},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// ---------------------------------------------------------------------------
// Source / callback panic recovery
// ---------------------------------------------------------------------------

// testSourcePanicRecovery verifies that a panicking AssignmentSource doesn't
// kill the collector. Status callbacks should continue after the panic.
func testSourcePanicRecovery(t *testing.T) {
	bs := newTestBlockService(t)
	var statusCount atomic.Int64
	var sourceCalls atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		statusCount.Add(1)
	})
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		n := sourceCalls.Add(1)
		if n == 1 {
			panic("test: source panic")
		}
		return nil // subsequent calls return empty
	})
	go collector.Run()
	defer collector.Stop()

	// Wait for post-panic ticks (status callbacks should keep firing).
	deadline := time.After(500 * time.Millisecond)
	for statusCount.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("collector died after source panic: only %d status callbacks", statusCount.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if sourceCalls.Load() < 2 {
		t.Errorf("expected AssignmentSource called again after panic, got %d calls", sourceCalls.Load())
	}
}

// testAssignCallbackPanicRecovery verifies that a panicking AssignmentCallback
// doesn't kill the collector.
func testAssignCallbackPanicRecovery(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	var statusCount atomic.Int64
	var cbCalls atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		statusCount.Add(1)
	})
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		// Always return a valid promote-to-Primary assignment.
		// After first call, HandleAssignment will return "same role" (no-op or error),
		// but that's fine -- we're testing the callback panic, not the assignment.
		return []blockvol.BlockVolumeAssignment{{
			Path:       path,
			Epoch:      1,
			Role:       uint32(blockvol.RolePrimary),
			LeaseTtlMs: 30000,
		}}
	})
	collector.SetAssignmentCallback(func(a []blockvol.BlockVolumeAssignment, errs []error) {
		n := cbCalls.Add(1)
		if n == 1 {
			panic("test: callback panic")
		}
	})

	go collector.Run()
	defer collector.Stop()

	deadline := time.After(500 * time.Millisecond)
	for statusCount.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("collector died after callback panic: only %d status callbacks", statusCount.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if cbCalls.Load() < 2 {
		t.Errorf("expected AssignmentCallback called again after panic, got %d calls", cbCalls.Load())
	}
}

// ---------------------------------------------------------------------------
// Source edge cases
// ---------------------------------------------------------------------------

// testSourceReturnsNil verifies that AssignmentSource returning nil
// doesn't cause ProcessBlockVolumeAssignments to be called.
func testSourceReturnsNil(t *testing.T) {
	bs := newTestBlockService(t)
	var processAttempted atomic.Bool

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		return nil
	})
	// If ProcessBlockVolumeAssignments were called with nil,
	// it returns a nil slice (len 0) -- harmless. But the collector
	// short-circuits on len==0 before calling Process. We verify
	// by ensuring AssignmentCallback is never called.
	collector.SetAssignmentCallback(func(a []blockvol.BlockVolumeAssignment, errs []error) {
		processAttempted.Store(true)
	})

	go collector.Run()
	time.Sleep(50 * time.Millisecond)
	collector.Stop()

	if processAttempted.Load() {
		t.Error("AssignmentCallback called when source returned nil -- should short-circuit")
	}
}

// testSourceReturnsEmpty verifies that an empty slice from AssignmentSource
// is handled the same as nil (no processing).
func testSourceReturnsEmpty(t *testing.T) {
	bs := newTestBlockService(t)
	var processAttempted atomic.Bool

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		return []blockvol.BlockVolumeAssignment{}
	})
	collector.SetAssignmentCallback(func(a []blockvol.BlockVolumeAssignment, errs []error) {
		processAttempted.Store(true)
	})

	go collector.Run()
	time.Sleep(50 * time.Millisecond)
	collector.Stop()

	if processAttempted.Load() {
		t.Error("AssignmentCallback called when source returned empty -- should short-circuit")
	}
}

// testSlowSourceBlocksTick verifies that a slow AssignmentSource blocks the
// entire collector tick (status + assignment are sequential in the same tick).
func testSlowSourceBlocksTick(t *testing.T) {
	bs := newTestBlockService(t)
	var statusCount atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 5*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		statusCount.Add(1)
	})
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		time.Sleep(50 * time.Millisecond) // 10x the tick interval
		return nil
	})

	go collector.Run()
	time.Sleep(200 * time.Millisecond)
	collector.Stop()

	// With 50ms source sleep, expect ~4 status callbacks (200ms/50ms), not 40.
	n := statusCount.Load()
	if n > 10 {
		t.Errorf("expected slow source to throttle ticks, got %d status callbacks", n)
	}
	if n < 1 {
		t.Error("expected at least 1 status callback")
	}
	t.Logf("slow source: %d status callbacks in 200ms (5ms interval, 50ms source)", n)
}

// testAssignSourceSetAfterRun verifies that setting AssignmentSource after
// Run() started still picks up assignments on subsequent ticks.
func testAssignSourceSetAfterRun(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {})
	// AssignmentSource left nil initially.

	go collector.Run()
	defer collector.Stop()

	// Let a few nil-source ticks fire.
	time.Sleep(30 * time.Millisecond)

	// Now set the source + callback.
	var cbCalled atomic.Bool
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		return []blockvol.BlockVolumeAssignment{{
			Path:       path,
			Epoch:      1,
			Role:       uint32(blockvol.RolePrimary),
			LeaseTtlMs: 30000,
		}}
	})
	collector.SetAssignmentCallback(func(a []blockvol.BlockVolumeAssignment, errs []error) {
		cbCalled.Store(true)
	})

	deadline := time.After(200 * time.Millisecond)
	for !cbCalled.Load() {
		select {
		case <-deadline:
			t.Fatal("AssignmentCallback never fired after setting source post-Run")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// ---------------------------------------------------------------------------
// Batch processing
// ---------------------------------------------------------------------------

// testSameVolumeBatchOrdering verifies that when the same volume appears
// multiple times in a batch, assignments are applied sequentially in order.
// HandleAssignment: Primary->Stale (demote does Primary->Draining->Stale internally).
func testSameVolumeBatchOrdering(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	// Batch: promote to Primary (epoch 1), then demote to Stale (epoch 2).
	// Order matters -- first must run before second.
	assignments := []blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: uint32(blockvol.RolePrimary), LeaseTtlMs: 30000},
		{Path: path, Epoch: 2, Role: uint32(blockvol.RoleStale)},
	}
	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	if errs[0] != nil {
		t.Fatalf("assignment 0 (promote): %v", errs[0])
	}
	if errs[1] != nil {
		t.Fatalf("assignment 1 (demote): %v", errs[1])
	}

	vol, _ := bs.Store().GetBlockVolume(path)
	if vol.Role() != blockvol.RoleStale {
		t.Errorf("expected Stale after sequential batch, got %s", vol.Role())
	}
}

// testUnknownRoleInAssignment verifies that a wire role value > maxValidRole
// is mapped to RoleNone by RoleFromWire. If the volume is already RoleNone,
// HandleAssignment with RoleNone is a no-op (same-role refresh).
func testUnknownRoleInAssignment(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	assignments := []blockvol.BlockVolumeAssignment{{
		Path:  path,
		Epoch: 0,
		Role:  255, // unknown -- RoleFromWire maps to RoleNone
	}}
	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	// RoleNone -> RoleNone is a no-op, should succeed.
	if errs[0] != nil {
		t.Fatalf("expected nil error for unknown role mapped to RoleNone, got: %v", errs[0])
	}

	vol, _ := bs.Store().GetBlockVolume(path)
	if vol.Role() != blockvol.RoleNone {
		t.Errorf("expected RoleNone, got %s", vol.Role())
	}
}

// testLargeBatch100 verifies that 100 assignments in a single batch all
// target the same (valid) volume path. The first promotes to Primary,
// and subsequent 99 are same-role refreshes (same epoch).
func testLargeBatch100(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	assignments := make([]blockvol.BlockVolumeAssignment, 100)
	assignments[0] = blockvol.BlockVolumeAssignment{
		Path: path, Epoch: 1, Role: uint32(blockvol.RolePrimary), LeaseTtlMs: 30000,
	}
	for i := 1; i < 100; i++ {
		assignments[i] = blockvol.BlockVolumeAssignment{
			Path: path, Epoch: 1, Role: uint32(blockvol.RolePrimary), LeaseTtlMs: 30000,
		}
	}

	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	if len(errs) != 100 {
		t.Fatalf("expected 100 error slots, got %d", len(errs))
	}
	// First should succeed (promote).
	if errs[0] != nil {
		t.Fatalf("assignment 0: %v", errs[0])
	}
	// Remaining are same-role refreshes -- should all succeed.
	for i := 1; i < 100; i++ {
		if errs[i] != nil {
			t.Errorf("assignment %d: unexpected error: %v", i, errs[i])
		}
	}
}

// testBatchAllUnknownVolumes verifies that a batch where every assignment
// targets a nonexistent volume returns all errors but doesn't panic.
func testBatchAllUnknownVolumes(t *testing.T) {
	bs := newTestBlockService(t)

	assignments := make([]blockvol.BlockVolumeAssignment, 5)
	for i := range assignments {
		assignments[i] = blockvol.BlockVolumeAssignment{
			Path:  "/nonexistent/vol.blk",
			Epoch: 1,
			Role:  uint32(blockvol.RolePrimary),
		}
	}

	errs := bs.Store().ProcessBlockVolumeAssignments(assignments)
	for i, err := range errs {
		if err == nil {
			t.Errorf("assignment %d: expected error for unknown volume", i)
		} else if !strings.Contains(err.Error(), "not found") {
			t.Errorf("assignment %d: expected 'not found', got: %v", i, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Integration: collector + assignments
// ---------------------------------------------------------------------------

// testStopDuringSlowAssignment verifies that Stop() waits for a slow
// assignment source to complete before returning.
func testStopDuringSlowAssignment(t *testing.T) {
	bs := newTestBlockService(t)
	var sourceEntered atomic.Bool
	var sourceFinished atomic.Bool

	collector := NewBlockVolumeHeartbeatCollector(bs, 5*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {})
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		sourceEntered.Store(true)
		time.Sleep(80 * time.Millisecond)
		sourceFinished.Store(true)
		return nil
	})

	go collector.Run()

	// Wait for source to enter.
	deadline := time.After(2 * time.Second)
	for !sourceEntered.Load() {
		select {
		case <-deadline:
			collector.Stop()
			t.Fatal("source never entered")
			return
		case <-time.After(time.Millisecond):
		}
	}

	// Stop should block until the current tick (including slow source) finishes.
	collector.Stop()
	if !sourceFinished.Load() {
		t.Error("Stop() returned before slow AssignmentSource finished")
	}
}

// testAssignmentAndStatusBothFire verifies that both StatusCallback and
// AssignmentCallback fire on the same tick.
func testAssignmentAndStatusBothFire(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	var statusCount atomic.Int64
	var assignCount atomic.Int64

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {
		statusCount.Add(1)
	})
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		return []blockvol.BlockVolumeAssignment{{
			Path:       path,
			Epoch:      1,
			Role:       uint32(blockvol.RolePrimary),
			LeaseTtlMs: 30000,
		}}
	})
	collector.SetAssignmentCallback(func(a []blockvol.BlockVolumeAssignment, errs []error) {
		assignCount.Add(1)
	})

	go collector.Run()
	defer collector.Stop()

	deadline := time.After(500 * time.Millisecond)
	for statusCount.Load() < 3 || assignCount.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("expected >=3 each, got status=%d assign=%d",
				statusCount.Load(), assignCount.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// testAssignmentCallbackReceivesErrs verifies that the AssignmentCallback
// receives the correct parallel error slice from ProcessBlockVolumeAssignments.
func testAssignmentCallbackReceivesErrs(t *testing.T) {
	bs := newTestBlockService(t)
	path := testBlockVolPath(t, bs)

	var mu sync.Mutex
	var gotAssignments []blockvol.BlockVolumeAssignment
	var gotErrs []error
	received := make(chan struct{}, 1)

	collector := NewBlockVolumeHeartbeatCollector(bs, 10*time.Millisecond)
	collector.SetStatusCallback(func(msgs []blockvol.BlockVolumeInfoMessage) {})
	collector.SetAssignmentSource(func() []blockvol.BlockVolumeAssignment {
		// Mixed batch: 1 valid + 1 unknown
		return []blockvol.BlockVolumeAssignment{
			{Path: path, Epoch: 1, Role: uint32(blockvol.RolePrimary), LeaseTtlMs: 30000},
			{Path: "/nonexistent/vol.blk", Epoch: 1, Role: uint32(blockvol.RolePrimary)},
		}
	})
	collector.SetAssignmentCallback(func(a []blockvol.BlockVolumeAssignment, errs []error) {
		mu.Lock()
		if gotAssignments == nil {
			gotAssignments = a
			gotErrs = errs
		}
		mu.Unlock()
		select {
		case received <- struct{}{}:
		default:
		}
	})

	go collector.Run()
	defer collector.Stop()

	select {
	case <-received:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("AssignmentCallback never fired")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(gotAssignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(gotAssignments))
	}
	if len(gotErrs) != 2 {
		t.Fatalf("expected 2 errors, got %d", len(gotErrs))
	}
	if gotErrs[0] != nil {
		t.Errorf("assignment 0: expected nil error, got %v", gotErrs[0])
	}
	if gotErrs[1] == nil {
		t.Error("assignment 1: expected error for unknown volume")
	} else if !strings.Contains(gotErrs[1].Error(), "not found") {
		t.Errorf("assignment 1: expected 'not found', got: %v", gotErrs[1])
	}
}
