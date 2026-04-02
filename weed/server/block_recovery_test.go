package weed_server

import (
	"path/filepath"
	"testing"
	"time"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

// ============================================================
// Phase 09 P4: Stronger live runtime ownership
//
// Proofs:
//   1. Live-path with real vol: ProcessAssignments → plan → executor
//   2. Serialized replacement: old drained before new starts
//   3. Shutdown drains all tasks
//   4. Rebuild address scoped
//   5. No split ownership under replacement
// ============================================================

func createTestBlockServiceWithVol(t *testing.T) (*BlockService, string) {
	t.Helper()
	dir := t.TempDir()

	// Create a real blockvol.
	volPath := filepath.Join(dir, "vol1.blk")
	vol, err := blockvol.CreateBlockVol(volPath, blockvol.CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	vol.Close()

	// Build BlockService with a real BlockVolumeStore.
	store := storage.NewBlockVolumeStore()
	if _, err := store.AddBlockVolume(volPath, ""); err != nil {
		t.Fatalf("AddBlockVolume: %v", err)
	}

	bs := &BlockService{
		blockStore:     store,
		blockDir:       dir,
		listenAddr:     "127.0.0.1:3260",
		localServerID:  "test-server-1",
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
	}
	bs.v2Recovery = NewRecoveryManager(bs)

	t.Cleanup(func() {
		bs.v2Recovery.Shutdown()
		store.Close()
	})

	return bs, volPath
}

// --- Live-path with real vol: reaches planning ---

func TestP4_LivePath_RealVol_ReachesPlan(t *testing.T) {
	bs, volPath := createTestBlockServiceWithVol(t)

	// Write data to the vol so recovery has something to plan against.
	if err := bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			vol.WriteLBA(uint64(i), make([]byte, 4096))
		}
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Process assignment through real path.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2",
			ReplicaDataAddr: "10.0.0.2:9333",
			ReplicaCtrlAddr: "10.0.0.2:9334",
		},
	})

	// Give recovery goroutine time to reach planning.
	time.Sleep(200 * time.Millisecond)

	// Verify: sender exists and engine processed it.
	replicaID := volPath + "/vs2"
	s := bs.v2Orchestrator.Registry.Sender(replicaID)
	if s == nil {
		t.Fatal("sender not created")
	}

	// Assert the full chain completed: plan → executor → in_sync.
	events := bs.v2Orchestrator.Log.EventsFor(replicaID)
	required := map[string]bool{
		"plan_catchup":         false,
		"exec_catchup_started": false,
		"exec_completed":       false,
	}
	for _, ev := range events {
		if _, ok := required[ev.Event]; ok {
			required[ev.Event] = true
		}
	}
	for event, found := range required {
		if !found {
			t.Fatalf("missing required event: %s (events=%d)", event, len(events))
		}
	}

	// Assert final sender state.
	if s.State() != engine.StateInSync {
		t.Fatalf("sender state=%s, want in_sync", s.State())
	}

	t.Log("P4 live-path: ProcessAssignments → plan_catchup → exec_catchup_started → exec_completed → in_sync")
}

// --- Serialized replacement: old drained before new starts ---

func TestP4_SerializedReplacement_DrainsBeforeStart(t *testing.T) {
	bs, volPath := createTestBlockServiceWithVol(t)
	rm := bs.v2Recovery

	// Write data so recovery has work to do.
	if err := bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 10; i++ {
			vol.WriteLBA(uint64(i), make([]byte, 4096))
		}
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Hook: block the first recovery goroutine so it is DEFINITELY still
	// alive when the supersede arrives. Release it via channel.
	holdFirst := make(chan struct{})
	firstReached := make(chan struct{}, 1)
	callCount := 0
	rm.OnBeforeExecute = func(replicaID string) {
		callCount++
		if callCount == 1 {
			// First call: signal that we're alive, then block.
			firstReached <- struct{}{}
			<-holdFirst // block until test releases
		}
		// Second call (replacement): proceed immediately.
	}

	replicaID := volPath + "/vs2"

	// Epoch 1: start recovery goroutine (will block at OnBeforeExecute).
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: volPath, Epoch: 1, Role: uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2", ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334"},
	})

	// Wait for the first goroutine to reach the hook (still alive).
	select {
	case <-firstReached:
		t.Log("first recovery goroutine is alive and blocked at OnBeforeExecute")
	case <-time.After(5 * time.Second):
		t.Fatal("first goroutine did not reach OnBeforeExecute")
	}

	// Capture the old task's done channel WHILE it is still running.
	rm.mu.Lock()
	oldTask := rm.tasks[replicaID]
	rm.mu.Unlock()
	if oldTask == nil {
		t.Fatal("old task must exist while goroutine is blocked")
	}
	oldDone := oldTask.done

	// Verify: old done channel is NOT closed yet.
	select {
	case <-oldDone:
		t.Fatal("old task done should NOT be closed yet")
	default:
		t.Log("confirmed: old task still running (done channel open)")
	}

	// Epoch 2: supersede. cancelAndDrain will cancel the old context and
	// block on oldDone. We release the hold from another goroutine so
	// cancelAndDrain can complete.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(holdFirst) // release the blocked first goroutine
	}()

	// This call blocks inside cancelAndDrain until the old goroutine exits.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: volPath, Epoch: 2, Role: uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2", ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334"},
	})

	// After ProcessAssignments returns, the old goroutine MUST be drained.
	select {
	case <-oldDone:
		t.Log("confirmed: old task drained (done channel closed) before replacement started")
	default:
		t.Fatal("old task done channel still open after ProcessAssignments returned")
	}

	time.Sleep(200 * time.Millisecond)

	// At most 1 active task.
	count := rm.ActiveTaskCount()
	if count > 1 {
		t.Fatalf("overlap: %d tasks after replacement", count)
	}

	t.Log("P4 serialized: old goroutine blocked → supersede → drain waited → old exited → replacement started")
}

// --- Shutdown drains all ---

func TestP4_ShutdownDrain(t *testing.T) {
	bs, volPath := createTestBlockServiceWithVol(t)
	rm := bs.v2Recovery

	// Hook: block the goroutine so it's alive when shutdown arrives.
	holdTask := make(chan struct{})
	taskReached := make(chan struct{}, 1)
	rm.OnBeforeExecute = func(replicaID string) {
		taskReached <- struct{}{}
		<-holdTask
	}

	// Write data so recovery has work.
	if err := bs.blockStore.WithVolume(volPath, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			vol.WriteLBA(uint64(i), make([]byte, 4096))
		}
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: volPath, Epoch: 1, Role: uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2", ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334"},
	})

	// Wait for task to be alive.
	select {
	case <-taskReached:
		t.Log("task is alive and blocked before shutdown")
	case <-time.After(5 * time.Second):
		t.Fatal("task did not reach hook")
	}

	// Assert: a live task exists.
	if rm.ActiveTaskCount() == 0 {
		t.Fatal("expected live task before shutdown")
	}

	// Release the blocked goroutine from another goroutine so Shutdown can drain.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(holdTask)
	}()

	done := make(chan bool, 1)
	go func() {
		rm.Shutdown()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete within 5 seconds")
	}

	if rm.ActiveTaskCount() != 0 {
		t.Fatalf("expected 0 active tasks, got %d", rm.ActiveTaskCount())
	}

	t.Log("P4 shutdown: live task existed → shutdown drained it → 0 active")
}

// --- Rebuild address scoped ---

func TestP4_RebuildAddrScoped(t *testing.T) {
	bs, _ := createTestBlockServiceWithVol(t)
	rm := bs.v2Recovery

	assignments := []blockvol.BlockVolumeAssignment{
		{Path: "/data/vol1.blk", RebuildAddr: "10.0.0.1:5000"},
		{Path: "/data/vol2.blk", RebuildAddr: "10.0.0.2:5000"},
	}

	if addr := rm.deriveRebuildAddr("/data/vol1.blk/vs2", assignments); addr != "10.0.0.1:5000" {
		t.Fatalf("vol1 addr=%s", addr)
	}
	if addr := rm.deriveRebuildAddr("/data/vol2.blk/vs3", assignments); addr != "10.0.0.2:5000" {
		t.Fatalf("vol2 addr=%s", addr)
	}
	if addr := rm.deriveRebuildAddr("/data/vol3.blk/vs4", assignments); addr != "" {
		t.Fatalf("vol3 addr=%s", addr)
	}
}
