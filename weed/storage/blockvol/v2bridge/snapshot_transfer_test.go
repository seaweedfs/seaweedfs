package v2bridge

import (
	"os"
	"path/filepath"
	"testing"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 09 P2: Snapshot execution closure (snapshot_tail)
//
// Proofs:
//   1. Component: TCP snapshot transfer + exact boundary install
//   2. One-chain: engine plan → RebuildExecutor → TransferSnapshot → tail replay → InSync
//   3. Boundary-drift: checkpoint advances after plan → fail closed
//   4. Convergence: post-import runtime = snapshotLSN, post-tail = targetLSN
//   5. Cleanup: temp snapshot released on all paths
// ============================================================

// setupSnapshotPrimary creates a primary vol with data, flushes it, and
// returns the vol + its checkpoint LSN (= snapshot boundary for snapshot_tail).
func setupSnapshotPrimary(t *testing.T, dir string, lbaCount int) (*blockvol.BlockVol, uint64) {
	t.Helper()
	vol := createTestVolNamed(t, dir, "primary.blockvol")

	for i := 0; i < lbaCount; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	vol.ForceFlush()

	state := NewReader(vol).ReadState()
	return vol, state.CheckpointLSN
}

// --- Component Proof: TCP snapshot transfer + exact boundary ---

func TestP2_TransferSnapshot_RealTCP(t *testing.T) {
	dir := t.TempDir()

	primaryVol, checkpointLSN := setupSnapshotPrimary(t, dir, 20)
	defer primaryVol.Close()
	t.Logf("primary checkpoint: %d", checkpointLSN)

	if checkpointLSN == 0 {
		t.Fatal("checkpoint must be > 0 after flush")
	}

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	executor := NewExecutor(replicaVol, rebuildServer.Addr())

	// Transfer snapshot at exact checkpointLSN.
	if err := executor.TransferSnapshot(checkpointLSN); err != nil {
		t.Fatalf("TransferSnapshot: %v", err)
	}

	// Verify: replica data matches primary at the snapshot boundary.
	verifyLBAMatch(t, primaryVol, replicaVol, 20)

	// Verify: local runtime converged to exact snapshotLSN.
	replicaState := NewReader(replicaVol).ReadState()
	if replicaState.CheckpointLSN != checkpointLSN {
		t.Fatalf("checkpoint: got %d, want %d", replicaState.CheckpointLSN, checkpointLSN)
	}
	if replicaState.WALHeadLSN != checkpointLSN {
		t.Fatalf("WALHeadLSN: got %d, want %d", replicaState.WALHeadLSN, checkpointLSN)
	}

	t.Logf("P2 component: snapshot transferred at exact BaseLSN=%d, runtime converged", checkpointLSN)
}

// --- One-Chain Proof: engine → TransferSnapshot → tail replay → InSync ---

func TestP2_SnapshotTailRebuild_OneChain(t *testing.T) {
	dir := t.TempDir()

	// Primary: write 10 entries, flush (creates checkpoint), then write 5 more (tail).
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryVol.ForceFlush()
	checkpointLSN := NewReader(primaryVol).ReadState().CheckpointLSN
	t.Logf("checkpoint after first flush: %d", checkpointLSN)

	// Write tail entries AFTER checkpoint.
	for i := 10; i < 15; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryState := NewReader(primaryVol).ReadState()
	t.Logf("primary: head=%d tail=%d committed=%d checkpoint=%d",
		primaryState.WALHeadLSN, primaryState.WALTailLSN,
		primaryState.CommittedLSN, primaryState.CheckpointLSN)

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// Engine setup: StorageAdapter reads from PRIMARY with TRUSTED checkpoint.
	// This forces snapshot_tail path (checkpoint is trusted + replayable tail).
	primaryReader := NewReader(primaryVol)
	primaryPinner := NewPinner(primaryVol)
	sa := bridge.NewStorageAdapter(
		&readerShim{primaryReader},
		&pinnerShim{primaryPinner},
	)
	ca := bridge.NewControlAdapter()
	driver := engine.NewRecoveryDriver(sa)

	// Assignment + plan.
	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent)

	plan, _ := driver.PlanRecovery("vol1/vs2", 0)
	if plan.Outcome != engine.OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s", plan.Outcome)
	}

	rebuildIntent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "rebuilding",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
		},
	)
	driver.Orchestrator.ProcessAssignment(rebuildIntent)

	rebuildPlan, err := driver.PlanRebuild("vol1/vs2")
	if err != nil {
		t.Fatalf("PlanRebuild: %v", err)
	}

	if rebuildPlan.RebuildSource != engine.RebuildSnapshotTail {
		t.Fatalf("source=%s, want snapshot_tail", rebuildPlan.RebuildSource)
	}
	t.Logf("plan: source=%s snapshot=%d target=%d",
		rebuildPlan.RebuildSource, rebuildPlan.RebuildSnapshotLSN, rebuildPlan.RebuildTargetLSN)

	// Execute: single executor handles BOTH TransferSnapshot and
	// StreamWALEntries. When rebuildAddr is set, StreamWALEntries connects
	// to the primary via TCP and applies entries to the local replica.
	// No test shim needed — this is the production path.
	replicaExecutor := NewExecutor(replicaVol, rebuildServer.Addr())

	exec := engine.NewRebuildExecutor(driver, rebuildPlan)
	exec.IO = replicaExecutor

	if err := exec.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Verify sender state → InSync.
	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if s.State() != engine.StateInSync {
		t.Fatalf("state=%s, want InSync", s.State())
	}

	// Verify pins released.
	if primaryPinner.ActiveHoldCount() != 0 {
		t.Fatalf("%d pins leaked", primaryPinner.ActiveHoldCount())
	}

	// Verify all 15 LBAs match (10 from snapshot + 5 from tail replay).
	verifyLBAMatch(t, primaryVol, replicaVol, 15)

	// Verify observability.
	events := driver.Orchestrator.Log.EventsFor("vol1/vs2")
	hasStarted, hasCompleted := false, false
	for _, ev := range events {
		if ev.Event == "exec_rebuild_started" {
			hasStarted = true
		}
		if ev.Event == "exec_rebuild_completed" {
			hasCompleted = true
		}
	}
	if !hasStarted || !hasCompleted {
		t.Fatalf("observability: started=%v completed=%v", hasStarted, hasCompleted)
	}

	t.Log("P2 one-chain: plan(snapshot_tail) → TransferSnapshot → tail replay → InSync → data verified")
}

// --- Boundary-drift: checkpoint advances after plan → fail closed ---

func TestP2_TransferSnapshot_BoundaryDrift(t *testing.T) {
	dir := t.TempDir()

	primaryVol, checkpointLSN := setupSnapshotPrimary(t, dir, 10)
	defer primaryVol.Close()

	// Write more + flush → checkpoint advances past the original value.
	for i := 10; i < 20; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('X')))
	}
	primaryVol.ForceFlush()

	newCheckpoint := NewReader(primaryVol).ReadState().CheckpointLSN
	t.Logf("checkpoint advanced: %d → %d", checkpointLSN, newCheckpoint)
	if newCheckpoint == checkpointLSN {
		t.Fatal("checkpoint must have advanced for boundary-drift test")
	}

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	executor := NewExecutor(replicaVol, rebuildServer.Addr())

	// Request snapshot at OLD checkpoint → server should reject (boundary mismatch).
	err = executor.TransferSnapshot(checkpointLSN)
	if err == nil {
		t.Fatal("should fail: checkpoint advanced past requested boundary")
	}
	t.Logf("boundary drift: %v", err)
}

// --- Fail-Closed: no rebuild address ---

func TestP2_TransferSnapshot_NoAddress(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	executor := NewExecutor(vol, "")
	err := executor.TransferSnapshot(10)
	if err == nil {
		t.Fatal("should fail without rebuild address")
	}
}

// --- Convergence: post-import runtime state ---

func TestP2_TransferSnapshot_RuntimeConvergence(t *testing.T) {
	dir := t.TempDir()

	primaryVol, checkpointLSN := setupSnapshotPrimary(t, dir, 20)
	defer primaryVol.Close()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Replica: has stale higher state (like the P1 stale-higher test).
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	for i := 0; i < 30; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('R')))
	}

	if err := replicaVol.StartReplicaReceiver("127.0.0.1:0", "127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplicaReceiver: %v", err)
	}

	staleState := NewReader(replicaVol).ReadState()
	staleRecv := replicaVol.ReceivedLSN()
	t.Logf("replica before: head=%d receivedLSN=%d", staleState.WALHeadLSN, staleRecv)

	executor := NewExecutor(replicaVol, rebuildServer.Addr())
	if err := executor.TransferSnapshot(checkpointLSN); err != nil {
		t.Fatalf("TransferSnapshot: %v", err)
	}

	// All runtime state must converge to checkpointLSN (exact, not conservative).
	postState := NewReader(replicaVol).ReadState()
	postRecv := replicaVol.ReceivedLSN()

	if postState.WALHeadLSN != checkpointLSN {
		t.Fatalf("WALHeadLSN=%d != checkpointLSN=%d", postState.WALHeadLSN, checkpointLSN)
	}
	if postState.CheckpointLSN != checkpointLSN {
		t.Fatalf("CheckpointLSN=%d != snapshotLSN=%d", postState.CheckpointLSN, checkpointLSN)
	}
	if postRecv != checkpointLSN {
		t.Fatalf("receivedLSN=%d != snapshotLSN=%d", postRecv, checkpointLSN)
	}

	t.Logf("convergence: staleHead=%d staleRecv=%d → all converged to %d",
		staleState.WALHeadLSN, staleRecv, checkpointLSN)
}

// --- Temp snapshot cleanup verification ---

func TestP2_TransferSnapshot_TempSnapshotCleaned(t *testing.T) {
	dir := t.TempDir()

	primaryVol, checkpointLSN := setupSnapshotPrimary(t, dir, 10)
	defer primaryVol.Close()

	snapsBefore := primaryVol.ListSnapshots()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	executor := NewExecutor(replicaVol, rebuildServer.Addr())
	if err := executor.TransferSnapshot(checkpointLSN); err != nil {
		t.Fatalf("TransferSnapshot: %v", err)
	}

	// Verify: no leaked temp snapshots on primary after transfer.
	snapsAfter := primaryVol.ListSnapshots()
	if len(snapsAfter) != len(snapsBefore) {
		// Find the leaked snapshot.
		leaked := []uint32{}
		beforeSet := map[uint32]bool{}
		for _, s := range snapsBefore {
			beforeSet[s.ID] = true
		}
		for _, s := range snapsAfter {
			if !beforeSet[s.ID] {
				leaked = append(leaked, s.ID)
			}
		}
		t.Fatalf("temp snapshot leaked: before=%d after=%d leaked=%v",
			len(snapsBefore), len(snapsAfter), leaked)
	}

	// Also check on failure path: request with wrong boundary.
	for i := 10; i < 15; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('X')))
	}
	primaryVol.ForceFlush()

	subDir := filepath.Join(dir, "sub")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	r2Vol := createTestVolNamed(t, subDir, "r2.blockvol")
	defer r2Vol.Close()
	executor2 := NewExecutor(r2Vol, rebuildServer.Addr())
	// This should fail (boundary drift).
	_ = executor2.TransferSnapshot(checkpointLSN)

	// Verify: still no leaked snapshots after failure.
	snapsAfterFail := primaryVol.ListSnapshots()
	if len(snapsAfterFail) != len(snapsBefore) {
		t.Fatalf("temp snapshot leaked after failure: before=%d after=%d",
			len(snapsBefore), len(snapsAfterFail))
	}

	t.Log("P2 cleanup: temp snapshots cleaned on success and failure paths")
}
