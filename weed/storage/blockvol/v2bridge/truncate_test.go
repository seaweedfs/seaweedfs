package v2bridge

import (
	"testing"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// ============================================================
// Phase 09 P3: Truncation execution closure
//
// Proofs:
//   1. Component: TruncateWAL performs real local correction
//   2. One-chain: engine plan(replica ahead) → CatchUpExecutor → TruncateWAL → InSync
//   3. Exact-boundary: runtime converges to exactly truncateLSN
//   4. Stale-higher: active receiver ahead of truncation point is corrected
//   5. Fail-closed: truncation failure prevents completion
//   6. Adversarial: truncation then resumed catch-up from truncated boundary
// ============================================================

// --- Component Proof: TruncateWAL performs real local correction ---

func TestP3_TruncateWAL_RealCorrection(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Phase 1: Write 10 "base" entries, flush to extent.
	// These represent data the primary also has (shared truth).
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('B'))) // B = base
	}
	vol.ForceFlush()
	baseCheckpoint := NewReader(vol).ReadState().CheckpointLSN
	t.Logf("base flushed: checkpoint=%d", baseCheckpoint)

	// Phase 2: Write 10 MORE "ahead" entries WITHOUT flushing.
	// These represent entries the replica received but the primary didn't commit.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'))) // A = ahead (overwrites B)
	}

	stateBefore := NewReader(vol).ReadState()
	t.Logf("before truncation: head=%d checkpoint=%d", stateBefore.WALHeadLSN, stateBefore.CheckpointLSN)

	// Verify: reads return 'A' (ahead data from dirty map).
	data, _ := vol.ReadLBA(0, vol.Info().BlockSize)
	if data[0] != 'A' {
		t.Fatalf("pre-truncate: LBA 0 = %c, want A", data[0])
	}

	// Truncate to base checkpoint — discard ahead entries.
	executor := NewExecutor(vol, "")
	if err := executor.TruncateWAL(baseCheckpoint); err != nil {
		t.Fatalf("TruncateWAL: %v", err)
	}

	stateAfter := NewReader(vol).ReadState()
	t.Logf("after truncation: head=%d checkpoint=%d", stateAfter.WALHeadLSN, stateAfter.CheckpointLSN)

	// Exact boundary: WALHeadLSN must be at the truncation point.
	if stateAfter.WALHeadLSN != baseCheckpoint {
		t.Fatalf("WALHeadLSN=%d, want %d", stateAfter.WALHeadLSN, baseCheckpoint)
	}

	// DATA PROOF: reads must return 'B' (base data from extent), not 'A'.
	// The ahead entries were discarded from WAL without flushing, so
	// reads fall through dirty map → extent → base data.
	for i := 0; i < 10; i++ {
		data, err := vol.ReadLBA(uint64(i), vol.Info().BlockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != 'B' {
			t.Fatalf("LBA %d = %c, want B (base data after truncation)", i, data[0])
		}
	}

	// Verify: can write new entry after truncation (no gap).
	vol.WriteLBA(0, makeBlock('Z'))
	statePost := NewReader(vol).ReadState()
	expectedNext := baseCheckpoint + 1
	if statePost.WALHeadLSN != expectedNext {
		t.Fatalf("post-truncate write: head=%d, want %d", statePost.WALHeadLSN, expectedNext)
	}

	t.Logf("P3 component: truncated to %d — ahead data discarded, base data restored, next write at %d",
		baseCheckpoint, expectedNext)
}

// --- One-Chain Proof: engine → CatchUpExecutor → TruncateWAL → InSync ---

func TestP3_TruncateWAL_OneChain(t *testing.T) {
	dir := t.TempDir()

	// Primary: 10 entries with 'P' data, flush (committed=10).
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('P')))
	}
	primaryVol.ForceFlush()

	primaryState := NewReader(primaryVol).ReadState()
	t.Logf("primary: committed=%d", primaryState.CommittedLSN)

	// Replica: first 10 entries with 'P' (same as primary, flushed),
	// then 10 MORE with 'R' (ahead, NOT flushed).
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	for i := 0; i < 10; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('P'))) // shared base
	}
	replicaVol.ForceFlush()

	for i := 0; i < 10; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('R'))) // ahead (overwrites P)
	}
	replicaState := NewReader(replicaVol).ReadState()
	t.Logf("replica: head=%d (ahead of primary's %d)", replicaState.WALHeadLSN, primaryState.CommittedLSN)

	// Engine setup: StorageAdapter reads PRIMARY state.
	primaryReader := NewReader(primaryVol)
	primaryPinner := NewPinner(primaryVol)
	sa := bridge.NewStorageAdapter(
		&readerShim{primaryReader},
		&pinnerShim{primaryPinner},
	)
	ca := bridge.NewControlAdapter()
	driver := engine.NewRecoveryDriver(sa)

	// Assignment.
	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent)

	// Plan recovery with replica flushed at 20, primary committed at 10.
	// → replica_ahead_needs_truncation, TruncateLSN = 10.
	plan, err := driver.PlanRecovery("vol1/vs2", replicaState.WALHeadLSN)
	if err != nil {
		t.Fatalf("PlanRecovery: %v", err)
	}
	if plan.TruncateLSN == 0 {
		t.Fatalf("expected truncation plan, got TruncateLSN=0 (outcome=%s)", plan.Outcome)
	}
	t.Logf("plan: outcome=%s truncateLSN=%d", plan.Outcome, plan.TruncateLSN)

	// Execute: CatchUpExecutor with IO wired to replica vol.
	// The executor calls TruncateWAL (real correction on replica).
	replicaExecutor := NewExecutor(replicaVol, "")
	exec := engine.NewCatchUpExecutor(driver, plan)
	exec.IO = replicaExecutor

	if err := exec.Execute(nil, 0); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Verify sender → InSync.
	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if s.State() != engine.StateInSync {
		t.Fatalf("state=%s, want InSync", s.State())
	}

	// Verify: replica runtime converged to truncateLSN.
	replicaAfter := NewReader(replicaVol).ReadState()
	if replicaAfter.WALHeadLSN != plan.TruncateLSN {
		t.Fatalf("WALHeadLSN=%d != truncateLSN=%d", replicaAfter.WALHeadLSN, plan.TruncateLSN)
	}

	// DATA PROOF: replica data must match primary (base data 'P'),
	// not ahead data ('R'). The unflushed ahead entries were discarded.
	for i := 0; i < 10; i++ {
		data, err := replicaVol.ReadLBA(uint64(i), replicaVol.Info().BlockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != 'P' {
			t.Fatalf("LBA %d = %c, want P (primary's base data after truncation)", i, data[0])
		}
	}

	// Verify pins released.
	if primaryPinner.ActiveHoldCount() != 0 {
		t.Fatalf("%d pins leaked", primaryPinner.ActiveHoldCount())
	}

	// Verify observability.
	events := driver.Orchestrator.Log.EventsFor("vol1/vs2")
	hasTruncation := false
	for _, ev := range events {
		if ev.Event == "exec_truncation" {
			hasTruncation = true
		}
	}
	if !hasTruncation {
		t.Fatal("missing exec_truncation event")
	}

	t.Logf("P3 one-chain: plan(replica_ahead) → CatchUpExecutor → TruncateWAL(%d) → InSync → data matches primary",
		plan.TruncateLSN)
}

// --- Stale-higher: active receiver corrected down ---

func TestP3_TruncateWAL_ActiveReceiverCorrected(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Write 10 base entries, flush → checkpoint=10.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('B')))
	}
	vol.ForceFlush()

	// Write 10 ahead entries (NOT flushed).
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A')))
	}

	// Start receiver so receivedLSN reflects head (20).
	if err := vol.StartReplicaReceiver("127.0.0.1:0", "127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplicaReceiver: %v", err)
	}

	staleRecv := vol.ReceivedLSN()
	t.Logf("before: receivedLSN=%d", staleRecv)

	// Truncate to 10 (checkpoint == 10 == truncateLSN → safe).
	executor := NewExecutor(vol, "")
	if err := executor.TruncateWAL(10); err != nil {
		t.Fatalf("TruncateWAL: %v", err)
	}

	postRecv := vol.ReceivedLSN()
	if postRecv != 10 {
		t.Fatalf("receivedLSN=%d, want 10 (corrected down)", postRecv)
	}

	t.Logf("P3 active receiver: receivedLSN %d→%d — corrected to truncation boundary", staleRecv, postRecv)
}

// --- Adversarial: truncation then resumed catch-up from truncated boundary ---

func TestP3_TruncateWAL_ThenCatchUp(t *testing.T) {
	dir := t.TempDir()

	// Primary: 15 entries with 'P', flush.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 15; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('P')))
	}
	primaryVol.ForceFlush()

	primaryState := NewReader(primaryVol).ReadState()

	// Replica: first 15 entries with 'P' (shared base, flushed),
	// then 5 ahead entries with 'R' (NOT flushed).
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	for i := 0; i < 15; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('P')))
	}
	replicaVol.ForceFlush()

	for i := 0; i < 5; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('R'))) // ahead, overwrites P
	}

	executor := NewExecutor(replicaVol, "")
	if err := executor.TruncateWAL(primaryState.CommittedLSN); err != nil {
		t.Fatalf("TruncateWAL: %v", err)
	}

	// Verify truncation boundary.
	postState := NewReader(replicaVol).ReadState()
	if postState.WALHeadLSN != primaryState.CommittedLSN {
		t.Fatalf("post-truncate: head=%d, want %d", postState.WALHeadLSN, primaryState.CommittedLSN)
	}

	// DATA PROOF: replica data at truncated LBAs must be 'P' (base), not 'R' (ahead).
	for i := 0; i < 5; i++ {
		data, err := replicaVol.ReadLBA(uint64(i), replicaVol.Info().BlockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != 'P' {
			t.Fatalf("LBA %d = %c, want P (base data after truncation)", i, data[0])
		}
	}

	// Replica writes from next LSN should work (no gap).
	replicaVol.WriteLBA(0, makeBlock('T'))
	postWrite := NewReader(replicaVol).ReadState()
	expectedLSN := primaryState.CommittedLSN + 1
	if postWrite.WALHeadLSN != expectedLSN {
		t.Fatalf("post-truncate write: head=%d, want %d", postWrite.WALHeadLSN, expectedLSN)
	}

	t.Logf("P3 adversarial: truncate to %d → base data restored → next write at %d — no gap",
		primaryState.CommittedLSN, expectedLSN)
}

// --- Fail-closed: nil vol ---

func TestP3_TruncateWAL_NilVol(t *testing.T) {
	executor := &Executor{vol: nil}
	err := executor.TruncateWAL(10)
	if err == nil {
		t.Fatal("should fail with nil vol")
	}
}

// --- Flushed-ahead escalation: checkpoint > truncateLSN → error ---

func TestP3_TruncateWAL_FlushedAheadEscalates(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Write 10 base entries, flush.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('B')))
	}
	vol.ForceFlush()

	// Write 10 ahead entries AND FLUSH THEM — this contaminates the extent.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A')))
	}
	vol.ForceFlush()

	state := NewReader(vol).ReadState()
	t.Logf("flushed-ahead: head=%d checkpoint=%d", state.WALHeadLSN, state.CheckpointLSN)

	// Checkpoint is now 20 (all entries flushed). Truncation to 10 should
	// FAIL because checkpoint (20) > truncateLSN (10) — ahead data is in extent.
	executor := NewExecutor(vol, "")
	err := executor.TruncateWAL(10)
	if err == nil {
		t.Fatal("truncation should fail when ahead entries are already flushed to extent")
	}

	// Verify the error wraps ErrTruncationUnsafe.
	if !containsSubstring(err.Error(), "truncation unsafe") {
		t.Fatalf("error should indicate truncation unsafe, got: %v", err)
	}

	t.Logf("P3 escalation: checkpoint=%d > truncateLSN=10 → %v", state.CheckpointLSN, err)
}

// --- One-chain escalation: engine plan → truncation fails → NOT InSync ---

func TestP3_TruncateWAL_OneChain_FlushedAheadNotInSync(t *testing.T) {
	dir := t.TempDir()

	// Primary: 10 entries, flush (committed=10).
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('P')))
	}
	primaryVol.ForceFlush()

	// Replica: 10 base + 10 ahead, ALL FLUSHED (checkpoint > committed).
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	for i := 0; i < 10; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('P'))) // base
	}
	replicaVol.ForceFlush()
	for i := 0; i < 10; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('R'))) // ahead
	}
	replicaVol.ForceFlush() // contaminates extent

	replicaState := NewReader(replicaVol).ReadState()
	t.Logf("replica: head=%d checkpoint=%d (flushed ahead)", replicaState.WALHeadLSN, replicaState.CheckpointLSN)

	// Engine setup.
	primaryReader := NewReader(primaryVol)
	primaryPinner := NewPinner(primaryVol)
	sa := bridge.NewStorageAdapter(
		&readerShim{primaryReader},
		&pinnerShim{primaryPinner},
	)
	ca := bridge.NewControlAdapter()
	driver := engine.NewRecoveryDriver(sa)

	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent)

	plan, err := driver.PlanRecovery("vol1/vs2", replicaState.WALHeadLSN)
	if err != nil {
		t.Fatalf("PlanRecovery: %v", err)
	}
	if plan.TruncateLSN == 0 {
		t.Fatalf("expected truncation plan, got TruncateLSN=0")
	}
	t.Logf("plan: truncateLSN=%d", plan.TruncateLSN)

	// Execute: CatchUpExecutor with IO on the flushed-ahead replica.
	replicaExecutor := NewExecutor(replicaVol, "")
	exec := engine.NewCatchUpExecutor(driver, plan)
	exec.IO = replicaExecutor

	// Execute should FAIL — truncation detects flushed-ahead and escalates.
	execErr := exec.Execute(nil, 0)
	if execErr == nil {
		t.Fatal("execute should fail: flushed-ahead truncation must not succeed")
	}
	t.Logf("execute failed as expected: %v", execErr)

	// ESCALATION: sender must be in NeedsRebuild (not just "not InSync").
	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if s.State() != engine.StateNeedsRebuild {
		t.Fatalf("sender state=%s, want NeedsRebuild (must escalate, not just fail)", s.State())
	}
	t.Logf("sender state: %s — correctly escalated to rebuild", s.State())

	// Verify: the log shows explicit escalation event.
	events := driver.Orchestrator.Log.EventsFor("vol1/vs2")
	hasEscalation := false
	for _, ev := range events {
		if ev.Event == "truncation_escalated" {
			hasEscalation = true
		}
	}
	if !hasEscalation {
		t.Fatal("missing truncation_escalated event in log")
	}

	t.Logf("P3 escalation: flushed-ahead → truncation fails → sender NeedsRebuild → ready for rebuild assignment")
}

// --- Mixed case: checkpoint < truncateLSN → escalation (kept data in WAL) ---

func TestP3_TruncateWAL_MixedCase_CheckpointBelowTarget(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Write 10 entries, flush → checkpoint=10.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('B')))
	}
	vol.ForceFlush()

	// Write 10 more (entries 11-20), NOT flushed → in WAL only.
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('K'))) // K = kept range
	}

	state := NewReader(vol).ReadState()
	t.Logf("mixed case: head=%d checkpoint=%d", state.WALHeadLSN, state.CheckpointLSN)

	// Truncate to 15: checkpoint=10 < truncateLSN=15.
	// Entries 11-15 are "kept" but live only in WAL.
	// Truncation would discard them → data loss. Must escalate.
	executor := NewExecutor(vol, "")
	err := executor.TruncateWAL(15)
	if err == nil {
		t.Fatal("truncation should fail: checkpoint 10 < truncateLSN 15 (kept entries in WAL)")
	}
	if !containsSubstring(err.Error(), "truncation unsafe") {
		t.Fatalf("error should indicate truncation unsafe, got: %v", err)
	}

	t.Logf("P3 mixed case: checkpoint=%d < truncateLSN=15 → %v", state.CheckpointLSN, err)
}

// --- Repeated truncation across different boundaries ---

func TestP3_TruncateWAL_RepeatedEras(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	// Era 1: write 20 base, flush → checkpoint=20.
	for i := 0; i < 20; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	vol.ForceFlush()

	// Write 10 ahead (NOT flushed).
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('X')))
	}

	// Truncate to 20 (checkpoint=20 == truncateLSN → safe).
	executor := NewExecutor(vol, "")
	if err := executor.TruncateWAL(20); err != nil {
		t.Fatalf("first truncate: %v", err)
	}
	s1 := NewReader(vol).ReadState()
	if s1.WALHeadLSN != 20 {
		t.Fatalf("first truncate: head=%d, want 20", s1.WALHeadLSN)
	}

	// Era 2: write 5 more (LSN 21-25, NOT flushed).
	// After first truncation, checkpoint=20. Write 2 then flush to get checkpoint=22.
	for i := 0; i < 2; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('Y')))
	}
	vol.ForceFlush() // checkpoint=22

	// Write 3 more ahead (NOT flushed).
	for i := 2; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('Z')))
	}

	// Truncate to 22 (checkpoint=22 == truncateLSN → safe).
	if err := executor.TruncateWAL(22); err != nil {
		t.Fatalf("second truncate: %v", err)
	}
	s2 := NewReader(vol).ReadState()
	if s2.WALHeadLSN != 22 {
		t.Fatalf("second truncate: head=%d, want 22", s2.WALHeadLSN)
	}

	// Next write should be at LSN 23.
	vol.WriteLBA(0, makeBlock('W'))
	s3 := NewReader(vol).ReadState()
	if s3.WALHeadLSN != 23 {
		t.Fatalf("post-second-truncate write: head=%d, want 23", s3.WALHeadLSN)
	}

	t.Log("P3 repeated: era1(30→20) era2(25→22) — both exact, writes resume correctly")
}

// containsSubstring is shared with transfer_test.go.
func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
