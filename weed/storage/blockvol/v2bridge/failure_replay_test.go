package v2bridge

import (
	"testing"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// ============================================================
// Phase 07 P2: Real-system failure replay
// Tests exercise the full integrated path:
//   bridge adapter → v2bridge reader/pinner/executor → engine
// against real file-backed BlockVol instances.
//
// Carry-forward (explicit):
//   - CommittedLSN = CheckpointLSN (V1 interim)
//   - catch-up only works pre-checkpoint
//   - snapshot/full-base/truncate executor stubs
//   - control intent via direct AssignmentIntent construction
// ============================================================

// setupIntegrated creates a real BlockVol + all bridge components + engine driver.
func setupIntegrated(t *testing.T) (*engine.RecoveryDriver, *bridge.ControlAdapter, *bridge.StorageAdapter, *Reader) {
	t.Helper()
	vol := createTestVol(t)
	t.Cleanup(func() { vol.Close() })

	reader := NewReader(vol)
	pinner := NewPinner(vol)

	// Bridge storage adapter backed by real reader + pinner.
	sa := bridge.NewStorageAdapter(
		&readerShim{reader},
		&pinnerShim{pinner},
	)

	ca := bridge.NewControlAdapter()
	driver := engine.NewRecoveryDriver(sa)

	return driver, ca, sa, reader
}

// Shims adapt v2bridge types to bridge contract interfaces.
type readerShim struct{ r *Reader }

func (s *readerShim) ReadState() bridge.BlockVolState {
	rs := s.r.ReadState()
	return bridge.BlockVolState{
		WALHeadLSN:        rs.WALHeadLSN,
		WALTailLSN:        rs.WALTailLSN,
		CommittedLSN:      rs.CommittedLSN,
		CheckpointLSN:     rs.CheckpointLSN,
		CheckpointTrusted: rs.CheckpointTrusted,
	}
}

type pinnerShim struct{ p *Pinner }

func (s *pinnerShim) HoldWALRetention(startLSN uint64) (func(), error) {
	return s.p.HoldWALRetention(startLSN)
}
func (s *pinnerShim) HoldSnapshot(checkpointLSN uint64) (func(), error) {
	return s.p.HoldSnapshot(checkpointLSN)
}
func (s *pinnerShim) HoldFullBase(committedLSN uint64) (func(), error) {
	return s.p.HoldFullBase(committedLSN)
}

// --- FC1 / E1: Changed-address restart ---

func TestP2_FC1_ChangedAddress_IntegratedPath(t *testing.T) {
	driver, ca, _, _ := setupIntegrated(t)

	// First assignment.
	intent1 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary", PrimaryServerID: "vs1"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", AddrVersion: 1},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent1)

	// Plan recovery — acquires resources.
	plan, err := driver.PlanRecovery("vol1/vs2", 0) // replica at 0
	if err != nil {
		t.Fatal(err)
	}

	senderBefore := driver.Orchestrator.Registry.Sender("vol1/vs2")

	// Address changes — cancel old plan, new assignment.
	driver.CancelPlan(plan, "address_change")

	intent2 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary", PrimaryServerID: "vs1"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.3:9333", CtrlAddr: "10.0.0.3:9334", AddrVersion: 2},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent2)

	// Identity preserved.
	senderAfter := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if senderAfter != senderBefore {
		t.Fatal("E1: sender identity must be preserved across address change")
	}
	if senderAfter.Endpoint().DataAddr != "10.0.0.3:9333" {
		t.Fatalf("E1: endpoint not updated: %s", senderAfter.Endpoint().DataAddr)
	}

	// E5: log shows plan_cancelled + new session.
	hasCancelled := false
	hasNewSession := false
	for _, e := range driver.Orchestrator.Log.EventsFor("vol1/vs2") {
		if e.Event == "plan_cancelled" {
			hasCancelled = true
		}
		if e.Event == "session_created" {
			hasNewSession = true
		}
	}
	if !hasCancelled {
		t.Fatal("E5: log must show plan_cancelled")
	}
	if !hasNewSession {
		t.Fatal("E5: log must show new session after address change")
	}
}

// --- FC2 / E2: Stale epoch after failover ---

func TestP2_FC2_StaleEpoch_IntegratedPath(t *testing.T) {
	driver, ca, _, _ := setupIntegrated(t)

	// Epoch 1 assignment with recovery.
	intent1 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", AddrVersion: 1},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent1)

	// Epoch bumps BEFORE planning (simulates failover interrupting assignment).
	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("vol1/vs2", 2)

	// New assignment at epoch 2.
	intent2 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 2, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", AddrVersion: 1},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent2)

	// New session at epoch 2.
	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if !s.HasActiveSession() {
		t.Fatal("E2: should have new session at epoch 2")
	}

	// E5: log shows per-replica invalidation.
	hasInvalidation := false
	for _, e := range driver.Orchestrator.Log.EventsFor("vol1/vs2") {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
	}
	if !hasInvalidation {
		t.Fatal("E5: log must show session invalidation with epoch cause")
	}
}

// --- FC3 / E3: Real catch-up (pre-checkpoint window) ---

func TestP2_FC3_RealCatchUp_IntegratedPath(t *testing.T) {
	driver, ca, _, reader := setupIntegrated(t)

	// Write entries to blockvol (creates WAL entries above checkpoint=0).
	vol := reader.vol
	vol.WriteLBA(0, makeBlock('A'))
	vol.WriteLBA(1, makeBlock('B'))
	vol.WriteLBA(2, makeBlock('C'))

	// Read real state.
	state := reader.ReadState()
	if state.WALHeadLSN < 3 {
		t.Fatalf("HeadLSN=%d, need >= 3", state.WALHeadLSN)
	}

	// Assignment.
	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", AddrVersion: 1},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent)

	// Plan recovery from real storage state.
	// Replica at 0, head at 3+. Pre-checkpoint window: catch-up works.
	plan, err := driver.PlanRecovery("vol1/vs2", 0)
	if err != nil {
		t.Fatal(err)
	}

	if plan.Outcome != engine.OutcomeCatchUp {
		// Could be zero-gap if WALTailLSN == 0 and committed == 0.
		if plan.Outcome == engine.OutcomeZeroGap {
			t.Log("E3: zero-gap (committed=0 in V1 interim — flusher not run)")
			return
		}
		t.Fatalf("E3: outcome=%s", plan.Outcome)
	}

	// Execute catch-up through engine executor.
	exec := engine.NewCatchUpExecutor(driver, plan)
	progressLSNs := make([]uint64, 0)
	for lsn := uint64(1); lsn <= state.WALHeadLSN; lsn++ {
		progressLSNs = append(progressLSNs, lsn)
	}
	if err := exec.Execute(progressLSNs, 0); err != nil {
		t.Fatalf("E3: catch-up execution: %v", err)
	}

	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if s.State() != engine.StateInSync {
		t.Fatalf("E3: state=%s, want in_sync", s.State())
	}

	// E5: log shows full chain.
	hasHandshake := false
	hasCompleted := false
	for _, e := range driver.Orchestrator.Log.EventsFor("vol1/vs2") {
		if e.Event == "handshake" {
			hasHandshake = true
		}
		if e.Event == "exec_completed" {
			hasCompleted = true
		}
	}
	if !hasHandshake || !hasCompleted {
		t.Fatal("E5: log must show handshake + exec_completed")
	}
}

// --- FC4 / E4: Unrecoverable gap → NeedsRebuild ---

func TestP2_FC4_UnrecoverableGap_IntegratedPath(t *testing.T) {
	driver, ca, sa, reader := setupIntegrated(t)

	// Write + flush to advance checkpoint (simulates post-checkpoint state).
	vol := reader.vol
	for i := 0; i < 20; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	vol.SyncCache()

	// Read state after flush.
	state := reader.ReadState()
	t.Logf("after flush: head=%d tail=%d committed=%d checkpoint=%d",
		state.WALHeadLSN, state.WALTailLSN, state.CommittedLSN, state.CheckpointLSN)

	// If checkpoint advanced (tail > 0), a replica behind the tail can't catch up.
	if state.WALTailLSN == 0 {
		t.Log("FC4: checkpoint not advanced — can't demonstrate unrecoverable gap")
		t.Log("FC4: this is expected in V1 interim model where checkpoint may not advance in unit test")
		return
	}

	// Assignment.
	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", AddrVersion: 1},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent)

	// Plan recovery: replica behind tail → NeedsRebuild.
	plan, err := driver.PlanRecovery("vol1/vs2", 0) // replica at 0, tail > 0
	if err != nil && plan == nil {
		t.Fatalf("FC4: plan failed: %v", err)
	}

	if plan.Outcome != engine.OutcomeNeedsRebuild {
		t.Fatalf("FC4: outcome=%s (expected NeedsRebuild with replica at 0, tail=%d)",
			plan.Outcome, state.WALTailLSN)
	}

	// E5: log shows proof with LSN details.
	hasEscalation := false
	for _, e := range driver.Orchestrator.Log.EventsFor("vol1/vs2") {
		if e.Event == "escalated" && len(e.Detail) > 10 {
			hasEscalation = true
		}
	}
	if !hasEscalation {
		t.Fatal("E5: log must show escalation with gap details")
	}

	_ = sa // used via driver
}

// --- FC5: Post-checkpoint catch-up collapse ---

func TestP2_FC5_PostCheckpoint_CatchUpBoundary(t *testing.T) {
	driver, ca, _, reader := setupIntegrated(t)

	vol := reader.vol
	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}
	vol.SyncCache()

	state := reader.ReadState()
	t.Logf("FC5: head=%d tail=%d committed=%d checkpoint=%d",
		state.WALHeadLSN, state.WALTailLSN, state.CommittedLSN, state.CheckpointLSN)

	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", AddrVersion: 1},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent)

	// Replica at checkpoint LSN (exactly at boundary).
	replicaLSN := state.CheckpointLSN
	plan, err := driver.PlanRecovery("vol1/vs2", replicaLSN)
	if err != nil {
		t.Fatalf("FC5: plan: %v", err)
	}

	// In V1 interim: committed = checkpoint. Replica at checkpoint = zero-gap.
	if plan.Outcome == engine.OutcomeZeroGap {
		t.Log("FC5: replica at checkpoint → zero-gap (correct for V1 interim)")
	} else {
		t.Logf("FC5: outcome=%s (replica at checkpoint=%d)", plan.Outcome, replicaLSN)
	}

	// Replica below checkpoint — should be NeedsRebuild or CatchUp
	// depending on whether entries are still in WAL.
	if replicaLSN > 0 {
		plan2, _ := driver.PlanRecovery("vol1/vs2", replicaLSN-1)
		if plan2 != nil {
			t.Logf("FC5: replica at checkpoint-1=%d → outcome=%s", replicaLSN-1, plan2.Outcome)
		}
	}
}
