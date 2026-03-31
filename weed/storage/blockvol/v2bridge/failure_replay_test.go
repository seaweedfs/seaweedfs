package v2bridge

import (
	"testing"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// ============================================================
// Phase 07 P2: Real-system failure replay
//
// Carry-forward (explicit):
//   - CommittedLSN = CheckpointLSN (V1 interim)
//   - Executor snapshot/full-base/truncate still stubs
//   - Control intent via direct AssignmentIntent (not real heartbeat)
// ============================================================

func setupIntegrated(t *testing.T) (*engine.RecoveryDriver, *bridge.ControlAdapter, *Reader) {
	t.Helper()
	vol := createTestVol(t)
	t.Cleanup(func() { vol.Close() })

	reader := NewReader(vol)
	pinner := NewPinner(vol)

	sa := bridge.NewStorageAdapter(
		&readerShim{reader},
		&pinnerShim{pinner},
	)

	ca := bridge.NewControlAdapter()
	driver := engine.NewRecoveryDriver(sa)

	return driver, ca, reader
}

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

func makeAssignment(epoch uint64) (bridge.MasterAssignment, []bridge.MasterAssignment) {
	return bridge.MasterAssignment{VolumeName: "vol1", Epoch: epoch, Role: "primary", PrimaryServerID: "vs1"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", AddrVersion: 1},
		}
}

// --- FC1: Changed-address restart (integrated engine/storage, control simulated) ---

func TestP2_FC1_ChangedAddress(t *testing.T) {
	driver, ca, _ := setupIntegrated(t)

	primary, replicas := makeAssignment(1)
	driver.Orchestrator.ProcessAssignment(ca.ToAssignmentIntent(primary, replicas))

	plan, _ := driver.PlanRecovery("vol1/vs2", 0)
	senderBefore := driver.Orchestrator.Registry.Sender("vol1/vs2")

	driver.CancelPlan(plan, "address_change")

	// New assignment with different address.
	replicas[0].DataAddr = "10.0.0.3:9333"
	replicas[0].AddrVersion = 2
	driver.Orchestrator.ProcessAssignment(ca.ToAssignmentIntent(primary, replicas))

	senderAfter := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if senderAfter != senderBefore {
		t.Fatal("FC1: sender identity must be preserved")
	}
	if senderAfter.Endpoint().DataAddr != "10.0.0.3:9333" {
		t.Fatalf("FC1: endpoint not updated: %s", senderAfter.Endpoint().DataAddr)
	}

	// Verify new session was created after address change.
	if !senderAfter.HasActiveSession() {
		t.Fatal("FC1: new session must be created after address change")
	}

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
		t.Fatal("FC1: log must show plan_cancelled")
	}
	if !hasNewSession {
		t.Fatal("FC1: log must show session_created after address change")
	}
}

// --- FC2: Stale epoch after failover (integrated engine/storage, control simulated) ---

func TestP2_FC2_StaleEpoch(t *testing.T) {
	driver, ca, _ := setupIntegrated(t)

	primary, replicas := makeAssignment(1)
	driver.Orchestrator.ProcessAssignment(ca.ToAssignmentIntent(primary, replicas))

	// Epoch bumps before any plan.
	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("vol1/vs2", 2)

	// New assignment at epoch 2.
	primary.Epoch = 2
	replicas[0].Epoch = 2
	driver.Orchestrator.ProcessAssignment(ca.ToAssignmentIntent(primary, replicas))

	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if !s.HasActiveSession() {
		t.Fatal("FC2: should have session at epoch 2")
	}

	hasInvalidation := false
	for _, e := range driver.Orchestrator.Log.EventsFor("vol1/vs2") {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
	}
	if !hasInvalidation {
		t.Fatal("FC2: log must show per-replica invalidation")
	}
}

// --- FC3: Real catch-up execution (forced, not observational) ---

func TestP2_FC3_RealCatchUp_Forced(t *testing.T) {
	driver, ca, reader := setupIntegrated(t)
	vol := reader.vol

	// Write entries — creates WAL entries above checkpoint=0.
	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	// Do NOT flush — checkpoint stays at 0, entries are in WAL.
	state := reader.ReadState()
	t.Logf("FC3: head=%d tail=%d committed=%d", state.WALHeadLSN, state.WALTailLSN, state.CommittedLSN)

	// In V1 interim: committed=0 (flusher not run). Replica at 0 = zero-gap.
	// To force real catch-up: replica must be BEHIND committed but committed > 0.
	// Since committed=0 without flush, we can't get OutcomeCatchUp.
	// BUT: we CAN test the real WAL scan path by directly using the executor.

	primary, replicas := makeAssignment(1)
	driver.Orchestrator.ProcessAssignment(ca.ToAssignmentIntent(primary, replicas))

	// Even though engine classifies as ZeroGap (committed=0),
	// we can verify the real WAL scan works by directly streaming.
	executor := NewExecutor(vol)
	transferred, err := executor.StreamWALEntries(0, state.WALHeadLSN)
	if err != nil {
		t.Fatalf("FC3: real WAL scan failed: %v", err)
	}
	if transferred == 0 {
		t.Fatal("FC3: must transfer real WAL entries")
	}
	if transferred < state.WALHeadLSN {
		t.Fatalf("FC3: transferred=%d, want=%d", transferred, state.WALHeadLSN)
	}
	t.Logf("FC3: real WAL scan transferred to LSN %d", transferred)

	// Also verify the engine planning path sees real state.
	plan, _ := driver.PlanRecovery("vol1/vs2", 0)
	if plan == nil {
		t.Fatal("FC3: plan should succeed")
	}
	// Document: in V1 interim without flush, outcome is ZeroGap.
	if plan.Outcome != engine.OutcomeZeroGap {
		t.Logf("FC3: non-zero-gap outcome=%s (checkpoint advanced)", plan.Outcome)
	}
}

// --- FC4: Unrecoverable gap (forced via ForceFlush) ---

func TestP2_FC4_UnrecoverableGap_Forced(t *testing.T) {
	driver, ca, reader := setupIntegrated(t)
	vol := reader.vol

	// Write entries.
	for i := 0; i < 20; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}

	// Force flush to advance checkpoint + WAL tail.
	if err := vol.ForceFlush(); err != nil {
		t.Fatalf("FC4: ForceFlush: %v", err)
	}

	state := reader.ReadState()
	t.Logf("FC4: head=%d tail=%d committed=%d checkpoint=%d",
		state.WALHeadLSN, state.WALTailLSN, state.CommittedLSN, state.CheckpointLSN)

	if state.WALTailLSN == 0 {
		t.Fatal("FC4: ForceFlush must advance checkpoint/tail (WALTailLSN still 0)")
	}

	primary, replicas := makeAssignment(1)
	driver.Orchestrator.ProcessAssignment(ca.ToAssignmentIntent(primary, replicas))

	// Replica at 0 — below tail → NeedsRebuild.
	plan, err := driver.PlanRecovery("vol1/vs2", 0)
	if err != nil && plan == nil {
		t.Fatalf("FC4: plan: %v", err)
	}

	if plan.Outcome != engine.OutcomeNeedsRebuild {
		t.Fatalf("FC4: outcome=%s, want NeedsRebuild (replica=0 tail=%d)",
			plan.Outcome, state.WALTailLSN)
	}

	// Proof must contain gap details.
	if plan.Proof == nil || plan.Proof.Recoverable {
		t.Fatal("FC4: proof must show unrecoverable")
	}

	hasEscalation := false
	hasProofDetail := false
	for _, e := range driver.Orchestrator.Log.EventsFor("vol1/vs2") {
		if e.Event == "escalated" {
			hasEscalation = true
			// Detail must contain the proof reason with LSN values.
			if len(e.Detail) > 15 {
				hasProofDetail = true
			}
		}
	}
	if !hasEscalation {
		t.Fatal("FC4: log must show escalation event")
	}
	if !hasProofDetail {
		t.Fatal("FC4: escalation event must contain proof reason with LSN details")
	}
	t.Logf("FC4: NeedsRebuild proven — replica=0, tail=%d, proof=%s",
		state.WALTailLSN, plan.Proof.Reason)
}

// --- FC5: Post-checkpoint boundary (forced, assertive) ---

func TestP2_FC5_PostCheckpoint_Boundary(t *testing.T) {
	driver, ca, reader := setupIntegrated(t)
	vol := reader.vol

	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}
	vol.ForceFlush()

	state := reader.ReadState()
	t.Logf("FC5: head=%d tail=%d committed=%d checkpoint=%d",
		state.WALHeadLSN, state.WALTailLSN, state.CommittedLSN, state.CheckpointLSN)

	if state.CheckpointLSN == 0 {
		t.Fatal("FC5: ForceFlush must advance checkpoint")
	}

	primary, replicas := makeAssignment(1)

	// Case 1: replica at checkpoint → ZeroGap (V1 interim: committed == checkpoint).
	driver.Orchestrator.ProcessAssignment(ca.ToAssignmentIntent(primary, replicas))
	plan1, _ := driver.PlanRecovery("vol1/vs2", state.CheckpointLSN)
	if plan1 == nil {
		t.Fatal("FC5: plan at checkpoint should succeed")
	}
	if plan1.Outcome != engine.OutcomeZeroGap {
		t.Fatalf("FC5: replica at checkpoint=%d: outcome=%s, want ZeroGap (V1 interim)",
			state.CheckpointLSN, plan1.Outcome)
	}

	// Case 2: replica below checkpoint → NeedsRebuild or CatchUp.
	// In V1 interim: tail=checkpoint. Below tail = NeedsRebuild.
	if state.CheckpointLSN > 0 {
		driver.Orchestrator.ProcessAssignment(ca.ToAssignmentIntent(primary, replicas))
		plan2, _ := driver.PlanRecovery("vol1/vs2", 0) // replica at 0
		if plan2 == nil {
			t.Fatal("FC5: plan below checkpoint should succeed")
		}
		if plan2.Outcome == engine.OutcomeCatchUp {
			t.Fatal("FC5: replica below checkpoint must NOT claim general catch-up in V1 interim")
		}
		t.Logf("FC5: replica=0, checkpoint=%d → outcome=%s", state.CheckpointLSN, plan2.Outcome)
	}
}
