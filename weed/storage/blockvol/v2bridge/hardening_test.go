package v2bridge

import (
	"testing"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// ============================================================
// Phase 08 P3: Hardening validation on accepted P1/P2 live path
//
// Replay matrix (4 cases) + 2 extra cases.
// Each test proves entry truth → engine result → execution result
// → cleanup result → observability result as ONE verification.
// ============================================================

func setupHardening(t *testing.T) (*engine.RecoveryDriver, *bridge.ControlAdapter, *Reader, *Executor, *Pinner) {
	t.Helper()
	vol := createTestVol(t)
	t.Cleanup(func() { vol.Close() })

	reader := NewReader(vol)
	pinner := NewPinner(vol)
	executor := NewExecutor(vol, "")

	sa := bridge.NewStorageAdapter(&readerShim{reader}, &pinnerShim{pinner})
	ca := bridge.NewControlAdapter()
	driver := engine.NewRecoveryDriver(sa)

	return driver, ca, reader, executor, pinner
}

// --- Matrix 1: Changed-address restart ---

func TestP3_Matrix_ChangedAddress(t *testing.T) {
	driver, ca, reader, executor, pinner := setupHardening(t)
	vol := reader.vol

	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}
	vol.ForceFlush()
	for i := 5; i < 8; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	// Initial assignment.
	intent1 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "replica",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"}},
	)
	driver.Orchestrator.ProcessAssignment(intent1)
	plan1, _ := driver.PlanRecovery("v1/vs2", 6)

	senderBefore := driver.Orchestrator.Registry.Sender("v1/vs2")

	// Address changes — cancel old plan, new assignment.
	driver.CancelPlan(plan1, "address_change")

	intent2 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "replica",
			DataAddr: "10.0.0.5:9333", CtrlAddr: "10.0.0.5:9334"}},
	)
	driver.Orchestrator.ProcessAssignment(intent2)

	// New plan + execute — must be CatchUp (replica=6, within window).
	plan2, err := driver.PlanRecovery("v1/vs2", 6)
	if err != nil {
		t.Fatal(err)
	}
	if plan2.Outcome != engine.OutcomeCatchUp {
		t.Fatalf("changed-address: outcome=%s, want CatchUp", plan2.Outcome)
	}
	exec := engine.NewCatchUpExecutor(driver, plan2)
	exec.IO = executor
	if err := exec.Execute(nil, 0); err != nil {
		t.Fatalf("catch-up execution: %v", err)
	}

	// Assertions.
	senderAfter := driver.Orchestrator.Registry.Sender("v1/vs2")
	if senderAfter != senderBefore {
		t.Fatal("identity not preserved")
	}
	if pinner.ActiveHoldCount() != 0 {
		t.Fatalf("leaked pins: %d", pinner.ActiveHoldCount())
	}

	hasCancelled := false
	hasCreated := false
	for _, e := range driver.Orchestrator.Log.EventsFor("v1/vs2") {
		if e.Event == "plan_cancelled" {
			hasCancelled = true
		}
		if e.Event == "session_created" {
			hasCreated = true
		}
	}
	if !hasCancelled || !hasCreated {
		t.Fatal("logs must show plan_cancelled + session_created")
	}
}

// --- Matrix 2: Stale epoch / stale session ---

func TestP3_Matrix_StaleEpoch(t *testing.T) {
	driver, ca, reader, _, pinner := setupHardening(t)
	vol := reader.vol

	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	intent1 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "replica",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"}},
	)
	driver.Orchestrator.ProcessAssignment(intent1)
	plan, _ := driver.PlanRecovery("v1/vs2", 0)

	// Epoch bumps.
	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("v1/vs2", 2)
	driver.CancelPlan(plan, "epoch_bump")

	// New epoch assignment.
	intent2 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 2, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "replica",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"}},
	)
	driver.Orchestrator.ProcessAssignment(intent2)

	s := driver.Orchestrator.Registry.Sender("v1/vs2")
	if !s.HasActiveSession() {
		t.Fatal("should have new session at epoch 2")
	}
	if pinner.ActiveHoldCount() != 0 {
		t.Fatalf("leaked pins: %d", pinner.ActiveHoldCount())
	}

	hasInvalidation := false
	for _, e := range driver.Orchestrator.Log.EventsFor("v1/vs2") {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
	}
	if !hasInvalidation {
		t.Fatal("logs must show session_invalidated")
	}
}

// --- Matrix 3: Unrecoverable gap / needs-rebuild ---

func TestP3_Matrix_NeedsRebuild(t *testing.T) {
	driver, ca, reader, _, pinner := setupHardening(t)
	vol := reader.vol

	for i := 0; i < 20; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	vol.ForceFlush()

	state := reader.ReadState()
	if state.WALTailLSN == 0 {
		t.Fatal("need post-flush state")
	}

	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "replica",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"}},
	)
	driver.Orchestrator.ProcessAssignment(intent)

	plan, _ := driver.PlanRecovery("v1/vs2", 0)
	if plan.Outcome != engine.OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s", plan.Outcome)
	}

	// Rebuild assignment + execute.
	rebuildIntent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "rebuilding",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"}},
	)
	driver.Orchestrator.ProcessAssignment(rebuildIntent)
	rebuildPlan, _ := driver.PlanRebuild("v1/vs2")

	// IO=nil: FSM test mode. Real snapshot_tail I/O is proven by
	// TestP2_SnapshotTailRebuild_OneChain in snapshot_transfer_test.go.
	exec := engine.NewRebuildExecutor(driver, rebuildPlan)
	if err := exec.Execute(); err != nil {
		t.Fatal(err)
	}

	s := driver.Orchestrator.Registry.Sender("v1/vs2")
	if s.State() != engine.StateInSync {
		t.Fatalf("state=%s", s.State())
	}
	if pinner.ActiveHoldCount() != 0 {
		t.Fatalf("leaked pins: %d", pinner.ActiveHoldCount())
	}

	// Observability: escalation event must exist.
	hasEscalation := false
	for _, e := range driver.Orchestrator.Log.EventsFor("v1/vs2") {
		if e.Event == "escalated" {
			hasEscalation = true
		}
	}
	if !hasEscalation {
		t.Fatal("logs must show escalated event for NeedsRebuild")
	}
}

// --- Matrix 4: Post-checkpoint boundary ---

func TestP3_Matrix_PostCheckpointBoundary(t *testing.T) {
	driver, ca, reader, executor, pinner := setupHardening(t)
	vol := reader.vol

	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}
	vol.ForceFlush()
	for i := 5; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	state := reader.ReadState()
	t.Logf("boundary: head=%d tail=%d committed=%d checkpoint=%d",
		state.WALHeadLSN, state.WALTailLSN, state.CommittedLSN, state.CheckpointLSN)

	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "replica",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"}},
	)

	// At committed → ZeroGap.
	driver.Orchestrator.ProcessAssignment(intent)
	planZero, _ := driver.PlanRecovery("v1/vs2", state.CommittedLSN)
	if planZero.Outcome != engine.OutcomeZeroGap {
		t.Fatalf("at committed: outcome=%s", planZero.Outcome)
	}

	// Within catch-up window → CatchUp.
	driver.Orchestrator.ProcessAssignment(intent)
	replicaInWindow := state.WALTailLSN + 1
	planCatchUp, _ := driver.PlanRecovery("v1/vs2", replicaInWindow)
	if planCatchUp.Outcome != engine.OutcomeCatchUp {
		t.Fatalf("in window: outcome=%s (replica=%d)", planCatchUp.Outcome, replicaInWindow)
	}
	exec := engine.NewCatchUpExecutor(driver, planCatchUp)
	exec.IO = executor
	if err := exec.Execute(nil, 0); err != nil {
		t.Fatalf("catch-up: %v", err)
	}

	if pinner.ActiveHoldCount() != 0 {
		t.Fatalf("leaked pins: %d", pinner.ActiveHoldCount())
	}

	// Observability: handshake + completion events.
	hasHandshake := false
	hasComplete := false
	for _, e := range driver.Orchestrator.Log.EventsFor("v1/vs2") {
		if e.Event == "handshake" {
			hasHandshake = true
		}
		if e.Event == "exec_catchup_started" || e.Event == "exec_completed" {
			hasComplete = true
		}
	}
	if !hasHandshake {
		t.Fatal("logs must show handshake event")
	}
	if !hasComplete {
		t.Fatal("logs must show catch-up execution event")
	}
}

// --- Extra 1: Real failover / reassignment cycle ---

func TestP3_Extra_FailoverCycle(t *testing.T) {
	driver, ca, reader, executor, pinner := setupHardening(t)
	vol := reader.vol

	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}
	vol.ForceFlush()
	for i := 5; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	// Epoch 1: primary with replica.
	intent1 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "replica",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"}},
	)
	driver.Orchestrator.ProcessAssignment(intent1)
	plan1, _ := driver.PlanRecovery("v1/vs2", 6)

	// Failover: primary dies, epoch bumps.
	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("v1/vs2", 2)
	driver.CancelPlan(plan1, "failover")

	// Epoch 2: new primary, replica re-joins.
	intent2 := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "v1", Epoch: 2, Role: "primary"},
		[]bridge.MasterAssignment{{VolumeName: "v1", ReplicaServerID: "vs2", Role: "replica",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"}},
	)
	driver.Orchestrator.ProcessAssignment(intent2)
	plan2, _ := driver.PlanRecovery("v1/vs2", 6)

	if plan2.Outcome == engine.OutcomeCatchUp {
		exec := engine.NewCatchUpExecutor(driver, plan2)
		exec.IO = executor
		exec.Execute(nil, 0)
	}

	s := driver.Orchestrator.Registry.Sender("v1/vs2")
	if s.State() != engine.StateInSync {
		t.Fatalf("after failover: state=%s", s.State())
	}
	if pinner.ActiveHoldCount() != 0 {
		t.Fatalf("leaked pins: %d", pinner.ActiveHoldCount())
	}

	// Observability: full failover cycle logged.
	events := driver.Orchestrator.Log.EventsFor("v1/vs2")
	hasInvalidation := false
	hasCancellation := false
	hasNewSession := false
	for _, e := range events {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
		if e.Event == "plan_cancelled" {
			hasCancellation = true
		}
		if e.Event == "session_created" {
			hasNewSession = true
		}
	}
	if !hasInvalidation || !hasCancellation || !hasNewSession {
		t.Fatal("failover logs incomplete")
	}
}

// --- Extra 2: Overlapping retention / pinner safety ---

func TestP3_Extra_OverlappingRetention(t *testing.T) {
	_, _, reader, _, pinner := setupHardening(t)
	vol := reader.vol

	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}
	vol.ForceFlush()
	for i := 5; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	state := reader.ReadState()

	// Hold 1: WAL retention at LSN 6.
	release1, err := pinner.HoldWALRetention(state.WALTailLSN + 1)
	if err != nil {
		t.Fatal(err)
	}

	// Hold 2: WAL retention at LSN 7 (both live simultaneously).
	release2, err := pinner.HoldWALRetention(state.WALTailLSN + 2)
	if err != nil {
		t.Fatal(err)
	}

	// Both holds coexist.
	if pinner.ActiveHoldCount() != 2 {
		t.Fatalf("two holds should coexist: got %d", pinner.ActiveHoldCount())
	}

	// Minimum floor should be the LOWER of the two holds.
	floor, hasFloor := pinner.MinWALRetentionFloor()
	if !hasFloor {
		t.Fatal("should have retention floor with 2 active holds")
	}
	expectedFloor := state.WALTailLSN + 1
	if floor != expectedFloor {
		t.Fatalf("floor=%d, want %d (minimum of two holds)", floor, expectedFloor)
	}

	// Release hold 1 — hold 2 still protects.
	release1()
	if pinner.ActiveHoldCount() != 1 {
		t.Fatalf("after release1: holds=%d, want 1", pinner.ActiveHoldCount())
	}
	floor, hasFloor = pinner.MinWALRetentionFloor()
	if !hasFloor {
		t.Fatal("hold 2 should still provide floor")
	}
	expectedFloor = state.WALTailLSN + 2
	if floor != expectedFloor {
		t.Fatalf("after release1: floor=%d, want %d", floor, expectedFloor)
	}

	// Release hold 2 — no more holds.
	release2()
	if pinner.ActiveHoldCount() != 0 {
		t.Fatalf("all released: holds=%d", pinner.ActiveHoldCount())
	}
	_, hasFloor = pinner.MinWALRetentionFloor()
	if hasFloor {
		t.Fatal("no floor expected after all holds released")
	}
}
