package replication

import (
	"testing"
)

// ============================================================
// Phase 06 P2: Executor tests — stepwise execution with release symmetry
//
// Tests map to tester expectation template E1-E5.
// ============================================================

func setupCatchUpDriver(t *testing.T, replicaFlushedLSN uint64) (*RecoveryDriver, *RecoveryPlan) {
	t.Helper()
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	plan, err := driver.PlanRecovery("r1", replicaFlushedLSN)
	if err != nil {
		t.Fatal(err)
	}
	return driver, plan
}

func setupRebuildDriver(t *testing.T) (*RecoveryDriver, *RecoveryPlan, *mockStorage) {
	t.Helper()
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	})
	driver := NewRecoveryDriver(storage)

	// First: catch-up fails → NeedsRebuild.
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})
	driver.PlanRecovery("r1", 10) // NeedsRebuild (10 < tail 30)

	// Rebuild assignment.
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	plan, err := driver.PlanRebuild("r1")
	if err != nil {
		t.Fatal(err)
	}
	return driver, plan, storage
}

// --- E1: Partial catch-up releases resources on failure ---

func TestExecutor_E1_PartialCatchUp_ProgressFailure_ReleasesWAL(t *testing.T) {
	driver, plan := setupCatchUpDriver(t, 70)
	storage := driver.Storage.(*mockStorage)

	if len(storage.pinnedWAL) != 1 {
		t.Fatalf("WAL pin should exist before execution: %d", len(storage.pinnedWAL))
	}

	exec := NewCatchUpExecutor(driver, plan)

	// Progress with LSN that will exceed frozen target (target=100, try 101).
	err := exec.Execute([]uint64{80, 90, 101}, 0, 0)
	if err == nil {
		t.Fatal("should fail on progress beyond frozen target")
	}

	// WAL pin released on failure.
	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E1: WAL pin must be released after partial catch-up failure")
	}

	// Release event logged.
	hasRelease := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "exec_resources_released" {
			hasRelease = true
		}
	}
	if !hasRelease {
		t.Fatal("E1: resource release should be logged")
	}
}

func TestExecutor_E1_BudgetEscalation_ReleasesWAL(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 1000, TailLSN: 0, CommittedLSN: 1000,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	// Attach with budget.
	s := driver.Orchestrator.Registry.Sender("r1")
	s.InvalidateSession("setup", StateDisconnected)
	s.AttachSession(1, SessionCatchUp, WithBudget(CatchUpBudget{MaxDurationTicks: 3}))

	plan, _ := driver.PlanRecovery("r1", 500)
	exec := NewCatchUpExecutor(driver, plan)

	// Execute with ticks that exceed budget (3 steps, each +1 tick, budget=3).
	err := exec.Execute([]uint64{600, 700, 800, 900}, 0, 0)
	if err == nil {
		t.Fatal("should escalate on budget")
	}

	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E1: WAL pin must be released after budget escalation")
	}
}

// --- E2: Partial rebuild releases resources on failure ---

func TestExecutor_E2_PartialRebuild_TransferFailure_ReleasesAll(t *testing.T) {
	driver, plan, storage := setupRebuildDriver(t)

	if plan.RebuildSource != RebuildSnapshotTail {
		t.Fatalf("source=%s", plan.RebuildSource)
	}

	// Pins should exist.
	if len(storage.pinnedSnaps) == 0 || len(storage.pinnedWAL) == 0 {
		t.Fatal("snapshot + WAL pins should exist before execution")
	}

	exec := NewRebuildExecutor(driver, plan)

	// Invalidate session before execution → will fail at connect.
	driver.Orchestrator.Registry.Sender("r1").UpdateEpoch(2)

	err := exec.Execute(&storage.history)
	if err == nil {
		t.Fatal("should fail on invalidated session")
	}

	// All pins released.
	if len(storage.pinnedSnaps) != 0 {
		t.Fatal("E2: snapshot pin must be released after failed rebuild")
	}
	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E2: WAL pin must be released after failed rebuild")
	}
}

// --- E3: Cancellation mid-execution releases resources ---

func TestExecutor_E3_EpochBump_MidCatchUp_ReleasesWAL(t *testing.T) {
	driver, plan := setupCatchUpDriver(t, 70)
	storage := driver.Storage.(*mockStorage)

	// Invalidate session BEFORE execution to simulate epoch bump.
	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("r1", 2)

	exec := NewCatchUpExecutor(driver, plan)
	err := exec.Execute([]uint64{80, 90, 100}, 0, 0)
	if err == nil {
		t.Fatal("should fail on stale session")
	}

	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E3: WAL pin must be released after epoch bump")
	}
}

func TestExecutor_E3_Cancel_ReleasesResources(t *testing.T) {
	driver, plan := setupCatchUpDriver(t, 70)
	storage := driver.Storage.(*mockStorage)

	exec := NewCatchUpExecutor(driver, plan)
	exec.Cancel("test_cancellation")

	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E3: WAL pin must be released after cancellation")
	}

	hasRelease := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "exec_resources_released" {
			hasRelease = true
		}
	}
	if !hasRelease {
		t.Fatal("E3: cancellation release should be logged")
	}
}

func TestExecutor_E3_RebuildCancel_ReleasesAll(t *testing.T) {
	driver, plan, storage := setupRebuildDriver(t)

	exec := NewRebuildExecutor(driver, plan)
	exec.Cancel("epoch_bump")

	if len(storage.pinnedSnaps) != 0 || len(storage.pinnedWAL) != 0 {
		t.Fatal("E3: all rebuild pins must be released after cancellation")
	}
}

// --- E4: Successful execution releases resources ---

func TestExecutor_E4_SuccessfulCatchUp_ReleasesWAL(t *testing.T) {
	driver, plan := setupCatchUpDriver(t, 70)
	storage := driver.Storage.(*mockStorage)

	exec := NewCatchUpExecutor(driver, plan)
	err := exec.Execute([]uint64{80, 90, 100}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E4: WAL pin must be released after successful catch-up")
	}
	if driver.Orchestrator.Registry.Sender("r1").State() != StateInSync {
		t.Fatalf("state=%s", driver.Orchestrator.Registry.Sender("r1").State())
	}
}

func TestExecutor_E4_SuccessfulRebuild_ReleasesAll(t *testing.T) {
	driver, plan, storage := setupRebuildDriver(t)

	exec := NewRebuildExecutor(driver, plan)
	err := exec.Execute(&storage.history)
	if err != nil {
		t.Fatal(err)
	}

	if len(storage.pinnedSnaps) != 0 || len(storage.pinnedWAL) != 0 {
		t.Fatal("E4: all pins must be released after successful rebuild")
	}
	if driver.Orchestrator.Registry.Sender("r1").State() != StateInSync {
		t.Fatalf("state=%s", driver.Orchestrator.Registry.Sender("r1").State())
	}
}

// --- E5: Executor drives sender APIs stepwise ---

func TestExecutor_E5_CatchUp_StepwiseNotConvenience(t *testing.T) {
	driver, plan := setupCatchUpDriver(t, 70)

	exec := NewCatchUpExecutor(driver, plan)
	err := exec.Execute([]uint64{80, 90, 100}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify stepwise execution happened via log events.
	events := driver.Orchestrator.Log.EventsFor("r1")
	hasStarted := false
	hasCompleted := false
	for _, e := range events {
		if e.Event == "exec_catchup_started" {
			hasStarted = true
		}
		if e.Event == "exec_completed" {
			hasCompleted = true
		}
	}
	if !hasStarted || !hasCompleted {
		t.Fatal("E5: executor should log stepwise start and completion")
	}
}

func TestExecutor_E5_Rebuild_StepwiseNotConvenience(t *testing.T) {
	driver, plan, storage := setupRebuildDriver(t)

	exec := NewRebuildExecutor(driver, plan)
	err := exec.Execute(&storage.history)
	if err != nil {
		t.Fatal(err)
	}

	events := driver.Orchestrator.Log.EventsFor("r1")
	hasStarted := false
	hasCompleted := false
	for _, e := range events {
		if e.Event == "exec_rebuild_started" {
			hasStarted = true
		}
		if e.Event == "exec_rebuild_completed" {
			hasCompleted = true
		}
	}
	if !hasStarted || !hasCompleted {
		t.Fatal("E5: rebuild executor should log stepwise start and completion")
	}
}
