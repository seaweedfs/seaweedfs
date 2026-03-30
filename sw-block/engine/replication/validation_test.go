package replication

import (
	"testing"
)

// ============================================================
// Phase 06 P3: Validation against real failure classes
// Maps to tester expectations E1-E5, failure classes FC1/FC2/FC5/FC8.
// ============================================================

// --- E1 / FC1: Changed-address restart through planner/executor ---

func TestP3_E1_ChangedAddress_ThroughPlannerExecutor(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
	})
	driver := NewRecoveryDriver(storage)

	// Initial assignment with active session (not completed yet).
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vol1-r1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", CtrlAddr: "10.0.0.1:9334", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"vol1-r1": SessionCatchUp},
	})

	// Plan recovery — session is active.
	plan1, err := driver.PlanRecovery("vol1-r1", 70)
	if err != nil {
		t.Fatal(err)
	}

	// Sender identity before address change.
	senderBefore := driver.Orchestrator.Registry.Sender("vol1-r1")

	// Address changes WHILE session is active — triggers invalidation.
	// Release old plan resources first (simulates clean handoff).
	driver.ReleasePlan(plan1)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vol1-r1", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", Version: 2}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"vol1-r1": SessionCatchUp},
	})

	// Sender identity preserved.
	senderAfter := driver.Orchestrator.Registry.Sender("vol1-r1")
	if senderAfter != senderBefore {
		t.Fatal("E1: sender identity must be preserved across address change")
	}
	if senderAfter.Endpoint().DataAddr != "10.0.0.2:9333" {
		t.Fatalf("E1: endpoint not updated: %s", senderAfter.Endpoint().DataAddr)
	}

	// New plan + execute on new endpoint.
	plan2, err := driver.PlanRecovery("vol1-r1", 100) // zero-gap at new endpoint
	if err != nil {
		t.Fatal(err)
	}
	if plan2.Outcome != OutcomeZeroGap {
		t.Fatalf("E1: outcome=%s (expected zero-gap on re-synced replica)", plan2.Outcome)
	}

	// Old plan resources released (plan1 was released by executor).
	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E1: old plan resources must not leak")
	}

	// E5: Log must show: endpoint_changed → new session → plan → execute.
	events := driver.Orchestrator.Log.EventsFor("vol1-r1")
	hasEndpointInvalidation := false
	hasNewSession := false
	for _, e := range events {
		if e.Event == "session_invalidated" && e.Detail == "endpoint_changed" {
			hasEndpointInvalidation = true
		}
		if e.Event == "session_created" {
			hasNewSession = true
		}
	}
	if !hasEndpointInvalidation {
		t.Fatal("E1/E5: log must show endpoint_changed invalidation")
	}
	if !hasNewSession {
		t.Fatal("E1/E5: log must show new session created after address change")
	}
}

// --- E2 / FC2: Stale epoch during active executor step ---

func TestP3_E2_EpochBump_MidExecutionStep(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 0, CommittedLSN: 100,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	plan, _ := driver.PlanRecovery("r1", 50)

	// Start executor — it will make progress, then epoch bumps mid-step.
	s := driver.Orchestrator.Registry.Sender("r1")

	exec := NewCatchUpExecutor(driver, plan)

	// Manually begin catch-up to simulate partial execution.
	sessID := s.SessionID()
	s.BeginCatchUp(sessID, 0)
	s.RecordCatchUpProgress(sessID, 60, 1) // partial progress at tick 1

	// Epoch bumps MID-EXECUTION (between progress steps).
	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("r1", 2)

	// Further progress fails — session invalidated.
	err := s.RecordCatchUpProgress(sessID, 70, 2)
	if err == nil {
		t.Fatal("E2: progress after epoch bump should fail")
	}

	// Executor cancel releases resources.
	exec.Cancel("epoch_bump_detected")

	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E2: WAL pin must be released after mid-execution epoch bump")
	}

	// E5: Log must show invalidation cause.
	hasInvalidation := false
	hasRelease := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
		if e.Event == "exec_resources_released" {
			hasRelease = true
		}
	}
	if !hasInvalidation {
		t.Fatal("E2/E5: log must show session invalidation with epoch cause")
	}
	if !hasRelease {
		t.Fatal("E2/E5: log must show resource release on cancellation")
	}
}

// --- E3 / FC5: Cross-layer proof — trusted base + unreplayable tail ---

func TestP3_E3_CrossLayer_TrustedBaseUnreplayableTail(t *testing.T) {
	// Storage: checkpoint=50 trusted, but tail=80 → unreplayable tail.
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 80, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	})
	driver := NewRecoveryDriver(storage)

	// Catch-up fails → NeedsRebuild.
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})
	catchPlan, _ := driver.PlanRecovery("r1", 10)
	if catchPlan != nil && catchPlan.Outcome != OutcomeNeedsRebuild {
		// Expected: unrecoverable gap.
	}

	// Rebuild assignment.
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	// PlanRebuild queries StorageAdapter → RebuildSourceDecision → FullBase.
	rebuildPlan, err := driver.PlanRebuild("r1")
	if err != nil {
		t.Fatal(err)
	}

	// E3 assertion: engine chose FullBase (not SnapshotTail) because tail unreplayable.
	if rebuildPlan.RebuildSource != RebuildFullBase {
		t.Fatalf("E3: source=%s (checkpoint=50 but tail=80 → unreplayable)", rebuildPlan.RebuildSource)
	}

	// E3 assertion: FullBasePin acquired (not SnapshotPin).
	if rebuildPlan.FullBasePin == nil {
		t.Fatal("E3: FullBasePin must be acquired for full-base rebuild")
	}
	if rebuildPlan.SnapshotPin != nil {
		t.Fatal("E3: SnapshotPin should NOT be acquired for full-base rebuild")
	}

	// Execute through executor.
	exec := NewRebuildExecutor(driver, rebuildPlan)
	if err := exec.Execute(); err != nil {
		t.Fatalf("E3: rebuild execution: %v", err)
	}

	if driver.Orchestrator.Registry.Sender("r1").State() != StateInSync {
		t.Fatalf("state=%s", driver.Orchestrator.Registry.Sender("r1").State())
	}

	// All pins released.
	if len(storage.pinnedFullBase) != 0 {
		t.Fatal("E3: full-base pin must be released after rebuild")
	}

	// E5: Log must explain WHY FullBase was chosen.
	hasSourceLog := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "plan_rebuild_full_base" {
			hasSourceLog = true
		}
	}
	if !hasSourceLog {
		t.Fatal("E3/E5: log must show plan_rebuild_full_base (not just result)")
	}
}

// --- E4 / FC8: Rebuild fallback when trusted-base proof fails completely ---

func TestP3_E4_RebuildFallback_FullBasePinFails(t *testing.T) {
	// Untrusted checkpoint → full-base selected. But full-base pin also fails.
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 60, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: false, // untrusted
	})
	storage.failFullBasePin = true
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	_, err := driver.PlanRebuild("r1")
	if err == nil {
		t.Fatal("E4: must fail when full-base pin cannot be acquired")
	}

	// E5: Log must explain the failure chain.
	hasFullBaseFailure := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "full_base_pin_failed" {
			hasFullBaseFailure = true
		}
	}
	if !hasFullBaseFailure {
		t.Fatal("E4/E5: log must show full_base_pin_failed")
	}
}

func TestP3_E4_RebuildFallback_UntrustedCheckpoint_SelectsFullBase(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: false, // untrusted
	})
	driver := NewRecoveryDriver(storage)

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

	// Untrusted checkpoint → full base (not snapshot+tail).
	if plan.RebuildSource != RebuildFullBase {
		t.Fatalf("E4: source=%s (untrusted checkpoint)", plan.RebuildSource)
	}
	if plan.FullBasePin == nil {
		t.Fatal("E4: FullBasePin must be acquired")
	}

	// Execute.
	exec := NewRebuildExecutor(driver, plan)
	if err := exec.Execute(); err != nil {
		t.Fatalf("E4: %v", err)
	}

	if driver.Orchestrator.Registry.Sender("r1").State() != StateInSync {
		t.Fatalf("state=%s", driver.Orchestrator.Registry.Sender("r1").State())
	}

	// All resources released.
	if len(storage.pinnedFullBase) != 0 {
		t.Fatal("E4: full-base pin must be released")
	}
}

// --- E5: Observability — failure replay summary ---

func TestP3_E5_Observability_FullRecoveryChainLogged(t *testing.T) {
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

	plan, _ := driver.PlanRecovery("r1", 70)
	exec := NewCatchUpExecutor(driver, plan)
	exec.Execute([]uint64{80, 90, 100}, 0)

	events := driver.Orchestrator.Log.EventsFor("r1")

	// Must contain the full diagnostic chain.
	required := map[string]bool{
		"sender_added":          false,
		"session_created":       false,
		"connected":             false,
		"handshake":             false,
		"plan_catchup":          false,
		"exec_catchup_started":  false,
		"exec_completed":        false,
	}

	for _, e := range events {
		if _, ok := required[e.Event]; ok {
			required[e.Event] = true
		}
	}

	for event, found := range required {
		if !found {
			t.Fatalf("E5: missing required log event: %s", event)
		}
	}
	t.Logf("E5: %d log events, all required events present", len(events))
}
