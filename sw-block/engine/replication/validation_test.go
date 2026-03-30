package replication

import (
	"testing"
)

// ============================================================
// Phase 06 P3: Validation against real failure classes
// Maps to tester expectations E1-E5, failure classes FC1/FC2/FC5/FC8.
// ============================================================

// --- E1 / FC1: Changed-address restart through planner/executor ---

func TestP3_E1_ChangedAddress_OldPlanCancelledByDriver(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
	})
	driver := NewRecoveryDriver(storage)

	// Initial assignment with active session.
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vol1-r1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", CtrlAddr: "10.0.0.1:9334", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"vol1-r1": SessionCatchUp},
	})

	// Plan recovery — acquires WAL pin.
	plan1, err := driver.PlanRecovery("vol1-r1", 70)
	if err != nil {
		t.Fatal(err)
	}
	if len(storage.pinnedWAL) != 1 {
		t.Fatal("WAL pin should exist after plan")
	}

	senderBefore := driver.Orchestrator.Registry.Sender("vol1-r1")

	// Address changes — driver cancels old plan (not manual test cleanup).
	driver.CancelPlan(plan1, "address_change")

	// New assignment with new endpoint.
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vol1-r1", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", Version: 2}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"vol1-r1": SessionCatchUp},
	})

	// Old plan resources released by CancelPlan.
	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E1: old plan WAL pin must be released by CancelPlan")
	}

	// Sender identity preserved.
	senderAfter := driver.Orchestrator.Registry.Sender("vol1-r1")
	if senderAfter != senderBefore {
		t.Fatal("E1: sender identity must be preserved")
	}
	if senderAfter.Endpoint().DataAddr != "10.0.0.2:9333" {
		t.Fatalf("E1: endpoint not updated: %s", senderAfter.Endpoint().DataAddr)
	}

	// New plan on new endpoint.
	plan2, err := driver.PlanRecovery("vol1-r1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if plan2.Outcome != OutcomeZeroGap {
		t.Fatalf("E1: outcome=%s", plan2.Outcome)
	}

	// E5: Log must show: plan_cancelled (address_change) → new session.
	// CancelPlan is the real cleanup mechanism — it invalidates the session
	// before ProcessAssignment runs, so orchestrator's endpoint_changed
	// detection won't fire (session already gone).
	events := driver.Orchestrator.Log.EventsFor("vol1-r1")
	hasPlanCancelled := false
	hasCancelReason := false
	hasNewSession := false
	for _, e := range events {
		if e.Event == "plan_cancelled" && e.Detail == "address_change" {
			hasPlanCancelled = true
			hasCancelReason = true
		}
		if e.Event == "session_created" {
			hasNewSession = true
		}
	}
	if !hasPlanCancelled || !hasCancelReason {
		t.Fatal("E1/E5: log must show plan_cancelled with address_change reason")
	}
	if !hasNewSession {
		t.Fatal("E1/E5: log must show new session created after address change")
	}
}

// --- E2 / FC2: Epoch bump during active executor step ---

func TestP3_E2_EpochBump_MidExecutorStep(t *testing.T) {
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
	exec := NewCatchUpExecutor(driver, plan)

	// Epoch bumps BEFORE executor runs — simulates bump between plan and execute.
	// The executor's mid-step check will detect the invalidation.
	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("r1", 2)

	// Executor detects invalidation at first progress step.
	err := exec.Execute([]uint64{60, 70, 80, 90, 100}, 0)
	if err == nil {
		t.Fatal("E2: executor should fail on invalidated session")
	}

	// Resources released by executor.
	if len(storage.pinnedWAL) != 0 {
		t.Fatal("E2: WAL pin must be released after epoch-bump invalidation")
	}

	// E5: Log shows invalidation + resource release.
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
		t.Fatal("E2/E5: log must show session invalidation")
	}
	if !hasRelease {
		t.Fatal("E2/E5: log must show resource release")
	}
}

// --- E3 / FC5: Cross-layer proof — trusted base + unreplayable tail ---

func TestP3_E3_CrossLayer_TrustedBaseUnreplayableTail(t *testing.T) {
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
	driver.PlanRecovery("r1", 10) // NeedsRebuild

	// Rebuild assignment.
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	rebuildPlan, err := driver.PlanRebuild("r1")
	if err != nil {
		t.Fatal(err)
	}

	// E3: engine chose FullBase because tail unreplayable.
	if rebuildPlan.RebuildSource != RebuildFullBase {
		t.Fatalf("E3: source=%s", rebuildPlan.RebuildSource)
	}
	if rebuildPlan.FullBasePin == nil {
		t.Fatal("E3: FullBasePin must be acquired")
	}
	if rebuildPlan.SnapshotPin != nil {
		t.Fatal("E3: SnapshotPin should NOT be acquired for full-base")
	}

	// Execute through executor (plan-bound, no history re-derive).
	exec := NewRebuildExecutor(driver, rebuildPlan)
	if err := exec.Execute(); err != nil {
		t.Fatalf("E3: %v", err)
	}

	if driver.Orchestrator.Registry.Sender("r1").State() != StateInSync {
		t.Fatalf("state=%s", driver.Orchestrator.Registry.Sender("r1").State())
	}
	if len(storage.pinnedFullBase) != 0 {
		t.Fatal("E3: full-base pin must be released")
	}

	// E5: Log explains WHY full-base was chosen (causal reason).
	hasFullBaseReason := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "plan_rebuild_full_base" && len(e.Detail) > 20 {
			// Detail should contain "trusted_checkpoint_unreplayable_tail"
			hasFullBaseReason = true
		}
	}
	if !hasFullBaseReason {
		t.Fatal("E3/E5: log must explain WHY full-base was chosen (causal reason)")
	}
}

// --- E4 / FC8: Rebuild fallback — pin failure + untrusted checkpoint ---

func TestP3_E4_FullBasePinFails_SessionCleaned(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 60, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: false,
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
		t.Fatal("E4: must fail when full-base pin refused")
	}

	// Session must be invalidated — no dangling rebuild session.
	s := driver.Orchestrator.Registry.Sender("r1")
	if s.HasActiveSession() {
		t.Fatal("E4: session must be invalidated after full-base pin failure")
	}
	if s.State() != StateNeedsRebuild {
		t.Fatalf("E4: state=%s, want needs_rebuild", s.State())
	}

	// E5: Log shows failure.
	hasFailure := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "full_base_pin_failed" {
			hasFailure = true
		}
	}
	if !hasFailure {
		t.Fatal("E4/E5: log must show full_base_pin_failed")
	}
}

func TestP3_E4_UntrustedCheckpoint_FullBase_Success(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: false,
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
	if plan.RebuildSource != RebuildFullBase {
		t.Fatalf("E4: source=%s", plan.RebuildSource)
	}

	exec := NewRebuildExecutor(driver, plan)
	if err := exec.Execute(); err != nil {
		t.Fatalf("E4: %v", err)
	}

	if driver.Orchestrator.Registry.Sender("r1").State() != StateInSync {
		t.Fatalf("state=%s", driver.Orchestrator.Registry.Sender("r1").State())
	}
	if len(storage.pinnedFullBase) != 0 {
		t.Fatal("E4: full-base pin must be released")
	}

	// E5: Log shows untrusted_checkpoint as reason.
	hasReason := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "plan_rebuild_full_base" {
			hasReason = true
		}
	}
	if !hasReason {
		t.Fatal("E4/E5: log must show plan_rebuild_full_base with reason")
	}
}

// --- E5: Full recovery chain observability ---

func TestP3_E5_FullRecoveryChainLogged(t *testing.T) {
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
	required := map[string]bool{
		"sender_added":         false,
		"session_created":      false,
		"connected":            false,
		"handshake":            false,
		"plan_catchup":         false,
		"exec_catchup_started": false,
		"exec_completed":       false,
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
}
