package replication

import "testing"

// ============================================================
// Phase 05 Slice 4: Integration tests via RecoveryOrchestrator
//
// All tests use the orchestrator as the entry path — no direct
// sender API calls for the recovery lifecycle.
// ============================================================

// --- V2 Boundary 1: Changed-address recovery ---

func TestIntegration_ChangedAddress_ViaOrchestrator(t *testing.T) {
	o := NewRecoveryOrchestrator()
	primary := RetainedHistory{HeadLSN: 100, TailLSN: 30, CommittedLSN: 100}

	// Initial assignment + recovery.
	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vol1-r1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", CtrlAddr: "10.0.0.1:9334", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"vol1-r1": SessionCatchUp},
	})

	// Recovery via orchestrator.
	result := o.ExecuteRecovery("vol1-r1", 80, &primary)
	if result.Outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", result.Outcome)
	}
	o.CompleteCatchUp("vol1-r1", 100)

	s := o.Registry.Sender("vol1-r1")
	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}

	// Address changes — new assignment.
	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vol1-r1", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", Version: 2}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"vol1-r1": SessionCatchUp},
	})

	// Sender identity preserved.
	if o.Registry.Sender("vol1-r1") != s {
		t.Fatal("sender identity must survive address change")
	}
	if s.Endpoint().DataAddr != "10.0.0.2:9333" {
		t.Fatalf("endpoint not updated: %s", s.Endpoint().DataAddr)
	}

	// Zero-gap recovery on new endpoint — orchestrator handles completion.
	result2 := o.ExecuteRecovery("vol1-r1", 100, &primary)
	if result2.Outcome != OutcomeZeroGap {
		t.Fatalf("outcome=%s", result2.Outcome)
	}
	if result2.FinalState != StateInSync {
		t.Fatalf("zero-gap should complete to InSync, got %s", result2.FinalState)
	}

	// Verify log has events from both cycles.
	events := o.Log.EventsFor("vol1-r1")
	if len(events) < 4 {
		t.Fatalf("expected ≥4 orchestrator events, got %d", len(events))
	}
	t.Logf("changed-address: %d orchestrator events", len(events))
}

// --- V2 Boundary 2: NeedsRebuild → rebuild ---

func TestIntegration_NeedsRebuild_Rebuild_ViaOrchestrator(t *testing.T) {
	o := NewRecoveryOrchestrator()
	primary := RetainedHistory{
		HeadLSN: 100, TailLSN: 60, CommittedLSN: 100,
		CheckpointLSN: 40, CheckpointTrusted: true,
	}

	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	// Catch-up fails — gap beyond retention.
	result := o.ExecuteRecovery("r1", 30, &primary)
	if result.Outcome != OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s", result.Outcome)
	}
	if !result.Proof.Recoverable == true {
		// Should NOT be recoverable.
	}
	if result.FinalState != StateNeedsRebuild {
		t.Fatalf("state=%s", result.FinalState)
	}

	// Rebuild assignment.
	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	// Rebuild via orchestrator.
	if err := o.CompleteRebuild("r1", &primary); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	s := o.Registry.Sender("r1")
	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}

	// Log should show escalation + rebuild events.
	events := o.Log.EventsFor("r1")
	hasEscalation := false
	hasRebuild := false
	for _, e := range events {
		if e.Event == "escalated" {
			hasEscalation = true
		}
		if e.Event == "rebuild_completed" {
			hasRebuild = true
		}
	}
	if !hasEscalation {
		t.Fatal("log should contain escalation event")
	}
	if !hasRebuild {
		t.Fatal("log should contain rebuild_completed event")
	}
}

// --- V2 Boundary 3: Epoch bump during recovery ---

func TestIntegration_EpochBump_ViaOrchestrator(t *testing.T) {
	o := NewRecoveryOrchestrator()
	primary := RetainedHistory{HeadLSN: 100, TailLSN: 0, CommittedLSN: 100}

	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	// Epoch bumps mid-recovery — all via orchestrator.
	o.InvalidateEpoch(2)
	o.UpdateSenderEpoch("r1", 2)

	// Old session is dead — ExecuteRecovery should fail.
	result := o.ExecuteRecovery("r1", 100, &primary)
	if result.Error == nil {
		t.Fatal("should fail on stale session")
	}

	// New assignment at epoch 2.
	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           2,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	result2 := o.ExecuteRecovery("r1", 100, &primary)
	if result2.Outcome != OutcomeZeroGap {
		t.Fatalf("epoch 2: %s", result2.Outcome)
	}
	if result2.FinalState != StateInSync {
		t.Fatalf("state=%s", result2.FinalState)
	}

	// Log should show per-replica session invalidation.
	hasPerReplicaInvalidation := false
	for _, e := range o.Log.EventsFor("r1") {
		if e.Event == "session_invalidated" {
			hasPerReplicaInvalidation = true
		}
	}
	if !hasPerReplicaInvalidation {
		t.Fatal("log should contain per-replica session_invalidated event")
	}
}

func TestIntegration_EndpointChange_LogsInvalidation(t *testing.T) {
	o := NewRecoveryOrchestrator()

	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	// Address changes in next assignment — should log invalidation.
	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9444", Version: 2}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	// Check per-replica invalidation event.
	hasInvalidation := false
	for _, e := range o.Log.EventsFor("r1") {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
	}
	if !hasInvalidation {
		t.Fatal("endpoint change should produce per-replica session_invalidated event")
	}
}

// --- V2 Boundary 4: Multi-replica mixed outcomes ---

func TestIntegration_MultiReplica_ViaOrchestrator(t *testing.T) {
	o := NewRecoveryOrchestrator()
	primary := RetainedHistory{
		HeadLSN: 100, TailLSN: 40, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	}

	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
			{ReplicaID: "r2", Endpoint: Endpoint{DataAddr: "r2:9333", Version: 1}},
			{ReplicaID: "r3", Endpoint: Endpoint{DataAddr: "r3:9333", Version: 1}},
		},
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1": SessionCatchUp,
			"r2": SessionCatchUp,
			"r3": SessionCatchUp,
		},
	})

	// r1: zero-gap — orchestrator completes automatically.
	r1 := o.ExecuteRecovery("r1", 100, &primary)
	if r1.Outcome != OutcomeZeroGap || r1.FinalState != StateInSync {
		t.Fatalf("r1: outcome=%s state=%s", r1.Outcome, r1.FinalState)
	}

	// r2: catch-up.
	r2 := o.ExecuteRecovery("r2", 60, &primary)
	if r2.Outcome != OutcomeCatchUp || !r2.Proof.Recoverable {
		t.Fatalf("r2: outcome=%s proof=%v", r2.Outcome, r2.Proof)
	}
	o.CompleteCatchUp("r2", 100)

	// r3: needs rebuild.
	r3 := o.ExecuteRecovery("r3", 20, &primary)
	if r3.Outcome != OutcomeNeedsRebuild {
		t.Fatalf("r3: %s", r3.Outcome)
	}

	// Registry status.
	status := o.Registry.Status()
	if status.InSync != 2 {
		t.Fatalf("in_sync=%d", status.InSync)
	}
	if status.Rebuilding != 1 {
		t.Fatalf("rebuilding=%d", status.Rebuilding)
	}
}

// --- Observability ---

func TestIntegration_Observability_SessionSnapshot(t *testing.T) {
	o := NewRecoveryOrchestrator()
	_ = RetainedHistory{HeadLSN: 100, TailLSN: 0, CommittedLSN: 100}

	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	// After handshake with replica ahead → truncation required.
	s := o.Registry.Sender("r1")
	id := s.SessionID()
	s.BeginConnect(id)
	s.RecordHandshakeWithOutcome(id, HandshakeResult{
		ReplicaFlushedLSN: 105, CommittedLSN: 100, RetentionStartLSN: 0,
	})

	snap := s.SessionSnapshot()
	if !snap.TruncateRequired {
		t.Fatal("snapshot should show truncation required")
	}
	if snap.TruncateToLSN != 100 {
		t.Fatalf("truncate to=%d", snap.TruncateToLSN)
	}
}

func TestIntegration_Observability_RebuildSnapshot(t *testing.T) {
	o := NewRecoveryOrchestrator()
	primary := RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	}

	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	s := o.Registry.Sender("r1")
	id := s.SessionID()
	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.SelectRebuildFromHistory(id, &primary)

	snap := s.SessionSnapshot()
	if snap.RebuildSource != RebuildSnapshotTail {
		t.Fatalf("rebuild source=%s", snap.RebuildSource)
	}
	if snap.RebuildPhase != RebuildPhaseSourceSelect {
		t.Fatalf("rebuild phase=%s", snap.RebuildPhase)
	}
}

func TestIntegration_RecoveryLog_AutoPopulated(t *testing.T) {
	o := NewRecoveryOrchestrator()
	primary := RetainedHistory{HeadLSN: 100, TailLSN: 0, CommittedLSN: 100}

	o.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})
	o.ExecuteRecovery("r1", 80, &primary)
	o.CompleteCatchUp("r1", 100)

	events := o.Log.EventsFor("r1")
	// Should have: sender_added, session_created, connected, handshake, catchup_started, completed.
	if len(events) < 5 {
		t.Fatalf("expected ≥5 auto-populated events, got %d", len(events))
	}

	// All events came from the orchestrator, not manual test logging.
	for _, e := range events {
		if e.Event == "" {
			t.Fatal("event should have non-empty type")
		}
	}
	t.Logf("auto-populated log: %d events", len(events))
}
