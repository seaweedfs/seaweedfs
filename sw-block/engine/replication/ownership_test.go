package replication

import "testing"

// ============================================================
// Phase 05 Slice 1: Engine ownership/fencing tests
// ============================================================

// Helper: build ReplicaAssignment list from map.
func replicas(m map[string]Endpoint) []ReplicaAssignment {
	var out []ReplicaAssignment
	for id, ep := range m {
		out = append(out, ReplicaAssignment{ReplicaID: id, Endpoint: ep})
	}
	return out
}

// --- Changed-address invalidation (A10) ---

func TestEngine_ChangedDataAddr_PreservesSenderIdentity(t *testing.T) {
	// THE core V2 test: DataAddr changes but stable ReplicaID stays.
	// Sender must survive. Session must be invalidated (endpoint changed).
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", CtrlAddr: "10.0.0.1:9334", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"replica-1": SessionCatchUp},
	})

	s := r.Sender("replica-1")
	sessID := s.SessionID()
	s.BeginConnect(sessID)

	// DataAddr changes (replica restarted on different port/IP).
	r.Reconcile([]ReplicaAssignment{
		{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", Version: 2}},
	}, 1)

	// Sender identity preserved (same pointer, same ReplicaID).
	if r.Sender("replica-1") != s {
		t.Fatal("sender identity must be preserved across DataAddr change")
	}
	// Session invalidated (endpoint changed).
	if s.HasActiveSession() {
		t.Fatal("session should be invalidated by DataAddr change")
	}
	// Endpoint updated.
	if s.Endpoint().DataAddr != "10.0.0.2:9333" {
		t.Fatalf("endpoint not updated: %s", s.Endpoint().DataAddr)
	}

	// New session can be attached on the updated endpoint.
	result := r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", Version: 2}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"replica-1": SessionCatchUp},
	})
	if len(result.SessionsCreated) != 1 {
		t.Fatalf("should create new session: %v", result)
	}
	if s.SessionID() == sessID {
		t.Fatal("new session should have different ID")
	}
	t.Logf("DataAddr changed: sender preserved, old session invalidated, new session attached")
}

func TestEngine_ChangedCtrlAddr_InvalidatesSession(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Replicas: replicas(map[string]Endpoint{
			"r1": {DataAddr: "10.0.0.1:9333", CtrlAddr: "10.0.0.1:9334", Version: 1},
		}),
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	s := r.Sender("r1")
	sessID := s.SessionID()
	s.BeginConnect(sessID)

	r.Reconcile(replicas(map[string]Endpoint{
		"r1": {DataAddr: "10.0.0.1:9333", CtrlAddr: "10.0.0.1:9445", Version: 2},
	}), 1)

	if s.HasActiveSession() {
		t.Fatal("CtrlAddr change should invalidate session")
	}
	if s.State() != StateDisconnected {
		t.Fatalf("state=%s", s.State())
	}
}

// --- Stale-session rejection (A3) ---

func TestEngine_StaleSessionID_RejectedAtAllAPIs(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	staleID, _ := s.AttachSession(1, SessionCatchUp)

	s.UpdateEpoch(2)
	s.AttachSession(2, SessionCatchUp)

	if err := s.BeginConnect(staleID); err == nil {
		t.Fatal("stale BeginConnect should reject")
	}
	if err := s.RecordHandshake(staleID, 0, 10); err == nil {
		t.Fatal("stale RecordHandshake should reject")
	}
	if err := s.BeginCatchUp(staleID); err == nil {
		t.Fatal("stale BeginCatchUp should reject")
	}
	if err := s.RecordCatchUpProgress(staleID, 5); err == nil {
		t.Fatal("stale RecordCatchUpProgress should reject")
	}
	if s.CompleteSessionByID(staleID) {
		t.Fatal("stale CompleteSessionByID should reject")
	}
}

func TestEngine_StaleCompletion_AfterSupersede(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id1, _ := s.AttachSession(1, SessionCatchUp)

	s.UpdateEpoch(2)
	s.AttachSession(2, SessionCatchUp)

	if s.CompleteSessionByID(id1) {
		t.Fatal("stale completion must be rejected")
	}
	if !s.HasActiveSession() {
		t.Fatal("new session should be active")
	}
}

// --- Epoch-bump invalidation (A3) ---

func TestEngine_EpochBump_InvalidatesAllSessions(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Replicas: replicas(map[string]Endpoint{
			"r1": {DataAddr: "r1:9333", Version: 1},
			"r2": {DataAddr: "r2:9333", Version: 1},
		}),
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1": SessionCatchUp,
			"r2": SessionCatchUp,
		},
	})

	count := r.InvalidateEpoch(2)
	if count != 2 {
		t.Fatalf("invalidated=%d, want 2", count)
	}
}

func TestEngine_EpochBump_StaleAssignment_Rejected(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Replicas: replicas(map[string]Endpoint{"r1": {DataAddr: "r1:9333", Version: 1}}),
		Epoch:    2,
	})

	result := r.ApplyAssignment(AssignmentIntent{
		Replicas:        replicas(map[string]Endpoint{"r1": {DataAddr: "r1:9333", Version: 1}}),
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	if len(result.SessionsFailed) != 1 {
		t.Fatalf("stale epoch should fail: %v", result)
	}
}

// --- Rebuild exclusivity ---

func TestEngine_Rebuild_CatchUpAPIs_Rejected(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sessID, _ := s.AttachSession(1, SessionRebuild)
	s.BeginConnect(sessID)
	s.RecordHandshake(sessID, 0, 100)

	if err := s.BeginCatchUp(sessID); err == nil {
		t.Fatal("rebuild: BeginCatchUp should reject")
	}
	if s.CompleteSessionByID(sessID) {
		t.Fatal("rebuild: catch-up completion should reject")
	}
}

func TestEngine_Rebuild_FullLifecycle(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sessID, _ := s.AttachSession(1, SessionRebuild)

	s.BeginConnect(sessID)
	s.RecordHandshake(sessID, 0, 100)
	s.SelectRebuildSource(sessID, 50, true, 100)
	s.BeginRebuildTransfer(sessID)
	s.RecordRebuildTransferProgress(sessID, 50)
	s.BeginRebuildTailReplay(sessID)
	s.RecordRebuildTailProgress(sessID, 100)

	if err := s.CompleteRebuild(sessID); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

// --- Bounded catch-up ---

func TestEngine_FrozenTarget_RejectsChase(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sessID, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(sessID)
	s.RecordHandshake(sessID, 0, 50)
	s.BeginCatchUp(sessID)

	if err := s.RecordCatchUpProgress(sessID, 51); err == nil {
		t.Fatal("beyond frozen target should be rejected")
	}
}

func TestEngine_BudgetViolation_Escalates(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sessID, _ := s.AttachSession(1, SessionCatchUp, WithBudget(CatchUpBudget{MaxDurationTicks: 5}))

	s.BeginConnect(sessID)
	s.RecordHandshake(sessID, 0, 100)
	s.BeginCatchUp(sessID, 0)
	s.RecordCatchUpProgress(sessID, 10)

	v, _ := s.CheckBudget(sessID, 10)
	if v != BudgetDurationExceeded {
		t.Fatalf("budget=%s", v)
	}
	if s.State() != StateNeedsRebuild {
		t.Fatalf("state=%s", s.State())
	}
}

// --- Encapsulation ---

func TestEngine_Encapsulation_SnapshotIsReadOnly(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sessID, _ := s.AttachSession(1, SessionCatchUp)

	snap := s.SessionSnapshot()
	snap.Phase = PhaseCompleted
	snap.Active = false

	if !s.HasActiveSession() {
		t.Fatal("snapshot mutation should not affect sender")
	}
	if err := s.BeginConnect(sessID); err != nil {
		t.Fatalf("execution should still work: %v", err)
	}
}

// --- E2E ---

func TestEngine_E2E_ThreeReplicas_ThreeOutcomes(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Replicas: replicas(map[string]Endpoint{
			"r1": {DataAddr: "r1:9333", Version: 1},
			"r2": {DataAddr: "r2:9333", Version: 1},
			"r3": {DataAddr: "r3:9333", Version: 1},
		}),
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1": SessionCatchUp,
			"r2": SessionCatchUp,
			"r3": SessionCatchUp,
		},
	})

	// r1: zero-gap.
	r1 := r.Sender("r1")
	id1 := r1.SessionID()
	r1.BeginConnect(id1)
	o1, _ := r1.RecordHandshakeWithOutcome(id1, HandshakeResult{
		ReplicaFlushedLSN: 100, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	if o1 != OutcomeZeroGap {
		t.Fatalf("r1: %s", o1)
	}
	r1.CompleteSessionByID(id1)

	// r2: catch-up.
	r2 := r.Sender("r2")
	id2 := r2.SessionID()
	r2.BeginConnect(id2)
	o2, _ := r2.RecordHandshakeWithOutcome(id2, HandshakeResult{
		ReplicaFlushedLSN: 70, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	if o2 != OutcomeCatchUp {
		t.Fatalf("r2: %s", o2)
	}
	r2.BeginCatchUp(id2)
	r2.RecordCatchUpProgress(id2, 100)
	r2.CompleteSessionByID(id2)

	// r3: needs rebuild.
	r3 := r.Sender("r3")
	id3 := r3.SessionID()
	r3.BeginConnect(id3)
	o3, _ := r3.RecordHandshakeWithOutcome(id3, HandshakeResult{
		ReplicaFlushedLSN: 10, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	if o3 != OutcomeNeedsRebuild {
		t.Fatalf("r3: %s", o3)
	}

	if r.InSyncCount() != 2 {
		t.Fatalf("in_sync=%d", r.InSyncCount())
	}
}
