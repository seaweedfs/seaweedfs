package replication

import "testing"

// ============================================================
// Phase 05 Slice 1: Engine ownership/fencing tests
// ============================================================

// --- Changed-address invalidation (A10) ---

func TestEngine_ChangedAddress_InvalidatesSession(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", CtrlAddr: "r1:9334", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	s := r.Sender("r1:9333")
	sessID := s.SessionID()
	s.BeginConnect(sessID)

	r.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", CtrlAddr: "r1:9445", Version: 2},
	}, 1)

	if s.HasActiveSession() {
		t.Fatal("session should be invalidated")
	}
	if s.State() != StateDisconnected {
		t.Fatalf("state=%s", s.State())
	}
}

func TestEngine_ChangedAddress_NewSessionAfterUpdate(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	s := r.Sender("r1:9333")
	oldID := s.SessionID()

	r.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 2},
	}, 1)
	result := r.ApplyAssignment(AssignmentIntent{
		Endpoints:       map[string]Endpoint{"r1:9333": {DataAddr: "r1:9333", Version: 2}},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	if len(result.SessionsCreated) != 1 {
		t.Fatalf("should create new session: %v", result)
	}
	if s.SessionID() == oldID {
		t.Fatal("should have different session ID")
	}
}

// --- Stale-session rejection (A3) ---

func TestEngine_StaleSessionID_RejectedAtAllAPIs(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	staleID, _ := s.AttachSession(1, SessionCatchUp)

	s.UpdateEpoch(2)
	s.AttachSession(2, SessionCatchUp)

	if err := s.BeginConnect(staleID); err == nil {
		t.Fatal("stale ID: BeginConnect should reject")
	}
	if err := s.RecordHandshake(staleID, 0, 10); err == nil {
		t.Fatal("stale ID: RecordHandshake should reject")
	}
	if err := s.BeginCatchUp(staleID); err == nil {
		t.Fatal("stale ID: BeginCatchUp should reject")
	}
	if err := s.RecordCatchUpProgress(staleID, 5); err == nil {
		t.Fatal("stale ID: RecordCatchUpProgress should reject")
	}
	if s.CompleteSessionByID(staleID) {
		t.Fatal("stale ID: CompleteSessionByID should reject")
	}
}

func TestEngine_StaleCompletion_AfterSupersede(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id1, _ := s.AttachSession(1, SessionCatchUp)

	s.UpdateEpoch(2)
	s.AttachSession(2, SessionCatchUp)

	if s.CompleteSessionByID(id1) {
		t.Fatal("stale completion must be rejected")
	}
	if s.HasActiveSession() != true {
		t.Fatal("new session should be active")
	}
	if s.State() == StateInSync {
		t.Fatal("sender should not be InSync from stale completion")
	}
}

// --- Epoch-bump invalidation (A3) ---

func TestEngine_EpochBump_InvalidatesAllSessions(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
			"r2:9333": {DataAddr: "r2:9333", Version: 1},
		},
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1:9333": SessionCatchUp,
			"r2:9333": SessionCatchUp,
		},
	})

	count := r.InvalidateEpoch(2)
	if count != 2 {
		t.Fatalf("should invalidate 2, got %d", count)
	}
	if r.Sender("r1:9333").HasActiveSession() || r.Sender("r2:9333").HasActiveSession() {
		t.Fatal("both sessions should be invalidated")
	}
}

func TestEngine_EpochBump_StaleAssignment_Rejected(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{"r1:9333": {DataAddr: "r1:9333", Version: 1}},
		Epoch:     2,
	})

	result := r.ApplyAssignment(AssignmentIntent{
		Endpoints:       map[string]Endpoint{"r1:9333": {DataAddr: "r1:9333", Version: 1}},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	if len(result.SessionsFailed) != 1 {
		t.Fatalf("stale epoch should fail: %v", result)
	}
}

// --- Rebuild exclusivity ---

func TestEngine_Rebuild_CatchUpAPIs_Rejected(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sessID, _ := s.AttachSession(1, SessionRebuild)
	s.BeginConnect(sessID)
	s.RecordHandshake(sessID, 0, 100)

	if err := s.BeginCatchUp(sessID); err == nil {
		t.Fatal("rebuild: BeginCatchUp should reject")
	}
	if err := s.RecordCatchUpProgress(sessID, 50); err == nil {
		t.Fatal("rebuild: RecordCatchUpProgress should reject")
	}
	if s.CompleteSessionByID(sessID) {
		t.Fatal("rebuild: catch-up completion should reject")
	}
}

func TestEngine_Rebuild_FullLifecycle(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
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
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sessID, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(sessID)
	s.RecordHandshake(sessID, 0, 50)
	s.BeginCatchUp(sessID)

	if err := s.RecordCatchUpProgress(sessID, 51); err == nil {
		t.Fatal("beyond frozen target should be rejected")
	}
}

func TestEngine_BudgetViolation_Escalates(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
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

// --- Encapsulation: no direct state mutation ---

func TestEngine_Encapsulation_SnapshotIsReadOnly(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sessID, _ := s.AttachSession(1, SessionCatchUp)

	snap := s.SessionSnapshot()
	if snap == nil || !snap.Active {
		t.Fatal("should have active session snapshot")
	}

	// Mutating the snapshot does not affect the sender.
	snap.Phase = PhaseCompleted
	snap.Active = false

	// Sender's session is still active.
	if !s.HasActiveSession() {
		t.Fatal("sender should still have active session after snapshot mutation")
	}
	snap2 := s.SessionSnapshot()
	if snap2.Phase == PhaseCompleted {
		t.Fatal("snapshot mutation should not leak back to sender")
	}

	// Can still execute on the real session.
	if err := s.BeginConnect(sessID); err != nil {
		t.Fatalf("execution should still work: %v", err)
	}
}

// --- E2E ---

func TestEngine_E2E_ThreeReplicas_ThreeOutcomes(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
			"r2:9333": {DataAddr: "r2:9333", Version: 1},
			"r3:9333": {DataAddr: "r3:9333", Version: 1},
		},
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1:9333": SessionCatchUp,
			"r2:9333": SessionCatchUp,
			"r3:9333": SessionCatchUp,
		},
	})

	// r1: zero-gap.
	r1 := r.Sender("r1:9333")
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
	r2 := r.Sender("r2:9333")
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
	r3 := r.Sender("r3:9333")
	id3 := r3.SessionID()
	r3.BeginConnect(id3)
	o3, _ := r3.RecordHandshakeWithOutcome(id3, HandshakeResult{
		ReplicaFlushedLSN: 10, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	if o3 != OutcomeNeedsRebuild {
		t.Fatalf("r3: %s", o3)
	}

	if r1.State() != StateInSync || r2.State() != StateInSync {
		t.Fatalf("r1=%s r2=%s", r1.State(), r2.State())
	}
	if r3.State() != StateNeedsRebuild {
		t.Fatalf("r3=%s", r3.State())
	}
	if r.InSyncCount() != 2 {
		t.Fatalf("in_sync=%d", r.InSyncCount())
	}
}
