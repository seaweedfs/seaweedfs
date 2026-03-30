package replication

import "testing"

// ============================================================
// Phase 05 Slice 1: Engine ownership/fencing tests
// Mapped to V2 acceptance criteria and boundary cases.
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
	sess := s.Session()
	s.BeginConnect(sess.ID)

	// Address changes mid-recovery.
	r.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", CtrlAddr: "r1:9445", Version: 2},
	}, 1)

	if sess.Active() {
		t.Fatal("session should be invalidated by address change")
	}
	if r.Sender("r1:9333") != s {
		t.Fatal("sender identity should be preserved")
	}
	if s.State != StateDisconnected {
		t.Fatalf("state=%s, want disconnected", s.State)
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
	oldSess := s.Session()
	s.BeginConnect(oldSess.ID)

	// Address change + new assignment.
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
	newSess := s.Session()
	if newSess.ID == oldSess.ID {
		t.Fatal("new session should have different ID")
	}
}

// --- Stale-session rejection (A3) ---

func TestEngine_StaleSessionID_RejectedAtAllAPIs(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	staleID := sess.ID

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
	sess1, _ := s.AttachSession(1, SessionCatchUp)
	id1 := sess1.ID

	s.UpdateEpoch(2)
	sess2, _ := s.AttachSession(2, SessionCatchUp)

	// Old session completion rejected.
	if s.CompleteSessionByID(id1) {
		t.Fatal("stale completion must be rejected")
	}
	// New session still active.
	if !sess2.Active() {
		t.Fatal("new session should be active")
	}
	// Sender not moved to InSync.
	if s.State == StateInSync {
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
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{
			"r1:9333": SessionCatchUp,
			"r2:9333": SessionCatchUp,
		},
	})

	s1 := r.Sender("r1:9333")
	s2 := r.Sender("r2:9333")
	sess1 := s1.Session()
	sess2 := s2.Session()

	count := r.InvalidateEpoch(2)
	if count != 2 {
		t.Fatalf("should invalidate 2, got %d", count)
	}
	if sess1.Active() || sess2.Active() {
		t.Fatal("both sessions should be invalidated")
	}
}

func TestEngine_EpochBump_StaleAssignment_Rejected(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch: 2,
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
	sess, _ := s.AttachSession(1, SessionRebuild)
	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)

	if err := s.BeginCatchUp(sess.ID); err == nil {
		t.Fatal("rebuild: BeginCatchUp should be rejected")
	}
	if err := s.RecordCatchUpProgress(sess.ID, 50); err == nil {
		t.Fatal("rebuild: RecordCatchUpProgress should be rejected")
	}
	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("rebuild: catch-up completion should be rejected")
	}
}

func TestEngine_Rebuild_FullLifecycle(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionRebuild)

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.SelectRebuildSource(sess.ID, 50, true, 100)
	s.BeginRebuildTransfer(sess.ID)
	s.RecordRebuildTransferProgress(sess.ID, 50)
	s.BeginRebuildTailReplay(sess.ID)
	s.RecordRebuildTailProgress(sess.ID, 100)

	if err := s.CompleteRebuild(sess.ID); err != nil {
		t.Fatalf("rebuild completion: %v", err)
	}
	if s.State != StateInSync {
		t.Fatalf("state=%s, want in_sync", s.State)
	}
}

// --- Bounded catch-up ---

func TestEngine_FrozenTarget_RejectsChase(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 50)
	s.BeginCatchUp(sess.ID)

	if err := s.RecordCatchUpProgress(sess.ID, 51); err == nil {
		t.Fatal("progress beyond frozen target should be rejected")
	}
}

func TestEngine_BudgetViolation_Escalates(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	sess.Budget = &CatchUpBudget{MaxDurationTicks: 5}

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.BeginCatchUp(sess.ID, 0)
	s.RecordCatchUpProgress(sess.ID, 10)

	v, _ := s.CheckBudget(sess.ID, 10)
	if v != BudgetDurationExceeded {
		t.Fatalf("budget=%s", v)
	}
	if s.State != StateNeedsRebuild {
		t.Fatalf("state=%s", s.State)
	}
}

// --- E2E: 3 replicas, 3 outcomes ---

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
	s1 := r1.Session()
	r1.BeginConnect(s1.ID)
	o1, _ := r1.RecordHandshakeWithOutcome(s1.ID, HandshakeResult{
		ReplicaFlushedLSN: 100, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	if o1 != OutcomeZeroGap {
		t.Fatalf("r1: %s", o1)
	}
	r1.CompleteSessionByID(s1.ID)

	// r2: catch-up.
	r2 := r.Sender("r2:9333")
	s2 := r2.Session()
	r2.BeginConnect(s2.ID)
	o2, _ := r2.RecordHandshakeWithOutcome(s2.ID, HandshakeResult{
		ReplicaFlushedLSN: 70, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	if o2 != OutcomeCatchUp {
		t.Fatalf("r2: %s", o2)
	}
	r2.BeginCatchUp(s2.ID)
	r2.RecordCatchUpProgress(s2.ID, 100)
	r2.CompleteSessionByID(s2.ID)

	// r3: needs rebuild.
	r3 := r.Sender("r3:9333")
	s3 := r3.Session()
	r3.BeginConnect(s3.ID)
	o3, _ := r3.RecordHandshakeWithOutcome(s3.ID, HandshakeResult{
		ReplicaFlushedLSN: 10, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	if o3 != OutcomeNeedsRebuild {
		t.Fatalf("r3: %s", o3)
	}

	// Final states.
	if r1.State != StateInSync || r2.State != StateInSync {
		t.Fatalf("r1=%s r2=%s", r1.State, r2.State)
	}
	if r3.State != StateNeedsRebuild {
		t.Fatalf("r3=%s", r3.State)
	}
	if r.InSyncCount() != 2 {
		t.Fatalf("in_sync=%d", r.InSyncCount())
	}
}
