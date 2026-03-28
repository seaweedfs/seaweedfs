package enginev2

import "testing"

// ============================================================
// Phase 04 P2: Outcome branching, assignment intent, end-to-end
// ============================================================

// --- Recovery outcome classification ---

func TestOutcome_ZeroGap(t *testing.T) {
	o := ClassifyRecoveryOutcome(HandshakeResult{
		ReplicaFlushedLSN: 100,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if o != OutcomeZeroGap {
		t.Fatalf("got %s, want zero_gap", o)
	}
}

func TestOutcome_ZeroGap_ReplicaAtCommitted(t *testing.T) {
	// Replica has exactly the committed prefix — zero gap.
	// Note: replica may have uncommitted tail beyond CommittedLSN;
	// that is handled by truncation, not by recovery classification.
	o := ClassifyRecoveryOutcome(HandshakeResult{
		ReplicaFlushedLSN: 100,
		CommittedLSN:      100,
		RetentionStartLSN: 50,
	})
	if o != OutcomeZeroGap {
		t.Fatalf("got %s, want zero_gap", o)
	}
}

func TestOutcome_CatchUp(t *testing.T) {
	o := ClassifyRecoveryOutcome(HandshakeResult{
		ReplicaFlushedLSN: 80,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if o != OutcomeCatchUp {
		t.Fatalf("got %s, want catchup", o)
	}
}

func TestOutcome_CatchUp_ExactBoundary(t *testing.T) {
	// ReplicaFlushedLSN+1 == RetentionStartLSN → recoverable (just barely).
	o := ClassifyRecoveryOutcome(HandshakeResult{
		ReplicaFlushedLSN: 49,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if o != OutcomeCatchUp {
		t.Fatalf("got %s, want catchup (exact boundary)", o)
	}
}

func TestOutcome_NeedsRebuild(t *testing.T) {
	o := ClassifyRecoveryOutcome(HandshakeResult{
		ReplicaFlushedLSN: 10,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if o != OutcomeNeedsRebuild {
		t.Fatalf("got %s, want needs_rebuild", o)
	}
}

func TestOutcome_NeedsRebuild_OffByOne(t *testing.T) {
	// ReplicaFlushedLSN+1 < RetentionStartLSN → unrecoverable.
	o := ClassifyRecoveryOutcome(HandshakeResult{
		ReplicaFlushedLSN: 48,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if o != OutcomeNeedsRebuild {
		t.Fatalf("got %s, want needs_rebuild (off-by-one)", o)
	}
}

// --- RecordHandshakeWithOutcome execution ---

func TestExec_HandshakeOutcome_ZeroGap_FastComplete(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	outcome, err := s.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 100,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome != OutcomeZeroGap {
		t.Fatalf("outcome=%s, want zero_gap", outcome)
	}

	// Zero-gap: can complete directly from handshake phase.
	if !s.CompleteSessionByID(sess.ID) {
		t.Fatal("zero-gap fast completion should succeed")
	}
	if s.State != StateInSync {
		t.Fatalf("state=%s, want in_sync", s.State)
	}
}

func TestExec_HandshakeOutcome_CatchUp_NormalPath(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	outcome, err := s.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 80,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s, want catchup", outcome)
	}

	// Must catch up before completing.
	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion should be rejected before catch-up")
	}

	s.BeginCatchUp(sess.ID)
	s.RecordCatchUpProgress(sess.ID, 100)
	if !s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion should succeed after convergence")
	}
}

func TestExec_HandshakeOutcome_NeedsRebuild_InvalidatesSession(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	outcome, err := s.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 10,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome != OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s, want needs_rebuild", outcome)
	}

	// Session invalidated, sender at NeedsRebuild.
	if sess.Active() {
		t.Fatal("session should be invalidated")
	}
	if s.State != StateNeedsRebuild {
		t.Fatalf("state=%s, want needs_rebuild", s.State)
	}
	if s.Session() != nil {
		t.Fatal("session should be nil after NeedsRebuild")
	}
}

// --- Assignment-intent orchestration ---

func TestAssignment_CreatesSessionsForTargets(t *testing.T) {
	sg := NewSenderGroup()

	result := sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
			"r2:9333": {DataAddr: "r2:9333", Version: 1},
		},
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1:9333": SessionCatchUp,
		},
	})

	if len(result.Added) != 2 {
		t.Fatalf("added=%d, want 2", len(result.Added))
	}
	if len(result.SessionsCreated) != 1 || result.SessionsCreated[0] != "r1:9333" {
		t.Fatalf("sessions created=%v", result.SessionsCreated)
	}

	// r1 has session, r2 does not.
	r1 := sg.Sender("r1:9333")
	if r1.Session() == nil {
		t.Fatal("r1 should have a session")
	}
	r2 := sg.Sender("r2:9333")
	if r2.Session() != nil {
		t.Fatal("r2 should not have a session")
	}
}

func TestAssignment_SupersedesExistingSession(t *testing.T) {
	sg := NewSenderGroup()

	// First assignment with catch-up session.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})
	oldSess := sg.Sender("r1:9333").Session()

	// Second assignment with rebuild session — supersedes.
	result := sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionRebuild},
	})
	newSess := sg.Sender("r1:9333").Session()

	if oldSess.Active() {
		t.Fatal("old session should be invalidated")
	}
	if !newSess.Active() {
		t.Fatal("new session should be active")
	}
	if newSess.Kind != SessionRebuild {
		t.Fatalf("new session kind=%s, want rebuild", newSess.Kind)
	}
	if len(result.SessionsSuperseded) != 1 || result.SessionsSuperseded[0] != "r1:9333" {
		t.Fatalf("superseded=%v, want [r1:9333]", result.SessionsSuperseded)
	}
}

func TestAssignment_FailsForUnknownReplica(t *testing.T) {
	sg := NewSenderGroup()

	result := sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r99:9333": SessionCatchUp},
	})

	if len(result.SessionsFailed) != 1 || result.SessionsFailed[0] != "r99:9333" {
		t.Fatalf("sessions failed=%v, want [r99:9333]", result.SessionsFailed)
	}
}

func TestAssignment_StaleEpoch_Rejected(t *testing.T) {
	sg := NewSenderGroup()

	// Epoch 2 assignment.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch: 2,
	})

	// Stale epoch 1 assignment with recovery — must be rejected.
	result := sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	if len(result.SessionsFailed) != 1 || result.SessionsFailed[0] != "r1:9333" {
		t.Fatalf("stale epoch should fail: failed=%v created=%v", result.SessionsFailed, result.SessionsCreated)
	}
	if sg.Sender("r1:9333").Session() != nil {
		t.Fatal("stale intent must not create a session")
	}
}

// --- End-to-end prototype recovery flows ---

func TestE2E_CatchUpRecovery_FullFlow(t *testing.T) {
	sg := NewSenderGroup()

	// Step 1: Assignment creates replicas + recovery intent.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
			"r2:9333": {DataAddr: "r2:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	r1 := sg.Sender("r1:9333")
	sess := r1.Session()

	// Step 2: Execute recovery.
	r1.BeginConnect(sess.ID)

	outcome, _ := r1.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 80,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", outcome)
	}

	r1.BeginCatchUp(sess.ID)
	r1.RecordCatchUpProgress(sess.ID, 90)
	r1.RecordCatchUpProgress(sess.ID, 100) // converged

	// Step 3: Complete.
	if !r1.CompleteSessionByID(sess.ID) {
		t.Fatal("completion should succeed")
	}

	// Step 4: Verify final state.
	if r1.State != StateInSync {
		t.Fatalf("r1 state=%s, want in_sync", r1.State)
	}
	if r1.Session() != nil {
		t.Fatal("session should be nil after completion")
	}

	t.Logf("e2e catch-up: assignment → connect → handshake(catchup) → progress → complete → InSync")
}

func TestE2E_NeedsRebuild_Escalation(t *testing.T) {
	sg := NewSenderGroup()

	// Step 1: Assignment with catch-up intent.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	r1 := sg.Sender("r1:9333")
	sess := r1.Session()

	// Step 2: Connect + handshake → unrecoverable gap.
	r1.BeginConnect(sess.ID)
	outcome, _ := r1.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 10,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if outcome != OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s", outcome)
	}

	// Step 3: Sender is at NeedsRebuild, session dead.
	if r1.State != StateNeedsRebuild {
		t.Fatalf("state=%s", r1.State)
	}

	// Step 4: New assignment with rebuild intent.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionRebuild},
	})

	rebuildSess := r1.Session()
	if rebuildSess == nil || rebuildSess.Kind != SessionRebuild {
		t.Fatal("should have rebuild session")
	}

	// Step 5: Execute rebuild recovery (simulated).
	r1.BeginConnect(rebuildSess.ID)
	r1.RecordHandshake(rebuildSess.ID, 0, 100) // full rebuild range
	r1.BeginCatchUp(rebuildSess.ID)
	r1.RecordCatchUpProgress(rebuildSess.ID, 100)

	if !r1.CompleteSessionByID(rebuildSess.ID) {
		t.Fatal("rebuild completion should succeed")
	}
	if r1.State != StateInSync {
		t.Fatalf("after rebuild: state=%s, want in_sync", r1.State)
	}

	t.Logf("e2e rebuild: catch-up→NeedsRebuild→rebuild assignment→recover→InSync")
}

func TestE2E_ZeroGap_FastPath(t *testing.T) {
	sg := NewSenderGroup()

	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	r1 := sg.Sender("r1:9333")
	sess := r1.Session()

	r1.BeginConnect(sess.ID)
	outcome, _ := r1.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 100,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	if outcome != OutcomeZeroGap {
		t.Fatalf("outcome=%s", outcome)
	}

	// Fast path: complete directly from handshake.
	if !r1.CompleteSessionByID(sess.ID) {
		t.Fatal("zero-gap fast completion should succeed")
	}
	if r1.State != StateInSync {
		t.Fatalf("state=%s, want in_sync", r1.State)
	}

	t.Logf("e2e zero-gap: assignment → connect → handshake(zero_gap) → complete → InSync")
}

func TestE2E_EpochBump_MidRecovery_FullCycle(t *testing.T) {
	sg := NewSenderGroup()

	// Epoch 1: start recovery.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	r1 := sg.Sender("r1:9333")
	sess1 := r1.Session()
	r1.BeginConnect(sess1.ID)

	// Epoch bumps mid-recovery.
	sg.InvalidateEpoch(2)
	// Must also update sender epoch for the new assignment.
	r1.UpdateEpoch(2)

	// Old session dead.
	if sess1.Active() {
		t.Fatal("epoch-1 session should be invalidated")
	}

	// Epoch 2: new assignment, new session.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           2,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	sess2 := r1.Session()
	if sess2 == nil || sess2.Epoch != 2 {
		t.Fatal("should have new session at epoch 2")
	}

	// Complete at epoch 2.
	r1.BeginConnect(sess2.ID)
	r1.RecordHandshakeWithOutcome(sess2.ID, HandshakeResult{
		ReplicaFlushedLSN: 100,
		CommittedLSN:    100,
		RetentionStartLSN: 50,
	})
	r1.CompleteSessionByID(sess2.ID)

	if r1.State != StateInSync {
		t.Fatalf("state=%s", r1.State)
	}
	t.Logf("e2e epoch bump: epoch1 recovery → bump → epoch2 recovery → InSync")
}
