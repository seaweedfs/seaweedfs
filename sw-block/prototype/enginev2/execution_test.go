package enginev2

import "testing"

// ============================================================
// Phase 04 P1: Session execution and sender-group orchestration
// ============================================================

// --- Execution API: full lifecycle ---

func TestExec_FullRecoveryLifecycle(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	id := sess.ID

	// init → connecting
	if err := s.BeginConnect(id); err != nil {
		t.Fatalf("BeginConnect: %v", err)
	}
	if s.State != StateConnecting {
		t.Fatalf("state=%s, want connecting", s.State)
	}

	// connecting → handshake
	if err := s.RecordHandshake(id, 5, 20); err != nil {
		t.Fatalf("RecordHandshake: %v", err)
	}
	if sess.StartLSN != 5 || sess.TargetLSN != 20 {
		t.Fatalf("range: start=%d target=%d", sess.StartLSN, sess.TargetLSN)
	}

	// handshake → catchup
	if err := s.BeginCatchUp(id); err != nil {
		t.Fatalf("BeginCatchUp: %v", err)
	}
	if s.State != StateCatchingUp {
		t.Fatalf("state=%s, want catching_up", s.State)
	}

	// progress
	if err := s.RecordCatchUpProgress(id, 15); err != nil {
		t.Fatalf("progress to 15: %v", err)
	}
	if err := s.RecordCatchUpProgress(id, 20); err != nil {
		t.Fatalf("progress to 20: %v", err)
	}
	if !sess.Converged() {
		t.Fatal("should be converged at 20/20")
	}

	// complete
	if !s.CompleteSessionByID(id) {
		t.Fatal("completion should succeed")
	}
	if s.State != StateInSync {
		t.Fatalf("state=%s, want in_sync", s.State)
	}
}

// --- Stale sessionID rejection across all execution APIs ---

func TestExec_StaleID_AllAPIsReject(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess1, _ := s.AttachSession(1, SessionCatchUp)
	oldID := sess1.ID

	// Supersede with new session.
	s.UpdateEpoch(2)
	sess2, _ := s.AttachSession(2, SessionCatchUp)
	_ = sess2

	// All APIs must reject oldID.
	if err := s.BeginConnect(oldID); err == nil {
		t.Fatal("BeginConnect should reject stale ID")
	}
	if err := s.RecordHandshake(oldID, 0, 10); err == nil {
		t.Fatal("RecordHandshake should reject stale ID")
	}
	if err := s.BeginCatchUp(oldID); err == nil {
		t.Fatal("BeginCatchUp should reject stale ID")
	}
	if err := s.RecordCatchUpProgress(oldID, 5); err == nil {
		t.Fatal("RecordCatchUpProgress should reject stale ID")
	}
	if s.CompleteSessionByID(oldID) {
		t.Fatal("CompleteSessionByID should reject stale ID")
	}
}

// --- Phase ordering enforcement ---

func TestExec_WrongPhaseOrder_Rejected(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	id := sess.ID

	// Skip connecting → go directly to handshake: rejected.
	if err := s.RecordHandshake(id, 0, 10); err == nil {
		t.Fatal("handshake from init should be rejected")
	}

	// Skip to catch-up from init: rejected.
	if err := s.BeginCatchUp(id); err == nil {
		t.Fatal("catch-up from init should be rejected")
	}

	// Progress from init: rejected (not in catch-up phase).
	if err := s.RecordCatchUpProgress(id, 5); err == nil {
		t.Fatal("progress from init should be rejected")
	}

	// Correct path: init → connecting.
	s.BeginConnect(id)
	// Now try catch-up from connecting: rejected (must handshake first).
	if err := s.BeginCatchUp(id); err == nil {
		t.Fatal("catch-up from connecting should be rejected")
	}
}

// --- Progress regression rejection ---

func TestExec_ProgressRegression_Rejected(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	id := sess.ID

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.BeginCatchUp(id)

	s.RecordCatchUpProgress(id, 50)

	// Regression: 30 < 50.
	if err := s.RecordCatchUpProgress(id, 30); err == nil {
		t.Fatal("progress regression should be rejected")
	}

	// Same value: 50 = 50.
	if err := s.RecordCatchUpProgress(id, 50); err == nil {
		t.Fatal("non-advancing progress should be rejected")
	}

	// Advance: 60 > 50.
	if err := s.RecordCatchUpProgress(id, 60); err != nil {
		t.Fatalf("valid progress should succeed: %v", err)
	}
}

// --- Epoch bump during execution ---

func TestExec_EpochBumpDuringExecution_InvalidatesAuthority(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	id := sess.ID

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.BeginCatchUp(id)
	s.RecordCatchUpProgress(id, 50)

	// Epoch bumps mid-execution.
	s.UpdateEpoch(2)

	// All further execution on old session rejected.
	if err := s.RecordCatchUpProgress(id, 60); err == nil {
		t.Fatal("progress after epoch bump should be rejected")
	}
	if s.CompleteSessionByID(id) {
		t.Fatal("completion after epoch bump should be rejected")
	}

	// Sender is disconnected, ready for new session.
	if s.State != StateDisconnected {
		t.Fatalf("state=%s, want disconnected", s.State)
	}
}

// --- Endpoint change during execution ---

func TestExec_EndpointChangeDuringExecution_InvalidatesAuthority(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", CtrlAddr: "r1:9334", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	id := sess.ID

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 50)
	s.BeginCatchUp(id)

	// Endpoint changes mid-execution.
	s.UpdateEndpoint(Endpoint{DataAddr: "r1:9444", CtrlAddr: "r1:9445", Version: 2})

	// All further execution rejected.
	if err := s.RecordCatchUpProgress(id, 10); err == nil {
		t.Fatal("progress after endpoint change should be rejected")
	}
	if s.CompleteSessionByID(id) {
		t.Fatal("completion after endpoint change should be rejected")
	}
}

// --- Completion authority enforcement ---

func TestExec_CompletionRejected_FromInit(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)

	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion from PhaseInit should be rejected")
	}
}

func TestExec_CompletionRejected_FromConnecting(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion from PhaseConnecting should be rejected")
	}
}

func TestExec_CompletionRejected_FromHandshakeWithGap(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 5, 20) // gap exists: 5 → 20

	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion from PhaseHandshake with gap should be rejected")
	}
}

func TestExec_CompletionAllowed_FromHandshakeZeroGap(t *testing.T) {
	// Fast path: handshake shows replica already at target (zero gap).
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 10, 10) // zero gap: start == target

	if !s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion from handshake with zero gap should be allowed")
	}
	if s.State != StateInSync {
		t.Fatalf("state=%s, want in_sync", s.State)
	}
}

func TestExec_CompletionRejected_FromCatchUpNotConverged(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.BeginCatchUp(sess.ID)
	s.RecordCatchUpProgress(sess.ID, 50) // not converged (50 < 100)

	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion before convergence should be rejected")
	}
}

func TestExec_HandshakeInvalidRange_Rejected(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	if err := s.RecordHandshake(sess.ID, 20, 5); err == nil {
		t.Fatal("handshake with target < start should be rejected")
	}
}

// --- SenderGroup orchestration ---

func TestOrch_RepeatedReconnectCycles_PreserveSenderIdentity(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
	}, 1)

	s := sg.Sender("r1:9333")
	original := s // save pointer

	// 5 reconnect cycles — sender identity preserved.
	for cycle := 0; cycle < 5; cycle++ {
		sess, err := s.AttachSession(1, SessionCatchUp)
		if err != nil {
			t.Fatalf("cycle %d attach: %v", cycle, err)
		}
		s.BeginConnect(sess.ID)
		s.RecordHandshake(sess.ID, 0, 10)
		s.BeginCatchUp(sess.ID)
		s.RecordCatchUpProgress(sess.ID, 10)
		s.CompleteSessionByID(sess.ID)

		if s.State != StateInSync {
			t.Fatalf("cycle %d: state=%s, want in_sync", cycle, s.State)
		}
	}

	// Same pointer — identity preserved.
	if sg.Sender("r1:9333") != original {
		t.Fatal("sender identity should be preserved across cycles")
	}
}

func TestOrch_EndpointUpdateSupersedesActiveSession(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", CtrlAddr: "r1:9334", Version: 1},
	}, 1)

	s := sg.Sender("r1:9333")
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	// Endpoint update via reconcile — session invalidated.
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", CtrlAddr: "r1:9334", Version: 2},
	}, 1)

	if sess.Active() {
		t.Fatal("session should be invalidated by endpoint update")
	}
	// Sender preserved, session gone.
	if sg.Sender("r1:9333") != s {
		t.Fatal("sender identity should be preserved")
	}
	if s.Session() != nil {
		t.Fatal("session should be nil after endpoint invalidation")
	}
}

func TestOrch_ReconcileMixedAddRemoveUpdate(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r2:9333": {DataAddr: "r2:9333", Version: 1},
		"r3:9333": {DataAddr: "r3:9333", Version: 1},
	}, 1)

	r1 := sg.Sender("r1:9333")
	r2 := sg.Sender("r2:9333")

	// Attach sessions to r1 and r2.
	r1Sess, _ := r1.AttachSession(1, SessionCatchUp)
	r2Sess, _ := r2.AttachSession(1, SessionCatchUp)

	// Reconcile: keep r1, remove r2, update r3, add r4.
	added, removed := sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},    // kept
		"r3:9333": {DataAddr: "r3:9333", Version: 2},    // updated
		"r4:9333": {DataAddr: "r4:9333", Version: 1},    // added
	}, 1)

	if len(added) != 1 || added[0] != "r4:9333" {
		t.Fatalf("added=%v", added)
	}
	if len(removed) != 1 || removed[0] != "r2:9333" {
		t.Fatalf("removed=%v", removed)
	}

	// r1: preserved with active session.
	if sg.Sender("r1:9333") != r1 {
		t.Fatal("r1 should be preserved")
	}
	if !r1Sess.Active() {
		t.Fatal("r1 session should still be active")
	}

	// r2: stopped and removed.
	if sg.Sender("r2:9333") != nil {
		t.Fatal("r2 should be removed")
	}
	if r2.Stopped() != true {
		t.Fatal("r2 should be stopped")
	}
	if r2Sess.Active() {
		t.Fatal("r2 session should be invalidated (sender stopped)")
	}

	// r4: new sender, no session.
	if sg.Sender("r4:9333") == nil {
		t.Fatal("r4 should exist")
	}
}

func TestOrch_EpochBumpInvalidatesExecutingSessions(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r2:9333": {DataAddr: "r2:9333", Version: 1},
	}, 1)

	r1 := sg.Sender("r1:9333")
	r2 := sg.Sender("r2:9333")

	sess1, _ := r1.AttachSession(1, SessionCatchUp)
	r1.BeginConnect(sess1.ID)
	r1.RecordHandshake(sess1.ID, 0, 50)
	r1.BeginCatchUp(sess1.ID)
	r1.RecordCatchUpProgress(sess1.ID, 25) // mid-execution

	sess2, _ := r2.AttachSession(1, SessionCatchUp)
	r2.BeginConnect(sess2.ID)

	// Epoch bump.
	count := sg.InvalidateEpoch(2)
	if count != 2 {
		t.Fatalf("should invalidate 2 sessions, got %d", count)
	}

	// Both sessions dead.
	if sess1.Active() || sess2.Active() {
		t.Fatal("both sessions should be invalidated")
	}

	// r1's mid-execution progress cannot continue.
	if err := r1.RecordCatchUpProgress(sess1.ID, 30); err == nil {
		t.Fatal("progress on invalidated session should be rejected")
	}
}
