package enginev2

import "testing"

// === Sender lifecycle ===

func TestSender_NewSender_Disconnected(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", CtrlAddr: "r1:9334", Version: 1}, 1)
	if s.State != StateDisconnected {
		t.Fatalf("new sender should be Disconnected, got %s", s.State)
	}
	if s.Session() != nil {
		t.Fatal("new sender should have no session")
	}
}

func TestSender_AttachSession_Success(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	sess, err := s.AttachSession(1, SessionCatchUp)
	if err != nil {
		t.Fatal(err)
	}
	if sess.Kind != SessionCatchUp {
		t.Fatalf("session kind: got %s, want catchup", sess.Kind)
	}
	if sess.Epoch != 1 {
		t.Fatalf("session epoch: got %d, want 1", sess.Epoch)
	}
	if !sess.Active() {
		t.Fatal("session should be active")
	}
	// AttachSession is ownership-only — sender stays Disconnected until BeginConnect.
	if s.State != StateDisconnected {
		t.Fatalf("sender state after attach: got %s, want disconnected (ownership-only)", s.State)
	}
}

func TestSender_AttachSession_RejectsDouble(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	_, err := s.AttachSession(1, SessionCatchUp)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.AttachSession(1, SessionBootstrap)
	if err == nil {
		t.Fatal("should reject second attach while session active")
	}
}

func TestSender_CompleteSession_TransitionsInSync(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	sess, _ := s.AttachSession(1, SessionCatchUp)
	// Must execute full lifecycle before completing.
	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 5, 10)
	s.BeginCatchUp(sess.ID)
	s.RecordCatchUpProgress(sess.ID, 10) // converged

	if !s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion should succeed when converged")
	}
	if s.State != StateInSync {
		t.Fatalf("after complete: got %s, want in_sync", s.State)
	}
	if s.Session() != nil {
		t.Fatal("session should be nil after complete")
	}
	if sess.Active() {
		t.Fatal("completed session should not be active")
	}
}

func TestSender_SupersedeSession(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	old, _ := s.AttachSession(1, SessionCatchUp)
	s.UpdateEpoch(2) // epoch bumps — old session invalidated by UpdateEpoch
	new := s.SupersedeSession(SessionReassign, "explicit_supersede")

	if old.Active() {
		t.Fatal("old session should be invalidated")
	}
	// Invalidated by UpdateEpoch, not by SupersedeSession (already dead).
	if old.InvalidateReason == "" {
		t.Fatal("old session should have invalidation reason")
	}
	if !new.Active() {
		t.Fatal("new session should be active")
	}
	if new.Epoch != 2 {
		t.Fatalf("new session epoch: got %d, want 2", new.Epoch)
	}
}

func TestSender_UpdateEndpoint_InvalidatesSession(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.UpdateEndpoint(Endpoint{DataAddr: "r1:9444", Version: 2})

	if sess.Active() {
		t.Fatal("session should be invalidated after endpoint change")
	}
	if sess.InvalidateReason != "endpoint_changed" {
		t.Fatalf("invalidation reason: got %q", sess.InvalidateReason)
	}
	if s.State != StateDisconnected {
		t.Fatalf("sender should be disconnected after endpoint change, got %s", s.State)
	}
	if s.Session() != nil {
		t.Fatal("session should be nil after endpoint change")
	}
}

func TestSender_UpdateEndpoint_SameAddr_PreservesSession(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.UpdateEndpoint(Endpoint{DataAddr: "r1:9333", Version: 1})

	if !sess.Active() {
		t.Fatal("same-address update should preserve session")
	}
}

func TestSender_UpdateEndpoint_CtrlAddrOnly_InvalidatesSession(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", CtrlAddr: "r1:9334", Version: 1}, 1)

	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.UpdateEndpoint(Endpoint{DataAddr: "r1:9333", CtrlAddr: "r1:9444", Version: 1})

	if sess.Active() {
		t.Fatal("CtrlAddr-only change should invalidate session")
	}
	if s.State != StateDisconnected {
		t.Fatalf("sender should be disconnected, got %s", s.State)
	}
}

func TestSender_Stop_InvalidatesSession(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.Stop()

	if sess.Active() {
		t.Fatal("session should be invalidated after stop")
	}
	if !s.Stopped() {
		t.Fatal("sender should be stopped")
	}

	// Attach after stop fails.
	_, err := s.AttachSession(1, SessionBootstrap)
	if err == nil {
		t.Fatal("attach after stop should fail")
	}
}

func TestSender_InvalidateSession_TargetState(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.InvalidateSession("timeout", StateNeedsRebuild)

	if sess.Active() {
		t.Fatal("session should be invalidated")
	}
	if s.State != StateNeedsRebuild {
		t.Fatalf("sender state: got %s, want needs_rebuild", s.State)
	}
}

// === Session lifecycle ===

func TestSession_Advance_ValidTransitions(t *testing.T) {
	sess := newRecoverySession("r1", 1, SessionCatchUp)

	if !sess.Advance(PhaseConnecting) {
		t.Fatal("init → connecting should succeed")
	}
	if !sess.Advance(PhaseHandshake) {
		t.Fatal("connecting → handshake should succeed")
	}
	if !sess.Advance(PhaseCatchUp) {
		t.Fatal("handshake → catchup should succeed")
	}
	if !sess.Advance(PhaseCompleted) {
		t.Fatal("catchup → completed should succeed")
	}
}

func TestSession_Advance_RejectsInvalidJump(t *testing.T) {
	sess := newRecoverySession("r1", 1, SessionCatchUp)

	// init → catchup is not valid (must go through connecting, handshake)
	if sess.Advance(PhaseCatchUp) {
		t.Fatal("init → catchup should be rejected")
	}
	// init → completed is not valid
	if sess.Advance(PhaseCompleted) {
		t.Fatal("init → completed should be rejected")
	}
}

func TestSession_Advance_StopsOnInvalidate(t *testing.T) {
	sess := newRecoverySession("r1", 1, SessionCatchUp)
	sess.Advance(PhaseConnecting)
	sess.Advance(PhaseHandshake)
	sess.invalidate("test")

	if sess.Advance(PhaseCatchUp) {
		t.Fatal("advance after invalidate should fail")
	}
}

func TestSender_AttachSession_RejectsEpochMismatch(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	_, err := s.AttachSession(2, SessionCatchUp)
	if err == nil {
		t.Fatal("should reject session at epoch 2 when sender is at epoch 1")
	}
}

func TestSender_UpdateEpoch_InvalidatesStaleSession(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)

	s.UpdateEpoch(2)

	if sess.Active() {
		t.Fatal("session at epoch 1 should be invalidated after UpdateEpoch(2)")
	}
	if s.Epoch != 2 {
		t.Fatalf("sender epoch should be 2, got %d", s.Epoch)
	}
	if s.State != StateDisconnected {
		t.Fatalf("sender should be disconnected after epoch bump, got %s", s.State)
	}

	// Can now attach at epoch 2.
	sess2, err := s.AttachSession(2, SessionCatchUp)
	if err != nil {
		t.Fatalf("attach at new epoch should succeed: %v", err)
	}
	if sess2.Epoch != 2 {
		t.Fatalf("new session epoch: got %d, want 2", sess2.Epoch)
	}
}

func TestSession_Progress_StopsOnComplete(t *testing.T) {
	sess := newRecoverySession("r1", 1, SessionCatchUp)
	sess.SetRange(0, 100)

	sess.UpdateProgress(50)
	if sess.Converged() {
		t.Fatal("should not converge at 50/100")
	}

	sess.complete()

	if sess.UpdateProgress(100) {
		t.Fatal("update after complete should return false")
	}
}

func TestSession_Converged(t *testing.T) {
	sess := newRecoverySession("r1", 1, SessionCatchUp)
	sess.SetRange(0, 10)

	sess.UpdateProgress(9)
	if sess.Converged() {
		t.Fatal("9 < 10: not converged")
	}

	sess.UpdateProgress(10)
	if !sess.Converged() {
		t.Fatal("10 >= 10: should be converged")
	}
}

// === Bridge tests: ownership invariants matching distsim scenarios ===

func TestBridge_StaleCompletion_AfterSupersede_HasNoEffect(t *testing.T) {
	// Matches distsim TestP04a_StaleCompletion_AfterSupersede_Rejected.
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	// First session.
	sess1, _ := s.AttachSession(1, SessionCatchUp)
	sess1.Advance(PhaseConnecting)
	sess1.Advance(PhaseHandshake)
	sess1.Advance(PhaseCatchUp)

	// Supersede with new session.
	s.UpdateEpoch(2)
	sess2, _ := s.AttachSession(2, SessionCatchUp)

	// Old session: advance/complete has no effect (already invalidated).
	if sess1.Advance(PhaseCompleted) {
		t.Fatal("stale session should not advance to completed")
	}
	if sess1.Active() {
		t.Fatal("old session should be inactive")
	}

	// New session: still active and owns the sender.
	if !sess2.Active() {
		t.Fatal("new session should be active")
	}
	if s.Session() != sess2 {
		t.Fatal("sender should own the new session")
	}

	// Stale completion by OLD session ID — REJECTED by identity check.
	if s.CompleteSessionByID(sess1.ID) {
		t.Fatal("stale completion with old session ID must be rejected")
	}
	// Sender must NOT have moved to InSync.
	if s.State == StateInSync {
		t.Fatal("sender must not be InSync after stale completion")
	}
	// New session must still be active.
	if !sess2.Active() {
		t.Fatal("new session must still be active after stale completion rejected")
	}

	// Correct completion by NEW session ID — requires full execution path.
	s.BeginConnect(sess2.ID)
	s.RecordHandshake(sess2.ID, 0, 10)
	s.BeginCatchUp(sess2.ID)
	s.RecordCatchUpProgress(sess2.ID, 10)
	if !s.CompleteSessionByID(sess2.ID) {
		t.Fatal("completion with correct session ID should succeed after convergence")
	}
	if s.State != StateInSync {
		t.Fatalf("sender should be InSync after correct completion, got %s", s.State)
	}
}

func TestBridge_EpochBump_RejectedCompletion(t *testing.T) {
	// Matches distsim TestP04a_EpochBumpDuringCatchup_InvalidatesSession.
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	sess, _ := s.AttachSession(1, SessionCatchUp)
	sess.Advance(PhaseConnecting)

	// Epoch bumps — session invalidated.
	s.UpdateEpoch(2)

	// Attempting to advance the old session fails.
	if sess.Advance(PhaseHandshake) {
		t.Fatal("stale session should not advance after epoch bump")
	}

	// Attempting to attach at old epoch fails.
	_, err := s.AttachSession(1, SessionCatchUp)
	if err == nil {
		t.Fatal("attach at stale epoch should fail")
	}

	// Attach at new epoch succeeds.
	sess2, err := s.AttachSession(2, SessionCatchUp)
	if err != nil {
		t.Fatalf("attach at new epoch should succeed: %v", err)
	}
	if sess2.Epoch != 2 {
		t.Fatalf("new session epoch=%d, want 2", sess2.Epoch)
	}
}

func TestBridge_EndpointChange_InvalidatesAndAllowsNewSession(t *testing.T) {
	// Matches distsim TestP04a_EndpointChangeDuringCatchup_InvalidatesSession.
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)

	sess, _ := s.AttachSession(1, SessionCatchUp)

	// Endpoint changes.
	s.UpdateEndpoint(Endpoint{DataAddr: "r1:9444", Version: 2})

	// Old session dead.
	if sess.Active() {
		t.Fatal("session should be invalidated")
	}

	// New session can be attached (same epoch, new endpoint).
	sess2, err := s.AttachSession(1, SessionCatchUp)
	if err != nil {
		t.Fatalf("new session after endpoint change: %v", err)
	}
	if !sess2.Active() {
		t.Fatal("new session should be active")
	}
}

func TestSession_DoubleInvalidate_Safe(t *testing.T) {
	sess := newRecoverySession("r1", 1, SessionCatchUp)
	sess.invalidate("first")
	sess.invalidate("second") // should not panic or change reason

	if sess.InvalidateReason != "first" {
		t.Fatalf("reason should be first, got %q", sess.InvalidateReason)
	}
}
