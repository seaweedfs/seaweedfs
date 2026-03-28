package distsim

import "testing"

// ============================================================
// Phase 04a: Session ownership validation in distsim
// ============================================================

// --- Scenario 1: Endpoint change during active catch-up ---

func TestP04a_EndpointChangeDuringCatchup_InvalidatesSession(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// Start catch-up session for r1.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	trigger, sessID, ok := c.TriggerRecoverySession("r1")
	if !ok || trigger != TriggerReassignment {
		t.Fatalf("should trigger reassignment, got %s/%v", trigger, ok)
	}

	// Session is active.
	sess := c.Sessions["r1"]
	if !sess.Active {
		t.Fatal("session should be active")
	}

	// Endpoint changes (replica restarts on new address).
	c.StopNode("r1")
	c.RestartNodeWithNewAddress("r1")

	// Session invalidated by endpoint change.
	if sess.Active {
		t.Fatal("session should be invalidated after endpoint change")
	}
	if sess.Reason != "endpoint_changed" {
		t.Fatalf("invalidation reason: got %q, want endpoint_changed", sess.Reason)
	}

	// Stale completion from old session is rejected.
	if c.CompleteRecoverySession("r1", sessID) {
		t.Fatal("stale session completion should be rejected")
	}
	t.Logf("endpoint change: session %d invalidated, stale completion rejected", sessID)
}

// --- Scenario 2: Epoch bump during active catch-up ---

func TestP04a_EpochBumpDuringCatchup_InvalidatesSession(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	_, sessID, ok := c.TriggerRecoverySession("r1")
	if !ok {
		t.Fatal("trigger should succeed")
	}
	sess := c.Sessions["r1"]

	// Epoch bumps (promotion).
	c.StopNode("p")
	c.Promote("r2")

	// Session invalidated by epoch bump.
	if sess.Active {
		t.Fatal("session should be invalidated after epoch bump")
	}
	if sess.Reason != "epoch_bump_promotion" {
		t.Fatalf("reason: got %q", sess.Reason)
	}

	// Stale completion rejected.
	if c.CompleteRecoverySession("r1", sessID) {
		t.Fatal("stale completion after epoch bump should be rejected")
	}
	t.Logf("epoch bump: session %d invalidated, completion rejected", sessID)
}

// --- Scenario 3: Stale late completion from old session ---

func TestP04a_StaleCompletion_AfterSupersede_Rejected(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// First session.
	_, oldSessID, _ := c.TriggerRecoverySession("r1")
	oldSess := c.Sessions["r1"]

	// Invalidate old session manually (simulate timeout or abort).
	c.InvalidateReplicaSession("r1", "timeout")
	if oldSess.Active {
		t.Fatal("old session should be invalidated")
	}

	// New session triggered.
	c.Nodes["r1"].ReplicaState = NodeStateLagging // reset state to allow retrigger
	_, newSessID, ok := c.TriggerRecoverySession("r1")
	if !ok {
		t.Fatal("second trigger should succeed after invalidation")
	}
	newSess := c.Sessions["r1"]

	// Old session completion attempt — must be rejected by ID mismatch.
	if c.CompleteRecoverySession("r1", oldSessID) {
		t.Fatal("old session completion must be rejected")
	}
	// New session still active.
	if !newSess.Active {
		t.Fatal("new session should still be active")
	}

	// New session completion succeeds.
	if !c.CompleteRecoverySession("r1", newSessID) {
		t.Fatal("new session completion should succeed")
	}
	if c.Nodes["r1"].ReplicaState != NodeStateInSync {
		t.Fatalf("r1 should be InSync after new session completes, got %s", c.Nodes["r1"].ReplicaState)
	}
	t.Logf("stale completion: old=%d rejected, new=%d accepted", oldSessID, newSessID)
}

// --- Scenario 4: Duplicate recovery trigger while session active ---

func TestP04a_DuplicateTrigger_WhileActive_Rejected(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// First trigger succeeds.
	_, _, ok := c.TriggerRecoverySession("r1")
	if !ok {
		t.Fatal("first trigger should succeed")
	}

	// Duplicate trigger while session active — rejected.
	_, _, ok = c.TriggerRecoverySession("r1")
	if ok {
		t.Fatal("duplicate trigger should be rejected while session active")
	}

	// Session count: only one in history.
	sessCount := 0
	for _, s := range c.SessionHistory {
		if s.ReplicaID == "r1" {
			sessCount++
		}
	}
	if sessCount != 1 {
		t.Fatalf("should have exactly 1 session in history, got %d", sessCount)
	}
	t.Logf("duplicate trigger correctly rejected")
}

// --- Scenario 5: Session tracking through full lifecycle ---

func TestP04a_FullLifecycle_SessionTracking(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// Disconnect, write, reconnect.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	for i := uint64(3); i <= 10; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// Trigger session.
	trigger, sessID, ok := c.TriggerRecoverySession("r1")
	if !ok {
		t.Fatal("trigger failed")
	}
	if trigger != TriggerReassignment {
		t.Fatalf("expected reassignment, got %s", trigger)
	}

	// Catch up.
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("catch-up should converge")
	}

	// Complete session.
	if !c.CompleteRecoverySession("r1", sessID) {
		t.Fatal("completion should succeed")
	}

	// Verify final state.
	if c.Nodes["r1"].ReplicaState != NodeStateInSync {
		t.Fatalf("r1 should be InSync, got %s", c.Nodes["r1"].ReplicaState)
	}
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("data incorrect: %v", err)
	}

	// Session in history, not active.
	sess := c.Sessions["r1"]
	if sess.Active {
		t.Fatal("session should not be active after completion")
	}
	if len(c.SessionHistory) != 1 {
		t.Fatalf("expected 1 session in history, got %d", len(c.SessionHistory))
	}
	t.Logf("full lifecycle: trigger=%s session=%d → catch-up → complete → InSync", trigger, sessID)
}
