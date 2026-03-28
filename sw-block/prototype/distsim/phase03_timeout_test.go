package distsim

import (
	"testing"
)

// ============================================================
// Phase 03 P0: Timeout-backed scenarios
// ============================================================

// --- Barrier timeout ---

func TestP03_BarrierTimeout_SyncAllBlocked(t *testing.T) {
	// Barrier sent to replica, link goes down, ack never arrives.
	// Barrier timeout fires → barrier removed from queue.
	// sync_all: write stays uncommitted.
	c := NewCluster(CommitSyncAll, "p", "r1")
	c.BarrierTimeoutTicks = 5

	c.CommitWrite(1)
	c.TickN(10) // enough for barrier timeout to fire and normal commit

	// LSN 1: p self-acks. r1 acks. sync_all: both must ack. Should commit.
	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("LSN 1 should commit normally, got committed=%d", c.Coordinator.CommittedLSN)
	}
	// No timeouts fired for LSN 1 (ack arrived in time).
	if c.FiredTimeoutsByKind(TimeoutBarrier) != 0 {
		t.Fatal("no barrier timeouts should have fired for LSN 1")
	}

	// Now disconnect r1. Write LSN 2. Barrier can't be acked.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.TickN(10) // barrier timeout fires after 5 ticks

	// Barrier timeout should have fired for r1/LSN 2.
	if c.FiredTimeoutsByKind(TimeoutBarrier) != 1 {
		t.Fatalf("expected 1 barrier timeout, got %d", c.FiredTimeoutsByKind(TimeoutBarrier))
	}

	// sync_all: LSN 2 NOT committed (r1 never acked).
	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("LSN 2 should not commit under sync_all without r1 ack, committed=%d",
			c.Coordinator.CommittedLSN)
	}

	// Barrier removed from queue (no indefinite re-queuing).
	for _, item := range c.Queue {
		if item.msg.Kind == MsgBarrier && item.msg.To == "r1" && item.msg.TargetLSN == 2 {
			t.Fatal("timed-out barrier should be removed from queue")
		}
	}
	t.Logf("barrier timeout: LSN 2 uncommitted, barrier cleaned from queue")
}

func TestP03_BarrierTimeout_SyncQuorum_StillCommits(t *testing.T) {
	// RF=3 sync_quorum: r1 times out, but r2 acks → quorum met → commits.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.BarrierTimeoutTicks = 5

	// Disconnect r1 only. r2 stays connected.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")

	c.CommitWrite(1)
	c.TickN(10)

	// r1 barrier times out, but r2 acked. quorum = p + r2 = 2 of 3.
	if c.FiredTimeoutsByKind(TimeoutBarrier) != 1 {
		t.Fatalf("expected 1 barrier timeout (r1), got %d", c.FiredTimeoutsByKind(TimeoutBarrier))
	}
	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("LSN 1 should commit via quorum (p+r2), committed=%d", c.Coordinator.CommittedLSN)
	}
	t.Logf("barrier timeout: r1 timed out, LSN 1 committed via quorum")
}

// --- Catch-up timeout ---

func TestP03_CatchupTimeout_EscalatesToNeedsRebuild(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// r1 disconnects, primary writes more.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	for i := uint64(2); i <= 20; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// Register catch-up timeout: 3 ticks from now.
	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp
	c.RegisterTimeout(TimeoutCatchup, "r1", 0, c.Now+3)

	// Tick 3 times — timeout fires before catch-up completes.
	c.TickN(3)

	if r1.ReplicaState != NodeStateNeedsRebuild {
		t.Fatalf("catch-up timeout should escalate to NeedsRebuild, got %s", r1.ReplicaState)
	}
	if c.FiredTimeoutsByKind(TimeoutCatchup) != 1 {
		t.Fatalf("expected 1 catchup timeout, got %d", c.FiredTimeoutsByKind(TimeoutCatchup))
	}
	t.Logf("catch-up timeout: escalated to NeedsRebuild after 3 ticks")
}

// --- Reservation expiry as timeout event ---

func TestP03_ReservationTimeout_AbortsCatchup(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	for i := uint64(1); i <= 10; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// r1 disconnects, more writes.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	for i := uint64(11); i <= 30; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// Register reservation timeout: 2 ticks.
	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp
	c.RegisterTimeout(TimeoutReservation, "r1", 0, c.Now+2)

	c.TickN(2)

	if r1.ReplicaState != NodeStateNeedsRebuild {
		t.Fatalf("reservation timeout should escalate to NeedsRebuild, got %s", r1.ReplicaState)
	}
	if c.FiredTimeoutsByKind(TimeoutReservation) != 1 {
		t.Fatalf("expected 1 reservation timeout, got %d", c.FiredTimeoutsByKind(TimeoutReservation))
	}
}

// --- Timer-race scenarios: same-tick resolution ---

func TestP03_Race_AckArrivesBeforeTimeout_Cancels(t *testing.T) {
	// Barrier ack arrives in the same tick as the timeout deadline.
	// Rule: data events (ack) process before timeouts → timeout is cancelled.
	c := NewCluster(CommitSyncAll, "p", "r1")
	c.BarrierTimeoutTicks = 4 // timeout at Now+4

	c.CommitWrite(1) // barrier enqueued at Now+2, ack back at Now+3
	// Barrier timeout registered at Now+4.

	// Tick 1: write delivered.
	// Tick 2: barrier delivered, ack enqueued at Now+1 = tick 3.
	// Tick 3: ack delivered → cancels timeout.
	// Tick 4: timeout deadline reached — but already cancelled.
	c.TickN(5)

	// Ack arrived first → timeout cancelled → LSN 1 committed.
	if c.FiredTimeoutsByKind(TimeoutBarrier) != 0 {
		t.Fatal("barrier timeout should be cancelled by ack arriving first")
	}
	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("LSN 1 should commit (ack arrived before timeout), committed=%d",
			c.Coordinator.CommittedLSN)
	}
	t.Logf("race resolved: ack cancelled timeout, LSN 1 committed")
}

func TestP03_Race_TimeoutBeforeAck_Fires(t *testing.T) {
	// Timeout fires before barrier can deliver (timeout < barrier delivery time).
	// CommitWrite enqueues barrier at Now+2. Timeout at Now+1 fires first.
	c := NewCluster(CommitSyncAll, "p", "r1")
	c.BarrierTimeoutTicks = 1 // timeout at Now+1 — before barrier delivers at Now+2

	c.CommitWrite(1)
	c.TickN(5)

	// Timeout fires at tick 1. Barrier would deliver at tick 2, but timeout
	// removes it from queue first.
	if c.FiredTimeoutsByKind(TimeoutBarrier) != 1 {
		t.Fatalf("expected barrier timeout to fire, got %d", c.FiredTimeoutsByKind(TimeoutBarrier))
	}
	// sync_all: uncommitted (r1 never acked).
	if c.Coordinator.CommittedLSN != 0 {
		t.Fatalf("LSN 1 should not commit (timeout before barrier delivery), committed=%d",
			c.Coordinator.CommittedLSN)
	}
	t.Logf("race resolved: timeout fired before barrier delivery, LSN 1 uncommitted")
}

func TestP03_Race_CatchupConverges_CancelsTimeout(t *testing.T) {
	// Catch-up completes before the timeout fires.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.CommitWrite(3)
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// Register catch-up timeout: 10 ticks (generous).
	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp
	c.RegisterTimeout(TimeoutCatchup, "r1", 0, c.Now+10)

	// Catch-up completes immediately (small gap).
	// CatchUpWithEscalation auto-cancels recovery timeouts on convergence.
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("catch-up should converge for small gap")
	}

	// Tick past deadline — timeout should already be cancelled.
	c.TickN(15)

	// Timeout should NOT have fired (was cancelled).
	if c.FiredTimeoutsByKind(TimeoutCatchup) != 0 {
		t.Fatal("catch-up timeout should be cancelled on convergence")
	}
	if r1.ReplicaState != NodeStateInSync {
		t.Fatalf("r1 should be InSync after convergence, got %s", r1.ReplicaState)
	}
	t.Logf("race resolved: catch-up converged, timeout auto-cancelled")
}

// --- Stale timeout hardening ---

func TestP03_StaleReservationTimeout_AfterRecoverySuccess(t *testing.T) {
	// Reservation timeout registered, but recovery completes before deadline.
	// The stale timeout must NOT regress state from InSync back to NeedsRebuild.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(3)
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// Register reservation timeout: 10 ticks.
	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp
	c.RegisterTimeout(TimeoutReservation, "r1", 0, c.Now+10)

	// Catch-up succeeds immediately — auto-cancels reservation timeout.
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("catch-up should converge")
	}
	if r1.ReplicaState != NodeStateInSync {
		t.Fatalf("expected InSync after convergence, got %s", r1.ReplicaState)
	}

	// Tick well past the deadline.
	c.TickN(20)

	// Stale reservation timeout must NOT fire (cancelled by convergence).
	if c.FiredTimeoutsByKind(TimeoutReservation) != 0 {
		t.Fatal("stale reservation timeout should not fire after recovery success")
	}
	if r1.ReplicaState != NodeStateInSync {
		t.Fatalf("stale timeout regressed state: expected InSync, got %s", r1.ReplicaState)
	}
	t.Logf("stale reservation timeout correctly suppressed after recovery")
}

func TestP03_LateBarrierAck_AfterTimeout_Rejected(t *testing.T) {
	// Barrier times out, then a late ack arrives. The late ack must be
	// rejected — it must not count toward DurableOn.
	c := NewCluster(CommitSyncAll, "p", "r1")
	c.BarrierTimeoutTicks = 1 // timeout at Now+1

	c.CommitWrite(1)

	// Tick 1: write delivered, timeout fires (barrier at Now+2 not yet delivered).
	c.TickN(1)

	if c.FiredTimeoutsByKind(TimeoutBarrier) != 1 {
		t.Fatalf("expected barrier timeout to fire, got %d", c.FiredTimeoutsByKind(TimeoutBarrier))
	}

	// LSN 1 should NOT be committed.
	if c.Coordinator.CommittedLSN != 0 {
		t.Fatalf("LSN 1 should not be committed after timeout, got %d", c.Coordinator.CommittedLSN)
	}

	// Now inject a late barrier ack (as if the network delayed it massively).
	c.InjectMessage(Message{
		Kind:      MsgBarrierAck,
		From:      "r1",
		To:        "p",
		Epoch:     c.Coordinator.Epoch,
		TargetLSN: 1,
	}, c.Now+1)

	c.TickN(5)

	// Late ack must be rejected with barrier_expired reason.
	expiredRejects := c.RejectedByReason(RejectBarrierExpired)
	if expiredRejects == 0 {
		t.Fatal("late barrier ack should be rejected as barrier_expired")
	}

	// LSN 1 must still be uncommitted (late ack did not count).
	if c.Coordinator.CommittedLSN != 0 {
		t.Fatalf("late ack should not commit LSN 1, got committed=%d", c.Coordinator.CommittedLSN)
	}

	// DurableOn should NOT include r1.
	p1 := c.Pending[1]
	if p1 != nil && p1.DurableOn["r1"] {
		t.Fatal("late ack should not set DurableOn for r1")
	}
	t.Logf("late barrier ack: rejected as barrier_expired, LSN 1 stays uncommitted")
}
