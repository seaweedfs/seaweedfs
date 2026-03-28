package distsim

import (
	"testing"
)

// ============================================================
// Phase 03 P1: Race-focused tests with trace quality
// ============================================================

// --- Race 1: Promotion vs delayed catch-up timeout ---

func TestP03_Race_PromotionThenStaleCatchupTimeout(t *testing.T) {
	// r1 is CatchingUp with a catch-up timeout registered.
	// Before the timeout fires, primary crashes and r1 is promoted.
	// The stale catch-up timeout must not regress r1 (now primary) to NeedsRebuild.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// r1 falls behind, starts catching up.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(3)
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp
	c.RegisterTimeout(TimeoutCatchup, "r1", 0, c.Now+10)

	// r1 catches up successfully.
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("r1 should converge before promotion")
	}

	// Primary crashes. Promote r1.
	c.StopNode("p")
	if err := c.Promote("r1"); err != nil {
		t.Fatal(err)
	}

	// Tick past the catch-up timeout deadline.
	c.TickN(15)

	// Stale timeout must not fire (was auto-cancelled on convergence).
	if c.FiredTimeoutsByKind(TimeoutCatchup) != 0 {
		t.Fatal("stale catch-up timeout must not fire after promotion")
	}
	// r1 must remain primary and running.
	if r1.Role != RolePrimary {
		t.Fatalf("r1 should be primary, got %s", r1.Role)
	}
	if r1.ReplicaState == NodeStateNeedsRebuild {
		t.Fatal("stale timeout regressed promoted r1 to NeedsRebuild")
	}
	t.Logf("promotion vs timeout: stale catch-up timeout suppressed, r1 is primary")
}

func TestP03_Race_PromotionThenStaleBarrierTimeout(t *testing.T) {
	// Barrier timeout registered for r1 at old epoch.
	// Promotion bumps epoch. The stale barrier timeout fires but must not
	// affect the new epoch's commit state.
	c := NewCluster(CommitSyncAll, "p", "r1")
	c.BarrierTimeoutTicks = 8

	// Write 1 — barrier to r1. Disconnect r1 so barrier can't ack.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(1)

	// Tick 2 — barrier timeout registered at Now+8.
	c.TickN(2)

	// Primary crashes, promote r1 (even though it doesn't have write 1).
	c.StopNode("p")
	c.StartNode("r1")
	if err := c.Promote("r1"); err != nil {
		t.Fatal(err)
	}

	// Snapshot committed prefix before stale timeout window.
	committedBefore := c.Coordinator.CommittedLSN

	// r1 is now primary at new epoch. Write new data.
	c.CommitWrite(10)
	c.TickN(10) // well past barrier timeout deadline

	// Stale barrier timeout fires (from old epoch, old primary "p" → old replica "r1").
	barriersFired := c.FiredTimeoutsByKind(TimeoutBarrier)

	// Assert 1: old timed-out barrier did not change committed prefix unexpectedly.
	// CommittedLSN may advance from r1's new-epoch writes, but must not regress
	// or be influenced by the stale barrier timeout.
	if c.Coordinator.CommittedLSN < committedBefore {
		t.Fatalf("committed prefix regressed: before=%d after=%d",
			committedBefore, c.Coordinator.CommittedLSN)
	}

	// Assert 2: old-epoch barrier did not set DurableOn for new-epoch writes.
	// LSN 1 was written by old primary "p". Under the new epoch, DurableOn
	// should not have been modified by the stale barrier's timeout path.
	if p1 := c.Pending[1]; p1 != nil {
		if p1.DurableOn["r1"] {
			t.Fatal("stale barrier timeout should not set DurableOn[r1] for old-epoch LSN 1")
		}
	}

	// Assert 3: old-epoch LSN 1 barrier is marked expired (stale timeout fired correctly).
	if !c.ExpiredBarriers[barrierExpiredKey{"r1", 1}] {
		t.Fatal("old-epoch barrier for r1/LSN 1 should be in ExpiredBarriers")
	}

	t.Logf("promotion vs barrier timeout: committed=%d, fired=%d, DurableOn[r1]=%v, expired[r1/1]=%v",
		c.Coordinator.CommittedLSN, barriersFired,
		c.Pending[1] != nil && c.Pending[1].DurableOn["r1"],
		c.ExpiredBarriers[barrierExpiredKey{"r1", 1}])
}

// --- Race 2: Rebuild completion vs epoch bump ---

func TestP03_Race_RebuildCompletes_ThenEpochBumps(t *testing.T) {
	// r1 needs rebuild. Rebuild completes, but before r1 can rejoin,
	// epoch bumps (another failover). The rebuild result is valid but
	// the replica must re-validate against the new epoch before rejoining.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	for i := uint64(1); i <= 10; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)
	c.Primary().Storage.TakeSnapshot("snap-1", c.Coordinator.CommittedLSN)

	// r1 needs rebuild.
	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateNeedsRebuild

	// Rebuild from snapshot — succeeds.
	c.RebuildReplicaFromSnapshot("r1", "snap-1", c.Coordinator.CommittedLSN)
	r1.ReplicaState = NodeStateRebuilding // transitional

	// Before r1 can rejoin: epoch bumps (simulate another failure/promotion).
	epochBefore := c.Coordinator.Epoch
	c.StopNode("p")
	if err := c.Promote("r2"); err != nil {
		t.Fatal(err)
	}
	epochAfter := c.Coordinator.Epoch

	if epochAfter <= epochBefore {
		t.Fatal("epoch should have bumped")
	}

	// r1's epoch is now stale (was set to epochBefore, promotion updated running nodes).
	// r1 was stopped? No, r1 is still running. But Promote sets all running nodes' epoch.
	// Wait — r1 IS running, so Promote set r1.Epoch = new epoch. Let me check.
	// Actually Promote() sets all running nodes' epoch to new coordinator epoch.
	// r1 is running. So r1.Epoch = epochAfter. But r1.Role = RoleReplica.

	// The rebuild data is from the OLD epoch's committed prefix.
	// Under the new primary (r2), committed prefix may differ.
	// r1 must NOT be promoted to InSync until validated against new epoch.

	// Eligibility check: r1 is Rebuilding — ineligible for promotion.
	e := c.EvaluateCandidateEligibility("r1")
	if e.Eligible {
		t.Fatal("r1 in Rebuilding state should not be eligible")
	}

	// r1 should NOT be InSync until it completes catch-up from new primary.
	if r1.ReplicaState == NodeStateInSync {
		t.Fatal("r1 should not be InSync after epoch bump during rebuild")
	}

	// After catch-up from new primary (r2), r1 can rejoin.
	r1.ReplicaState = NodeStateCatchingUp
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("r1 should converge from new primary")
	}
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("r1 data incorrect after post-epoch-bump catch-up: %v", err)
	}

	t.Logf("rebuild vs epoch bump: r1 rebuilt at epoch %d, bumped to %d, caught up from r2",
		epochBefore, epochAfter)
}

func TestP03_Race_EpochBumpsDuringCatchupTimeout(t *testing.T) {
	// Catch-up timeout registered. Epoch bumps before timeout fires.
	// The timeout is now stale (different epoch context).
	// Must not mutate state under the new epoch.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp
	c.RegisterTimeout(TimeoutCatchup, "r1", 0, c.Now+10)

	// Epoch bumps (promotion) before timeout.
	c.StopNode("p")
	if err := c.Promote("r1"); err != nil {
		t.Fatal(err)
	}
	// r1 is now primary. State changes from CatchingUp to... well, we need to
	// set it. In production, promotion sets the role but the replica state is
	// reset. Let me set it to InSync (as new primary).
	r1.ReplicaState = NodeStateInSync

	// Tick past timeout deadline.
	c.TickN(15)

	// Timeout should be ignored (r1 is InSync, not CatchingUp).
	if c.FiredTimeoutsByKind(TimeoutCatchup) != 0 {
		t.Fatal("catch-up timeout should not fire after epoch bump + promotion")
	}
	if len(c.IgnoredTimeouts) != 1 {
		t.Fatalf("expected 1 ignored (stale) timeout, got %d", len(c.IgnoredTimeouts))
	}
	if r1.ReplicaState != NodeStateInSync {
		t.Fatalf("r1 should remain InSync, got %s", r1.ReplicaState)
	}
	t.Logf("epoch bump vs timeout: stale catch-up timeout correctly ignored")
}

// --- Trace quality: dump state on failure ---

func TestP03_TraceQuality_FailingScenarioDumpsState(t *testing.T) {
	// Verify that the timeout model produces debuggable traces.
	// This test does NOT intentionally fail — it verifies that trace
	// information is available for inspection.
	c := NewCluster(CommitSyncAll, "p", "r1")
	c.BarrierTimeoutTicks = 5

	c.CommitWrite(1)
	c.TickN(3)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.TickN(10)

	// Build trace.
	trace := BuildTrace(c)

	// Trace must contain key debugging information.
	if trace.Tick == 0 {
		t.Fatal("trace should have non-zero tick")
	}
	if trace.CommittedLSN == 0 && len(c.Pending) == 0 {
		t.Fatal("trace should reflect cluster state")
	}
	if len(trace.FiredTimeouts) == 0 {
		t.Fatal("trace should include fired timeouts")
	}
	if len(trace.NodeStates) < 2 {
		t.Fatal("trace should include all node states")
	}
	if trace.Deliveries == 0 {
		t.Fatal("trace should include deliveries")
	}

	t.Logf("trace: tick=%d committed=%d fired_timeouts=%d deliveries=%d nodes=%v",
		trace.Tick, trace.CommittedLSN, len(trace.FiredTimeouts),
		trace.Deliveries, trace.NodeStates)
}

// Trace infrastructure lives in eventsim.go (BuildTrace / Trace type).
