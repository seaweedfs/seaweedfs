package distsim

import (
	"testing"
)

// ============================================================
// Phase 03 P2: Timer-ordering races
// ============================================================

// --- Race 1: Concurrent barrier timeouts under sync_quorum ---

func TestP03_P2_ConcurrentBarrierTimeout_QuorumEdge(t *testing.T) {
	// RF=3 (p, r1, r2). sync_quorum (quorum=2).
	// Both r1 and r2 have barrier timeouts. r1's ack arrives in the same tick
	// as r2's timeout fires. The "data before timers" rule means:
	// r1 ack processed → cancels r1 timeout → r2 timeout fires → quorum = p+r1 = 2 → committed.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.BarrierTimeoutTicks = 5

	// r2 disconnected — barrier will time out. r1 connected — will ack.
	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	c.CommitWrite(1) // barrier to r1 at Now+2, barrier to r2 at Now+2
	// Barrier timeout for both at Now+5.

	c.TickN(10)

	// r1 ack arrived → cancelled r1 timeout.
	// r2 barrier timed out (link down, no ack).
	firedBarriers := c.FiredTimeoutsByKind(TimeoutBarrier)
	if firedBarriers != 1 {
		t.Fatalf("expected 1 barrier timeout (r2), got %d", firedBarriers)
	}

	// Event log: r1's barrier timeout was cancelled (ack arrived earlier).
	// r2's barrier timeout fired. Verify both are in the TickLog.
	var cancelCount, fireCount int
	for _, e := range c.TickLog {
		if e.Kind == EventTimeoutCancelled {
			cancelCount++
		}
		if e.Kind == EventTimeoutFired {
			fireCount++
		}
	}
	if cancelCount != 1 {
		t.Fatalf("expected 1 timeout cancel (r1 ack), got %d", cancelCount)
	}
	if fireCount != 1 {
		t.Fatalf("expected 1 timeout fire (r2), got %d", fireCount)
	}

	// Quorum: p + r1 = 2 of 3 → committed.
	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("LSN 1 should commit via quorum (p+r1), committed=%d", c.Coordinator.CommittedLSN)
	}

	// DurableOn: p=true (self-ack), r1=true (ack), r2 NOT set (timed out).
	p1 := c.Pending[1]
	if !p1.DurableOn["p"] || !p1.DurableOn["r1"] {
		t.Fatal("DurableOn should have p and r1")
	}
	if p1.DurableOn["r2"] {
		t.Fatal("DurableOn should NOT have r2 (timed out)")
	}

	t.Logf("concurrent timeout: r1 acked, r2 timed out, quorum met, committed=%d", c.Coordinator.CommittedLSN)
}

func TestP03_P2_ConcurrentBarrierTimeout_BothTimeout_NoQuorum(t *testing.T) {
	// Both r1 and r2 disconnected. Both timeouts fire. Quorum = p alone = 1 < 2.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.BarrierTimeoutTicks = 5

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	c.CommitWrite(1)
	c.TickN(10)

	// Both barriers timed out.
	if c.FiredTimeoutsByKind(TimeoutBarrier) != 2 {
		t.Fatalf("expected 2 barrier timeouts, got %d", c.FiredTimeoutsByKind(TimeoutBarrier))
	}

	// No quorum — uncommitted.
	if c.Coordinator.CommittedLSN != 0 {
		t.Fatalf("LSN 1 should not commit without quorum, committed=%d", c.Coordinator.CommittedLSN)
	}

	// Neither r1 nor r2 in DurableOn.
	p1 := c.Pending[1]
	if p1.DurableOn["r1"] || p1.DurableOn["r2"] {
		t.Fatal("DurableOn should not have r1 or r2")
	}
	t.Logf("both timeouts: no quorum, LSN 1 uncommitted")
}

func TestP03_P2_ConcurrentBarrierTimeout_SameTick_AckAndTimeout(t *testing.T) {
	// The precise same-tick race: r1 ack arrives at exactly the tick when r2's
	// timeout fires. Verify data-before-timers ordering in the event log.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.BarrierTimeoutTicks = 4 // timeout at Now+4

	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	c.CommitWrite(1)
	// Write at Now+1, barrier at Now+2, ack back at Now+3.
	// Timeout for r1 at Now+4, timeout for r2 at Now+4.

	// Tick to barrier ack arrival (tick 3): r1 ack delivered, cancels r1 timeout.
	// Tick 4: r2 timeout fires. r1 timeout already cancelled.
	c.TickN(6)

	// Check event ordering at the timeout tick.
	timeoutTick := uint64(0)
	for _, ft := range c.FiredTimeouts {
		timeoutTick = ft.FiredAt
	}
	events := c.TickEventsAt(timeoutTick)

	// At the timeout tick, we should see: r2 timeout fired (r1 was cancelled earlier).
	var firedDetails []string
	for _, e := range events {
		if e.Kind == EventTimeoutFired {
			firedDetails = append(firedDetails, e.Detail)
		}
	}
	if len(firedDetails) != 1 {
		t.Fatalf("expected 1 timeout fire at tick %d, got %d: %v", timeoutTick, len(firedDetails), firedDetails)
	}

	// Committed via quorum.
	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("committed=%d, want 1", c.Coordinator.CommittedLSN)
	}
	t.Logf("same-tick race: r1 ack cancelled at tick 3, r2 timeout fired at tick %d, committed=1", timeoutTick)
}

// --- Race 2: Epoch bump during active barrier timeout window ---

func TestP03_P2_EpochBumpDuringBarrierTimeout_CrossSurface(t *testing.T) {
	// Three cleanup mechanisms interact for the same barrier:
	// 1. Epoch fencing in deliver() rejects old-epoch messages
	// 2. Barrier timeout in fireTimeouts() removes queued barriers + marks expired
	// 3. ExpiredBarriers in deliver() rejects late acks
	//
	// Scenario: barrier re-queues (r1 missing data), epoch bumps, then timeout fires.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.BarrierTimeoutTicks = 10

	c.CommitWrite(1) // write+barrier to r1, r2

	// Drop write to r1 so barrier keeps re-queuing.
	var kept []inFlightMessage
	for _, item := range c.Queue {
		if item.msg.Kind == MsgWrite && item.msg.To == "r1" && item.msg.Write.LSN == 1 {
			continue
		}
		kept = append(kept, item)
	}
	c.Queue = kept

	// Tick 1-3: r1's barrier delivers but r1 doesn't have data → re-queues.
	// r2 gets write+barrier normally → acks.
	c.TickN(3)

	// Epoch bump: promote r2 (p stays running as demoted replica).
	// This ensures the old-epoch barrier hits epoch fencing, not node_down.
	if err := c.Promote("r2"); err != nil {
		t.Fatal(err)
	}

	// Record state before timeout window.
	epochRejectsBefore := c.RejectedByReason(RejectEpochMismatch)

	// Tick 4-5: old-epoch barrier (p→r1) is in queue. deliver() rejects
	// with epoch_mismatch (msg epoch=1 vs coordinator epoch=2).
	c.TickN(2)

	// Old barrier rejected by epoch fencing.
	epochRejectsAfter := c.RejectedByReason(RejectEpochMismatch)
	newEpochRejects := epochRejectsAfter - epochRejectsBefore
	if newEpochRejects == 0 {
		t.Fatal("old-epoch barrier should be rejected by epoch fencing")
	}

	// Tick past barrier timeout deadline.
	c.TickN(10)

	// Barrier timeout fires for r1/LSN 1 (removes any remaining queued copies).
	if c.FiredTimeoutsByKind(TimeoutBarrier) == 0 {
		t.Fatal("barrier timeout should fire for r1/LSN 1")
	}

	// Expired barrier marked.
	if !c.ExpiredBarriers[barrierExpiredKey{"r1", 1}] {
		t.Fatal("r1/LSN 1 should be in ExpiredBarriers")
	}

	// Inject late ack from r1 for LSN 1 at current epoch (to new primary r2).
	// The barrier is expired — ack should be rejected by barrier_expired.
	deliveriesBefore := len(c.Deliveries)
	c.InjectMessage(Message{
		Kind: MsgBarrierAck, From: "r1", To: "r2",
		Epoch: c.Coordinator.Epoch, TargetLSN: 1,
	}, c.Now+1)
	c.TickN(2)

	// Late ack rejected by barrier_expired.
	lateRejected := false
	for _, d := range c.Deliveries[deliveriesBefore:] {
		if d.Msg.Kind == MsgBarrierAck && d.Msg.From == "r1" && d.Msg.TargetLSN == 1 {
			if !d.Accepted && d.Reason == RejectBarrierExpired {
				lateRejected = true
			}
		}
	}
	if !lateRejected {
		t.Fatal("late ack for expired barrier should be rejected as barrier_expired")
	}

	// Verify event log shows the cross-surface interaction.
	var epochRejectEvents, timeoutFireEvents int
	for _, e := range c.TickLog {
		if e.Kind == EventDeliveryRejected {
			epochRejectEvents++
		}
		if e.Kind == EventTimeoutFired {
			timeoutFireEvents++
		}
	}
	if epochRejectEvents == 0 || timeoutFireEvents == 0 {
		t.Fatalf("event log should show both epoch rejections (%d) and timeout fires (%d)",
			epochRejectEvents, timeoutFireEvents)
	}

	t.Logf("cross-surface: epoch_rejects=%d, timeout_fires=%d, expired_barrier=true, late_ack_rejected=true",
		newEpochRejects, c.FiredTimeoutsByKind(TimeoutBarrier))
}

// --- TickEvents trace verification ---

func TestP03_P2_TickEvents_OrderingVerifiable(t *testing.T) {
	// Verify that TickEvents captures delivery → timeout ordering within a tick.
	c := NewCluster(CommitSyncAll, "p", "r1")
	c.BarrierTimeoutTicks = 5

	c.CommitWrite(1)
	c.TickN(10) // normal flow: ack cancels timeout

	// TickLog should have events.
	if len(c.TickLog) == 0 {
		t.Fatal("TickLog should record events")
	}

	// Find delivery events and timeout events.
	var deliveries, cancels int
	for _, e := range c.TickLog {
		switch e.Kind {
		case EventDeliveryAccepted:
			deliveries++
		case EventTimeoutCancelled:
			cancels++
		}
	}
	if deliveries == 0 {
		t.Fatal("should have delivery events")
	}
	if cancels == 0 {
		t.Fatal("should have timeout cancel events (ack cancelled barrier timeout)")
	}

	// BuildTrace includes TickEvents.
	trace := BuildTrace(c)
	if len(trace.TickEvents) == 0 {
		t.Fatal("BuildTrace should include TickEvents")
	}

	t.Logf("tick events: %d deliveries, %d cancels, %d total events",
		deliveries, cancels, len(c.TickLog))
}
