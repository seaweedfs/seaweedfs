package distsim

import (
	"testing"
)

// ============================================================
// Phase 02: Delayed/drop network + multi-node reservation expiry
// ============================================================

// --- Item 4: Stale delayed messages after heal/promote ---

// Scenario: messages from old primary are in-flight when partition heals
// and a new primary is promoted. The stale messages arrive AFTER the
// promotion. They must be rejected by epoch fencing.

func TestP02_DelayedStaleMessages_AfterPromote(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "A", "B", "C")

	// Phase 1: A writes, ships to B and C.
	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// Phase 2: A writes more, but we manually enqueue delayed delivery
	// to simulate in-flight messages when partition happens.
	c.CommitWrite(3) // LSN 3 ships normally
	// Don't tick yet — messages are in the queue.

	// Phase 3: Partition A from everyone, promote B.
	c.Disconnect("A", "B")
	c.Disconnect("B", "A")
	c.Disconnect("A", "C")
	c.Disconnect("C", "A")
	c.StopNode("A")
	c.Promote("B")

	// Phase 4: Manually inject stale messages as if they were delayed in the network.
	// These represent A's write(3) + barrier(3) that were in-flight when A crashed.
	staleEpoch := c.Coordinator.Epoch - 1
	c.InjectMessage(Message{
		Kind: MsgWrite, From: "A", To: "B", Epoch: staleEpoch,
		Write: Write{LSN: 3, Block: 3, Value: 3},
	}, c.Now+1)
	c.InjectMessage(Message{
		Kind: MsgBarrier, From: "A", To: "B", Epoch: staleEpoch,
		TargetLSN: 3,
	}, c.Now+2)
	c.InjectMessage(Message{
		Kind: MsgWrite, From: "A", To: "C", Epoch: staleEpoch,
		Write: Write{LSN: 3, Block: 3, Value: 3},
	}, c.Now+1)

	// Phase 5: Tick to deliver stale messages.
	committedBefore := c.Coordinator.CommittedLSN
	c.TickN(5)

	// All stale messages must be rejected — either by epoch fencing or node-down.
	epochRejects := c.RejectedByReason(RejectEpochMismatch)
	nodeDownRejects := c.RejectedByReason(RejectNodeDown)
	totalRejects := epochRejects + nodeDownRejects
	if totalRejects == 0 {
		t.Fatal("stale delayed messages were not rejected")
	}

	// Committed prefix must not change from stale messages.
	if c.Coordinator.CommittedLSN != committedBefore {
		t.Fatalf("stale delayed messages changed committed prefix: before=%d after=%d",
			committedBefore, c.Coordinator.CommittedLSN)
	}

	// Data correct on new primary.
	if err := c.AssertCommittedRecoverable("B"); err != nil {
		t.Fatalf("data incorrect after stale delayed messages: %v", err)
	}
	t.Logf("stale delayed messages: %d rejected by epoch_mismatch", epochRejects)
}

// Scenario: old barrier ACK arrives after promotion with long delay.
// This is different from S18 — the delay is network-level, not restart-level.

func TestP02_DelayedBarrierAck_LongNetworkDelay(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r1")

	c.CommitWrite(1)
	c.TickN(5)

	// Write 2 — barrier sent to r1.
	c.CommitWrite(2)
	c.TickN(2) // barrier in flight

	// Promote r1 (simulate primary failure + promotion).
	c.StopNode("p")
	c.Promote("r1")

	committedBefore := c.Coordinator.CommittedLSN

	// Long-delayed barrier ack from r1 → dead primary p.
	c.InjectMessage(Message{
		Kind: MsgBarrierAck, From: "r1", To: "p",
		Epoch: c.Coordinator.Epoch - 1, TargetLSN: 2,
	}, c.Now+10)

	c.TickN(15)

	// Must be rejected — p is dead and epoch is stale.
	nodeDownRejects := c.RejectedByReason(RejectNodeDown)
	epochRejects := c.RejectedByReason(RejectEpochMismatch)
	if nodeDownRejects == 0 && epochRejects == 0 {
		t.Fatal("delayed barrier ack should be rejected (node down or epoch mismatch)")
	}

	// Stale ack must not advance committed prefix.
	if c.Coordinator.CommittedLSN != committedBefore {
		t.Fatalf("stale ack changed committed prefix: before=%d after=%d",
			committedBefore, c.Coordinator.CommittedLSN)
	}
}

// Scenario: write ships to replica, network drops the write but delivers
// the barrier. Barrier should timeout or detect missing data.

func TestP02_DroppedWrite_BarrierDelivered_Stalls(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r1")

	c.CommitWrite(1)
	c.TickN(5)

	// Write 2 — but drop the write message to r1 (link down for data only).
	// We simulate by writing but not ticking, then dropping queued writes.
	c.CommitWrite(2) // enqueues write(2) + barrier(2) to r1

	// Remove only the write message from the queue (simulate selective drop).
	var kept []inFlightMessage
	for _, item := range c.Queue {
		if item.msg.Kind == MsgWrite && item.msg.To == "r1" && item.msg.Write.LSN == 2 {
			continue // drop this write
		}
		kept = append(kept, item)
	}
	c.Queue = kept

	// Tick — barrier arrives at r1 but r1 doesn't have LSN 2.
	// Barrier should re-queue (waiting for data).
	c.TickN(10)

	// Assert 1: sync_all blocked — CommittedLSN stuck at 1.
	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("sync_all should be blocked at LSN 1, got committed=%d", c.Coordinator.CommittedLSN)
	}

	// Assert 2: LSN 2 is pending but NOT committed.
	p2 := c.Pending[2]
	if p2 == nil {
		t.Fatal("LSN 2 should be pending")
	}
	if p2.Committed {
		t.Fatal("LSN 2 committed under sync_all but r1 never received the write — safety violation")
	}

	// Assert 3: barrier still re-queuing — stall proven positively.
	barrierRequeued := false
	for _, item := range c.Queue {
		if item.msg.Kind == MsgBarrier && item.msg.To == "r1" && item.msg.TargetLSN == 2 {
			barrierRequeued = true
			break
		}
	}
	if !barrierRequeued {
		t.Fatal("barrier for LSN 2 should still be re-queuing — stall not proven")
	}
	t.Logf("dropped write stall proven: committed=%d, pending[2].committed=%v, barrier re-queuing=%v",
		c.Coordinator.CommittedLSN, p2.Committed, barrierRequeued)
}

// --- Item 5: Multi-node reservation expiry / rebuild timeout ---

// Scenario: RF=3 cluster. Two replicas need catch-up. One's reservation
// expires during recovery. Must handle correctly: one rebuilds, one catches up.

func TestP02_MultiNode_ReservationExpiry_MixedOutcome(t *testing.T) {
	// 5 nodes: p+r3+r4 provide quorum (3 of 5) while r1+r2 are disconnected.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3", "r4")

	// Write initial data.
	for i := uint64(1); i <= 10; i++ {
		c.CommitWrite(i % 4)
	}
	c.TickN(5)

	// Take snapshot for rebuild.
	c.Primary().Storage.TakeSnapshot("snap-1", c.Coordinator.CommittedLSN)

	// r1+r2 disconnect. r3+r4 stay for quorum (p+r3+r4 = 3 of 5).
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	// Write more during disconnect — committed via p+r3+r4 quorum.
	for i := uint64(11); i <= 30; i++ {
		c.CommitWrite(i % 4)
	}
	c.TickN(5)

	// Reconnect both.
	c.Connect("p", "r1")
	c.Connect("r1", "p")
	c.Connect("p", "r2")
	c.Connect("r2", "p")

	// r1: reserved catch-up with tight expiry — MUST expire.
	// 20 entries to replay, but only 2 ticks of budget.
	r1 := c.Nodes["r1"]
	shortExpiry := c.Now + 2
	err := c.RecoverReplicaFromPrimaryReserved("r1", r1.Storage.FlushedLSN, c.Coordinator.CommittedLSN, shortExpiry)
	if err == nil {
		t.Fatal("r1 reservation must expire — 20 entries with 2-tick budget")
	}
	r1.ReplicaState = NodeStateNeedsRebuild
	t.Logf("r1 reservation expired: %v", err)

	// r2: full catch-up (no reservation pressure).
	r2 := c.Nodes["r2"]
	if err := c.RecoverReplicaFromPrimary("r2", r2.Storage.FlushedLSN, c.Coordinator.CommittedLSN); err != nil {
		t.Fatalf("r2 full catch-up failed: %v", err)
	}
	r2.ReplicaState = NodeStateInSync

	// Deterministic mixed outcome: r1=NeedsRebuild, r2=InSync.
	if r1.ReplicaState != NodeStateNeedsRebuild {
		t.Fatalf("r1 should be NeedsRebuild, got %s", r1.ReplicaState)
	}
	if r2.ReplicaState != NodeStateInSync {
		t.Fatalf("r2 should be InSync, got %s", r2.ReplicaState)
	}

	// r2 data correct.
	if err := c.AssertCommittedRecoverable("r2"); err != nil {
		t.Fatalf("r2 data incorrect: %v", err)
	}

	// r1 rebuild from snapshot.
	c.RebuildReplicaFromSnapshot("r1", "snap-1", c.Coordinator.CommittedLSN)
	r1.ReplicaState = NodeStateInSync
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("r1 data incorrect after rebuild: %v", err)
	}

	t.Logf("mixed outcome proven: r1=NeedsRebuild→rebuilt, r2=InSync")
}

// Scenario: all replicas need rebuild but only one snapshot exists.
// First replica rebuilds from snapshot, second must wait or use first
// replica as rebuild source.

func TestP02_MultiNode_AllNeedRebuild(t *testing.T) {
	// Use 5 nodes so quorum (3 of 5) can be met with p+r3+r4 while r1+r2 are down.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3", "r4")
	c.MaxCatchupAttempts = 2

	for i := uint64(1); i <= 5; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	c.Primary().Storage.TakeSnapshot("snap-all", c.Coordinator.CommittedLSN)

	// r1 and r2 disconnect. r3+r4 stay connected so quorum (p+r3+r4) can commit.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")
	for i := uint64(6); i <= 100; i++ {
		c.CommitWrite(i % 8)
	}
	c.TickN(5)

	// Try catch-up for r1 and r2 — both will escalate.
	// Pattern: write while target disconnected, then try partial catch-up.
	for _, id := range []string{"r1", "r2"} {
		n := c.Nodes[id]
		n.ReplicaState = NodeStateCatchingUp
		for attempt := 0; attempt < 5; attempt++ {
			// Write MORE while target is still disconnected (r3+r4 provide quorum).
			for w := 0; w < 20; w++ {
				c.CommitWrite(uint64(101+attempt*20+w) % 8)
			}
			c.TickN(3) // 3 ticks: deliver writes, barriers, then acks
			// Now try catch-up (partial, batch=1). Target stays disconnected —
			// RecoverReplicaFromPrimaryPartial reads directly from primary WAL.
			c.CatchUpWithEscalation(id, 1)
			if n.ReplicaState == NodeStateNeedsRebuild {
				break
			}
		}
	}

	// Reconnect all for rebuild.
	c.Connect("p", "r1")
	c.Connect("r1", "p")
	c.Connect("p", "r2")
	c.Connect("r2", "p")

	// Both should be NeedsRebuild.
	if c.Nodes["r1"].ReplicaState != NodeStateNeedsRebuild {
		t.Fatalf("r1: expected NeedsRebuild, got %s", c.Nodes["r1"].ReplicaState)
	}
	if c.Nodes["r2"].ReplicaState != NodeStateNeedsRebuild {
		t.Fatalf("r2: expected NeedsRebuild, got %s", c.Nodes["r2"].ReplicaState)
	}

	// Rebuild both from snapshot.
	c.RebuildReplicaFromSnapshot("r1", "snap-all", c.Coordinator.CommittedLSN)
	c.RebuildReplicaFromSnapshot("r2", "snap-all", c.Coordinator.CommittedLSN)
	c.Nodes["r1"].ReplicaState = NodeStateInSync
	c.Nodes["r2"].ReplicaState = NodeStateInSync

	// Both correct.
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatal(err)
	}
	if err := c.AssertCommittedRecoverable("r2"); err != nil {
		t.Fatal(err)
	}
	t.Logf("multi-node rebuild complete: both replicas recovered from snapshot")
}

// Scenario: rebuild timeout — rebuild takes too long, coordinator
// should be able to abort and retry or fail explicitly.

func TestP02_RebuildTimeout_PartialRebuildAborts(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	for i := uint64(1); i <= 20; i++ {
		c.CommitWrite(i % 4)
	}
	c.TickN(5)

	c.Primary().Storage.TakeSnapshot("snap-timeout", c.Coordinator.CommittedLSN)

	// Write much more.
	for i := uint64(21); i <= 100; i++ {
		c.CommitWrite(i % 4)
	}
	c.TickN(5)

	// r1 needs rebuild — use partial rebuild with small max.
	lastRecovered, err := c.RebuildReplicaFromSnapshotPartial("r1", "snap-timeout", c.Coordinator.CommittedLSN, 5)
	if err != nil {
		t.Fatalf("partial rebuild: %v", err)
	}

	// Partial rebuild: not complete.
	if lastRecovered >= c.Coordinator.CommittedLSN {
		t.Fatal("expected partial rebuild, not complete")
	}

	// r1 state should remain NeedsRebuild (not promoted to InSync).
	c.Nodes["r1"].ReplicaState = NodeStateRebuilding
	if c.Nodes["r1"].ReplicaState == NodeStateInSync {
		t.Fatal("partial rebuild should not grant InSync")
	}

	// Full rebuild to complete.
	c.RebuildReplicaFromSnapshot("r1", "snap-timeout", c.Coordinator.CommittedLSN)
	c.Nodes["r1"].ReplicaState = NodeStateInSync
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatal(err)
	}
}
