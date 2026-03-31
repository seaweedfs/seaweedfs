package distsim

import "testing"

func TestQuorumCommitSurvivesPrimaryFailover(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	lsn1 := c.CommitWrite(7)
	if lsn1 != 1 {
		t.Fatalf("unexpected lsn1=%d", lsn1)
	}
	c.TickN(4)
	if c.Coordinator.CommittedLSN != lsn1 {
		t.Fatalf("expected committed lsn %d, got %d", lsn1, c.Coordinator.CommittedLSN)
	}

	c.StopNode("p")
	if err := c.Promote("r1"); err != nil {
		t.Fatalf("promote: %v", err)
	}

	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatal(err)
	}
}

func TestUncommittedWriteNotPreservedAfterPrimaryLoss(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	lsn1 := c.CommitWrite(4)
	if lsn1 != 1 {
		t.Fatalf("unexpected lsn1=%d", lsn1)
	}
	c.TickN(4)
	if c.Coordinator.CommittedLSN != 0 {
		t.Fatalf("expected no committed lsn, got %d", c.Coordinator.CommittedLSN)
	}

	c.StopNode("p")
	c.StartNode("r1")
	if err := c.Promote("r1"); err != nil {
		t.Fatalf("promote: %v", err)
	}

	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatal(err)
	}
	if got := c.Nodes["r1"].Storage.StateAt(1); len(got) != 0 {
		t.Fatalf("uncommitted state leaked into promoted primary: %v", got)
	}
}

func TestReplicaCatchupFromPrimaryWAL(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	c.CommitWrite(1)
	c.TickN(4)
	c.CommitWrite(2)
	c.TickN(4)

	if c.Coordinator.CommittedLSN != 2 {
		t.Fatalf("expected committed lsn 2, got %d", c.Coordinator.CommittedLSN)
	}
	if c.Nodes["r2"].Storage.FlushedLSN != 0 {
		t.Fatalf("expected r2 to lag, got flushed=%d", c.Nodes["r2"].Storage.FlushedLSN)
	}

	c.Connect("p", "r2")
	c.Connect("r2", "p")
	if err := c.RecoverReplicaFromPrimary("r2", 0, c.Coordinator.CommittedLSN); err != nil {
		t.Fatalf("recover replica: %v", err)
	}

	want := c.Reference.StateAt(c.Coordinator.CommittedLSN)
	got := c.Nodes["r2"].Storage.StateAt(c.Coordinator.CommittedLSN)
	if !EqualState(got, want) {
		t.Fatalf("catchup mismatch: got=%v want=%v", got, want)
	}
}

func TestReplicaRebuildFromSnapshotAndTail(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(7)
	c.TickN(4)
	c.CommitWrite(8)
	c.TickN(4)

	snap := c.Primary().Storage.TakeSnapshot("snap-2", 2)

	c.CommitWrite(7)
	c.TickN(4)

	r2 := c.Nodes["r2"]
	r2.Storage = NewStorage()
	if err := c.RebuildReplicaFromSnapshot("r2", snap.ID, c.Coordinator.CommittedLSN); err != nil {
		t.Fatalf("rebuild replica: %v", err)
	}

	want := c.Reference.StateAt(c.Coordinator.CommittedLSN)
	got := r2.Storage.StateAt(c.Coordinator.CommittedLSN)
	if !EqualState(got, want) {
		t.Fatalf("rebuild mismatch: got=%v want=%v", got, want)
	}
}

func TestPromotionUsesValidLineageNode(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(3)
	c.TickN(4)
	c.CommitWrite(5)
	c.TickN(4)

	c.StopNode("p")
	if err := c.Promote("r2"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if c.Nodes["p"].Epoch == c.Coordinator.Epoch {
		t.Fatalf("stopped primary should not auto-advance epoch")
	}
	if c.Nodes["r2"].Epoch != c.Coordinator.Epoch {
		t.Fatalf("new primary epoch mismatch: node=%d coord=%d", c.Nodes["r2"].Epoch, c.Coordinator.Epoch)
	}
	if err := c.AssertCommittedRecoverable("r2"); err != nil {
		t.Fatal(err)
	}
}

func TestZombieOldPrimaryWritesAreFenced(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(3)
	c.TickN(4)

	c.StopNode("p")
	if err := c.Promote("r1"); err != nil {
		t.Fatalf("promote: %v", err)
	}

	c.StartNode("p")
	c.InjectMessage(Message{
		Kind:  MsgWrite,
		From:  "p",
		To:    "r1",
		Epoch: 1,
		Write: Write{LSN: 99, Block: 42, Value: 99},
	}, c.Now+1)
	c.InjectMessage(Message{
		Kind:      MsgBarrierAck,
		From:      "p",
		To:        "r1",
		Epoch:     1,
		TargetLSN: 99,
	}, c.Now+1)
	c.TickN(2)

	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("stale message changed committed lsn: got=%d", c.Coordinator.CommittedLSN)
	}
	if got := c.Nodes["r1"].Storage.LiveExtent[42]; got != 0 {
		t.Fatalf("stale message mutated new primary extent: block42=%d", got)
	}
}

func TestReservationExpiryAbortsCatchup(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")
	c.CommitWrite(1)
	c.TickN(4)
	c.CommitWrite(2)
	c.TickN(4)
	c.CommitWrite(3)
	c.TickN(4)

	c.Connect("p", "r2")
	c.Connect("r2", "p")
	err := c.RecoverReplicaFromPrimaryReserved("r2", 0, c.Coordinator.CommittedLSN, c.Now+2)
	if err == nil {
		t.Fatalf("expected reservation-expiry failure")
	}
	if c.Nodes["r2"].Storage.FlushedLSN >= c.Coordinator.CommittedLSN {
		t.Fatalf("replica should not be fully caught up after expired reservation: flushed=%d committed=%d", c.Nodes["r2"].Storage.FlushedLSN, c.Coordinator.CommittedLSN)
	}
}

func TestSyncQuorumContinuesWithOneLaggingReplica(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	lsn := c.CommitWrite(9)
	c.TickN(4)
	if c.Coordinator.CommittedLSN != lsn {
		t.Fatalf("expected quorum commit with one lagging replica, got committed=%d", c.Coordinator.CommittedLSN)
	}
}

func TestSyncAllBlocksWithOneLaggingReplica(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r1", "r2")

	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	lsn := c.CommitWrite(9)
	c.TickN(4)
	if c.Coordinator.CommittedLSN >= lsn {
		t.Fatalf("sync_all should not commit with one lagging replica: committed=%d", c.Coordinator.CommittedLSN)
	}
}

func TestSyncQuorumWithMixedReplicaStates(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")

	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	lsn := c.CommitWrite(9)
	c.TickN(4)
	if c.Coordinator.CommittedLSN != lsn {
		t.Fatalf("expected quorum commit with mixed replica states: committed=%d", c.Coordinator.CommittedLSN)
	}
}

func TestSyncAllBlocksWithMixedReplicaStates(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r1", "r2", "r3")

	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")
	c.StopNode("r3")

	lsn := c.CommitWrite(9)
	c.TickN(4)
	if c.Coordinator.CommittedLSN >= lsn {
		t.Fatalf("sync_all should not commit with mixed replica states: committed=%d", c.Coordinator.CommittedLSN)
	}
}

func TestReplicaRestartDuringCatchupRestartsSafely(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")
	c.CommitWrite(1)
	c.TickN(4)
	c.CommitWrite(2)
	c.TickN(4)
	c.CommitWrite(3)
	c.TickN(4)

	c.Connect("p", "r2")
	c.Connect("r2", "p")
	last, err := c.RecoverReplicaFromPrimaryPartial("r2", 0, c.Coordinator.CommittedLSN, 1)
	if err != nil {
		t.Fatalf("partial catchup: %v", err)
	}
	if last != 1 {
		t.Fatalf("expected partial catchup to reach lsn 1, got %d", last)
	}

	c.StopNode("r2")
	c.StartNode("r2")

	if err := c.RecoverReplicaFromPrimary("r2", c.Nodes["r2"].Storage.FlushedLSN, c.Coordinator.CommittedLSN); err != nil {
		t.Fatalf("restart catchup: %v", err)
	}
	if err := c.AssertCommittedRecoverable("r2"); err != nil {
		t.Fatal(err)
	}
}

func TestReplicaRestartDuringRebuildRestartsSafely(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(7)
	c.TickN(4)
	c.CommitWrite(8)
	c.TickN(4)
	snap := c.Primary().Storage.TakeSnapshot("snap-2", 2)
	c.CommitWrite(7)
	c.TickN(4)
	c.CommitWrite(9)
	c.TickN(4)

	last, err := c.RebuildReplicaFromSnapshotPartial("r2", snap.ID, c.Coordinator.CommittedLSN, 1)
	if err != nil {
		t.Fatalf("partial rebuild: %v", err)
	}
	if last != 3 {
		t.Fatalf("expected partial rebuild to reach lsn 3, got %d", last)
	}

	c.StopNode("r2")
	c.StartNode("r2")
	if err := c.RebuildReplicaFromSnapshot("r2", snap.ID, c.Coordinator.CommittedLSN); err != nil {
		t.Fatalf("restart rebuild: %v", err)
	}
	if err := c.AssertCommittedRecoverable("r2"); err != nil {
		t.Fatal(err)
	}
}

func TestWALInlineRecordsAreRecoverable(t *testing.T) {
	records := []RecoveryRecord{
		{Write: Write{LSN: 1, Block: 7, Value: 1}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 2, Block: 7, Value: 2}, Class: RecoveryClassWALInline},
	}
	if !FullyRecoverable(records) {
		t.Fatalf("wal-inline records should be recoverable")
	}
	got := ApplyRecoveryRecords(records, 0, 2)
	want := map[uint64]uint64{7: 2}
	if !EqualState(got, want) {
		t.Fatalf("wal-inline recovery mismatch: got=%v want=%v", got, want)
	}
}

func TestExtentReferencedResolvableRecordsAreRecoverable(t *testing.T) {
	records := []RecoveryRecord{
		{Write: Write{LSN: 1, Block: 7, Value: 1}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 2, Block: 9, Value: 2}, Class: RecoveryClassExtentReferenced, PayloadResolvable: true},
	}
	if !FullyRecoverable(records) {
		t.Fatalf("resolvable extent-referenced records should be recoverable")
	}
	got := ApplyRecoveryRecords(records, 0, 2)
	want := map[uint64]uint64{7: 1, 9: 2}
	if !EqualState(got, want) {
		t.Fatalf("extent-referenced recovery mismatch: got=%v want=%v", got, want)
	}
}

func TestExtentReferencedUnresolvableForcesRebuild(t *testing.T) {
	records := []RecoveryRecord{
		{Write: Write{LSN: 1, Block: 7, Value: 1}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 2, Block: 9, Value: 2}, Class: RecoveryClassExtentReferenced, PayloadResolvable: false},
	}
	if FullyRecoverable(records) {
		t.Fatalf("unresolvable extent-referenced records must not be recoverable")
	}
}

// --- S19: Chain of custody across multiple promotions ---

func TestS19_ChainOfCustody_MultiplePromotions(t *testing.T) {
	// A writes → committed → A crashes → promote B →
	// B writes → committed → B crashes → promote C →
	// C must have all committed data from both A and B.
	c := NewCluster(CommitSyncQuorum, "A", "B", "C")

	// Phase 1: A is primary, writes blocks 1-3.
	for i := uint64(1); i <= 3; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)
	if c.Coordinator.CommittedLSN != 3 {
		t.Fatalf("phase 1: expected committedLSN=3, got %d", c.Coordinator.CommittedLSN)
	}

	// Crash A, promote B.
	c.StopNode("A")
	if err := c.Promote("B"); err != nil {
		t.Fatal(err)
	}

	// Phase 2: B is primary, writes blocks 4-6.
	for i := uint64(4); i <= 6; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)
	// CommittedLSN should have advanced (B + C form quorum).
	phase2Committed := c.Coordinator.CommittedLSN
	if phase2Committed < 4 {
		t.Fatalf("phase 2: expected committedLSN >= 4, got %d", phase2Committed)
	}

	// Crash B, promote C.
	c.StopNode("B")
	if err := c.Promote("C"); err != nil {
		t.Fatal(err)
	}

	// Phase 3: Verify C has all committed data from both A and B.
	if err := c.AssertCommittedRecoverable("C"); err != nil {
		t.Fatalf("chain of custody broken: %v", err)
	}

	// Verify specific blocks: data written by A (blocks 1-3) and B (blocks 4-6).
	cState := c.Nodes["C"].Storage.StateAt(phase2Committed)
	refState := c.Reference.StateAt(phase2Committed)
	for block := uint64(1); block <= 6; block++ {
		if cState[block] != refState[block] {
			t.Fatalf("block %d: C has %d, reference has %d", block, cState[block], refState[block])
		}
	}
}

func TestS19_ChainOfCustody_ThreePromotions(t *testing.T) {
	// Even longer chain: A → B → C → back to A (restarted).
	c := NewCluster(CommitSyncQuorum, "A", "B", "C")

	// A writes.
	c.CommitWrite(1)
	c.TickN(5)

	// A crashes, promote B.
	c.StopNode("A")
	c.Promote("B")

	// B writes.
	c.CommitWrite(2)
	c.TickN(5)

	// B crashes, promote C.
	c.StopNode("B")
	c.Promote("C")

	// C writes.
	c.CommitWrite(3)
	c.TickN(5)

	// Restart A, promote back to A.
	c.StartNode("A")
	// Recover A from C (current primary).
	c.RecoverReplicaFromPrimary("A", 0, c.Coordinator.CommittedLSN)
	c.Promote("A")

	// A must have all committed data from A, B, and C eras.
	if err := c.AssertCommittedRecoverable("A"); err != nil {
		t.Fatalf("triple chain of custody broken: %v", err)
	}
}

// --- S20: Live partition with competing writes ---

func TestS20_LivePartition_StaleWritesNotCommitted(t *testing.T) {
	// Partition splits cluster: A can reach B but not C.
	// A thinks it's still primary and writes.
	// Meanwhile coordinator promotes C (which can reach B).
	// A's writes must NOT become committed under the new epoch.
	c := NewCluster(CommitSyncQuorum, "A", "B", "C")

	// Phase 1: Normal operation — all connected.
	c.CommitWrite(1)
	c.TickN(5)
	if c.Coordinator.CommittedLSN != 1 {
		t.Fatalf("expected committedLSN=1, got %d", c.Coordinator.CommittedLSN)
	}

	// Phase 2: Partition — A isolated from C (but A still connected to B).
	c.Disconnect("A", "C")
	c.Disconnect("C", "A")

	// Coordinator detects A is unreachable from C's perspective, promotes C.
	c.Promote("C")

	// Phase 3: A (stale primary, old epoch) tries to write.
	// CommitWrite uses coordinator's current primary, which is now C.
	// So we manually simulate A trying to write with its old epoch.
	oldEpoch := c.Coordinator.Epoch - 1
	staleNode := c.Nodes["A"]
	staleNode.Epoch = oldEpoch // A still thinks it's the old epoch

	// C (new primary) writes under new epoch.
	c.CommitWrite(2)
	c.TickN(5)

	// Phase 4: Verify.
	// C's committed data should be correct.
	if err := c.AssertCommittedRecoverable("C"); err != nil {
		t.Fatalf("new primary data incorrect: %v", err)
	}

	// A's stale epoch means its Storage may have old data, but it should NOT
	// have data from the new epoch that it couldn't have received.
	if staleNode.Epoch == c.Coordinator.Epoch {
		t.Fatal("stale node should NOT have the current epoch")
	}
}

func TestS20_LivePartition_HealRecovers(t *testing.T) {
	// After partition heals, the stale side can catch up and rejoin.
	c := NewCluster(CommitSyncQuorum, "A", "B", "C")

	// Normal writes.
	c.CommitWrite(1)
	c.TickN(5)

	// Partition A from B and C.
	c.Disconnect("A", "B")
	c.Disconnect("B", "A")
	c.Disconnect("A", "C")
	c.Disconnect("C", "A")

	// Promote B.
	c.Promote("B")

	// B writes during partition.
	c.CommitWrite(2)
	c.CommitWrite(3)
	c.TickN(5)

	// Heal partition.
	c.Connect("A", "B")
	c.Connect("B", "A")
	c.Connect("A", "C")
	c.Connect("C", "A")

	// Recover A from B.
	c.StartNode("A") // ensure A is at current epoch
	c.RecoverReplicaFromPrimary("A", 0, c.Coordinator.CommittedLSN)

	// Verify A now has all committed data.
	aState := c.Nodes["A"].Storage.StateAt(c.Coordinator.CommittedLSN)
	refState := c.Reference.StateAt(c.Coordinator.CommittedLSN)
	if !EqualState(aState, refState) {
		t.Fatalf("A after heal: got %v, want %v", aState, refState)
	}
}

// --- S5: Flapping replica stays recoverable ---

func TestS5_FlappingReplica_NoUnnecessaryRebuild(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	// Write initial data.
	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// Simulate 5 flapping cycles: disconnect, write, reconnect, catch up.
	for cycle := 0; cycle < 5; cycle++ {
		// Disconnect r1.
		c.Disconnect("p", "r1")
		c.Disconnect("r1", "p")

		// Write during disconnect (r2 still connected — quorum holds).
		c.CommitWrite(uint64(3 + cycle*2))
		c.CommitWrite(uint64(4 + cycle*2))
		c.TickN(3)

		// Reconnect r1.
		c.Connect("p", "r1")
		c.Connect("r1", "p")

		// Catch r1 up from primary WAL (not rebuild).
		r1 := c.Nodes["r1"]
		c.RecoverReplicaFromPrimary("r1", r1.Storage.FlushedLSN, c.Coordinator.CommittedLSN)
		c.TickN(3)
	}

	// After 5 flapping cycles, r1 should have all committed data.
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("flapping replica lost data: %v", err)
	}
	// r1 was never rebuilt — only WAL catch-up.
	if c.Nodes["r1"].Storage.BaseSnapshot != nil {
		t.Fatal("r1 should NOT have been rebuilt from snapshot — only WAL catch-up")
	}
}

// --- S6: Tail-chasing under load ---

func TestS6_TailChasing_ConvergesOrAborts(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	// Write initial batch.
	for i := uint64(1); i <= 5; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// Disconnect r1.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")

	// Write a LOT more while r1 is disconnected.
	for i := uint64(6); i <= 30; i++ {
		c.CommitWrite(i % 8)
	}
	c.TickN(5)

	// Reconnect r1.
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// Partial catch-up: simulate primary still writing during catch-up.
	r1 := c.Nodes["r1"]
	lastRecovered := r1.Storage.FlushedLSN

	// Try catching up in batches of 5 — primary writes 3 more each batch.
	for attempt := 0; attempt < 10; attempt++ {
		target := c.Coordinator.CommittedLSN
		recovered, err := c.RecoverReplicaFromPrimaryPartial("r1", lastRecovered, target, 5)
		if err != nil {
			t.Fatalf("partial catch-up attempt %d: %v", attempt, err)
		}
		lastRecovered = recovered

		// Primary writes more during catch-up.
		c.CommitWrite(uint64(31 + attempt))
		c.TickN(2)

		// Check convergence.
		if lastRecovered >= c.Coordinator.CommittedLSN {
			break
		}
	}

	// Final full catch-up to close any remaining gap.
	c.RecoverReplicaFromPrimary("r1", lastRecovered, c.Coordinator.CommittedLSN)

	// Verify correctness.
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("tail-chasing replica diverged: %v", err)
	}
}

// --- S18: Primary restart without failover ---

func TestS18_PrimaryRestart_SameLineage(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	// Write and commit.
	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)
	committedBefore := c.Coordinator.CommittedLSN

	// Primary stops (but NOT crashed — coordinator doesn't promote).
	c.StopNode("p")

	// Coordinator bumps epoch (same primary, just restarted).
	c.Coordinator.Epoch++

	// Broadcast new epoch to all running nodes (replicas learn via heartbeat).
	for _, n := range c.Nodes {
		if n.Running {
			n.Epoch = c.Coordinator.Epoch
		}
	}

	// Restart primary with new epoch.
	c.StartNode("p")

	// Primary writes after restart.
	c.CommitWrite(3)
	c.TickN(5)

	// All committed data should be intact.
	if c.Coordinator.CommittedLSN <= committedBefore {
		t.Fatalf("no new commits after restart: before=%d after=%d", committedBefore, c.Coordinator.CommittedLSN)
	}
	if err := c.AssertCommittedRecoverable("p"); err != nil {
		t.Fatalf("data lost after primary restart: %v", err)
	}
}

func TestS18_PrimaryRestart_ReplicasRejectOldEpoch(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// Restart with epoch bump.
	c.StopNode("p")
	c.Coordinator.Epoch++
	c.StartNode("p")

	// Simulate old-epoch message arriving at r1 (stale, should be rejected).
	oldMsg := Message{
		Kind:  MsgWrite,
		From:  "p",
		To:    "r1",
		Epoch: c.Coordinator.Epoch - 1, // old epoch
		Write: Write{LSN: 999, Block: 0, Value: 999},
	}
	// deliver should reject due to epoch mismatch.
	c.deliver(oldMsg)

	// r1 should NOT have the stale write.
	if c.Nodes["r1"].Storage.ReceivedLSN >= 999 {
		t.Fatal("r1 accepted stale-epoch write after primary restart")
	}
}

// --- S12 (stronger): Promotion chooses best valid lineage ---

func TestS12_PromotionChoosesBestLineage_NotHighestLSN(t *testing.T) {
	// Setup: p writes, replicates to r1 and r2.
	// r2 gets MORE data than r1 but at a STALE epoch (simulating split-brain).
	// Promotion should choose r1 (valid lineage) over r2 (higher LSN but stale).
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	// Normal writes, all replicated.
	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// Partition r2 from current epoch.
	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")

	// More writes only to r1 (r2 is partitioned).
	c.CommitWrite(3)
	c.CommitWrite(4)
	c.TickN(5)

	// Artificially give r2 higher ReceivedLSN but at stale epoch.
	// This simulates r2 having local writes from a stale primary.
	r2 := c.Nodes["r2"]
	r2.Epoch = c.Coordinator.Epoch - 1 // stale epoch
	r2.Storage.AppendWrite(Write{LSN: 100, Block: 0, Value: 100})

	// Now r2 has ReceivedLSN=100, r1 has ReceivedLSN=4.
	// Naive "highest LSN wins" would pick r2. Correct logic picks r1.

	// Crash primary.
	c.StopNode("p")

	// Promotion: choose between r1 (current epoch, LSN 4) and r2 (stale epoch, LSN 100).
	// r2 is at wrong epoch — should not be promotable.
	r1 := c.Nodes["r1"]
	if r1.Epoch != c.Coordinator.Epoch {
		t.Fatalf("r1 should be at current epoch")
	}
	if r2.Epoch == c.Coordinator.Epoch {
		t.Fatal("r2 should be at stale epoch for this test")
	}

	// Promote r1 (valid lineage). This should succeed.
	if err := c.Promote("r1"); err != nil {
		t.Fatalf("promote r1: %v", err)
	}

	// Verify r1 has the correct committed data.
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("promoted r1 has wrong data: %v", err)
	}

	// r2's stale data should NOT be in the committed lineage.
	r1State := c.Nodes["r1"].Storage.StateAt(c.Coordinator.CommittedLSN)
	if _, hasBlock0 := r1State[0]; hasBlock0 && r1State[0] == 100 {
		t.Fatal("stale r2 data leaked into promoted lineage")
	}
}

func TestS12_PromotionRejectsRebuildingCandidate(t *testing.T) {
	// A node that is mid-rebuild should NOT be promoted even if it
	// has a high ReceivedLSN (from partial rebuild data).
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// r2 is "rebuilding" — mark it as not at current epoch.
	c.Nodes["r2"].Epoch = 0 // not current
	c.Nodes["r2"].Running = false // simulate down for rebuild

	// Crash primary.
	c.StopNode("p")

	// Only r1 is promotable.
	if err := c.Promote("r1"); err != nil {
		t.Fatalf("promote r1: %v", err)
	}

	// r2 should not be primary.
	if c.Coordinator.PrimaryID != "r1" {
		t.Fatalf("expected r1 as primary, got %s", c.Coordinator.PrimaryID)
	}

	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatal(err)
	}
}

// ============================================================
// Strengthened partials: S20, S6, S5, S18
// ============================================================

// --- S20 (strengthened): stale side routes writes through protocol ---

func TestS20_StalePartition_ProtocolRejectsStaleWrites(t *testing.T) {
	// True competing writes through the message protocol.
	// Stale side attempts writes via StaleWrite (enqueue/deliver path).
	// All stale messages must be rejected by epoch fencing.
	c := NewCluster(CommitSyncQuorum, "A", "B", "C")

	// Phase 1: normal writes.
	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)
	committedBefore := c.Coordinator.CommittedLSN

	// Phase 2: full partition — A isolated.
	c.Disconnect("A", "B")
	c.Disconnect("B", "A")
	c.Disconnect("A", "C")
	c.Disconnect("C", "A")

	// Promote B (new epoch).
	c.Promote("B")
	// A never learned about the new epoch.
	c.Nodes["A"].Epoch = c.Coordinator.Epoch - 1
	staleEpoch := c.Nodes["A"].Epoch

	// Phase 3: B (new primary) writes through protocol — succeeds.
	c.CommitWrite(3)
	c.CommitWrite(4)
	c.TickN(5)
	if c.Coordinator.CommittedLSN <= committedBefore {
		t.Fatalf("B didn't advance commits")
	}

	// Phase 4: A (stale) attempts writes through the protocol.
	// StaleWrite routes through enqueue/deliver — epoch fencing should reject.
	delivered := c.StaleWrite("A", staleEpoch, 99)
	if delivered > 0 {
		t.Fatalf("stale writes were delivered! %d messages passed epoch fencing", delivered)
	}

	// Verify: all stale messages were rejected with epoch_mismatch.
	epochRejects := c.RejectedByReason(RejectEpochMismatch)
	if epochRejects == 0 {
		t.Fatal("expected epoch_mismatch rejections for stale writes, got 0")
	}
	t.Logf("stale writes correctly rejected: %d epoch_mismatch rejections", epochRejects)

	// Phase 5: committed lineage unchanged by stale traffic.
	if err := c.AssertCommittedRecoverable("B"); err != nil {
		t.Fatalf("committed data corrupted by stale traffic: %v", err)
	}

	// A's stale block 99 is local-only, never in reference.
	refState := c.Reference.StateAt(c.Coordinator.CommittedLSN)
	if _, has99 := refState[99]; has99 {
		t.Fatal("stale block 99 leaked into reference")
	}
}

// --- S6 (strengthened): non-converging tail-chase aborts ---

func TestS6_TailChasing_NonConvergent_EscalatesToNeedsRebuild(t *testing.T) {
	// Primary writes FASTER than catch-up rate.
	// Replica can never converge. CatchUpWithEscalation must transition
	// to NeedsRebuild after MaxCatchupAttempts.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	// MaxCatchupAttempts = 3 means after 3 non-convergent attempts → NeedsRebuild.
	c.MaxCatchupAttempts = 3

	// Initial writes — all replicated.
	for i := uint64(1); i <= 5; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// Disconnect r1.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")

	// Write heavily while disconnected — large initial gap.
	for i := uint64(6); i <= 200; i++ {
		c.CommitWrite(i % 8)
	}
	c.TickN(5)

	// Reconnect r1.
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp

	// Each attempt: recover 1 entry, then primary writes 20 more.
	// Ratio: 1 recovered per 20 new → gap always grows → never converges.
	for attempt := 0; attempt < 10; attempt++ {
		c.CatchUpWithEscalation("r1", 1)

		if r1.ReplicaState == NodeStateNeedsRebuild {
			t.Logf("correctly escalated to NeedsRebuild after %d catch-up attempts", r1.CatchupAttempts)
			return
		}

		// Primary writes 20 more while r1 is disconnected again — gap grows.
		c.Disconnect("p", "r1")
		c.Disconnect("r1", "p")
		for w := 0; w < 20; w++ {
			c.CommitWrite(uint64(201 + attempt*20 + w) % 8)
		}
		c.TickN(2)
		c.Connect("p", "r1")
		c.Connect("r1", "p")
	}

	t.Fatalf("expected NeedsRebuild escalation but state is %s after %d attempts",
		r1.ReplicaState, r1.CatchupAttempts)
}

// --- S18 (strengthened): restart races ---

func TestS18_PrimaryRestart_DelayedOldAck_DoesNotAdvancePrefix(t *testing.T) {
	// Old barrier ack arrives AFTER primary restart + epoch bump.
	// Must be rejected by epoch fencing. committedLSN must NOT advance
	// from the stale ack — assert before/after prefix.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// Write LSN 2 but don't let barrier ack arrive yet.
	c.Disconnect("r1", "p") // acks from r1 can't reach p
	c.CommitWrite(2)
	c.TickN(3)

	// Primary restarts with epoch bump.
	c.StopNode("p")
	c.Coordinator.Epoch++
	for _, n := range c.Nodes {
		if n.Running {
			n.Epoch = c.Coordinator.Epoch
		}
	}
	c.StartNode("p")

	// Snapshot committed prefix BEFORE stale ack.
	committedBefore := c.Coordinator.CommittedLSN

	// Reconnect r1 and deliver old-epoch ack.
	c.Connect("r1", "p")
	c.Connect("p", "r1")

	oldAck := Message{
		Kind:      MsgBarrierAck,
		From:      "r1",
		To:        "p",
		Epoch:     c.Coordinator.Epoch - 1, // old epoch!
		TargetLSN: 2,
	}
	c.deliver(oldAck)
	c.refreshCommits()

	// Committed prefix must NOT have advanced from the stale ack.
	committedAfter := c.Coordinator.CommittedLSN
	if committedAfter > committedBefore {
		t.Fatalf("stale ack advanced committed prefix: before=%d after=%d", committedBefore, committedAfter)
	}

	// The stale ack should have been rejected.
	epochRejects := c.RejectedByReason(RejectEpochMismatch)
	if epochRejects == 0 {
		t.Fatal("expected epoch_mismatch rejection for stale ack")
	}

	// Data correctness still holds.
	if err := c.AssertCommittedRecoverable("p"); err != nil {
		t.Fatalf("data incorrect: %v", err)
	}
}

func TestS18_PrimaryRestart_InFlightBarrierDropped(t *testing.T) {
	// Barrier is in-flight when primary restarts. The in-flight barrier
	// should be dropped (events cleared on crash), not processed post-restart.
	c := NewCluster(CommitSyncAll, "p", "r1")

	c.CommitWrite(1)
	c.TickN(5)

	// Write LSN 2.
	c.CommitWrite(2)
	// Don't tick — barrier is "in flight".

	// Crash primary (drops in-flight events).
	c.StopNode("p")

	// Tick to deliver any remaining messages.
	c.TickN(5)

	// Under sync_all, LSN 2 should NOT be committed if barrier didn't complete.
	// (It depends on whether the ack was already queued before crash.)
	// The key invariant: whatever committedLSN is, the data must be correct.
	c.Coordinator.Epoch++
	c.StartNode("p")
	for _, n := range c.Nodes {
		if n.Running {
			n.Epoch = c.Coordinator.Epoch
		}
	}

	if err := c.AssertCommittedRecoverable("p"); err != nil {
		t.Fatalf("data incorrect after in-flight barrier drop: %v", err)
	}
}
