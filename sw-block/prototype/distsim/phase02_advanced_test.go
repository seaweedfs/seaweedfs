package distsim

import (
	"testing"
)

// ============================================================
// Phase 02: Item 4 — Smart WAL recovery-class transitions
// ============================================================

// Test: recovery starts with resolvable ExtentReferenced records,
// then a payload becomes unresolvable during active recovery.
// Protocol must detect the transition and abort to NeedsRebuild.

func TestP02_SmartWAL_RecoverableThenUnrecoverable(t *testing.T) {
	// Build recovery records: first 3 WALInline, then 2 ExtentReferenced.
	records := []RecoveryRecord{
		{Write: Write{LSN: 1, Block: 1, Value: 1}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 2, Block: 2, Value: 2}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 3, Block: 3, Value: 3}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 4, Block: 4, Value: 4}, Class: RecoveryClassExtentReferenced, PayloadResolvable: true},
		{Write: Write{LSN: 5, Block: 5, Value: 5}, Class: RecoveryClassExtentReferenced, PayloadResolvable: true},
	}

	// Initially fully recoverable.
	if !FullyRecoverable(records) {
		t.Fatal("initial records should be fully recoverable")
	}

	// Simulate payload becoming unresolvable (e.g., extent generation GC'd).
	records[4].PayloadResolvable = false

	// Now NOT recoverable — must detect and abort.
	if FullyRecoverable(records) {
		t.Fatal("after payload loss, records should NOT be recoverable")
	}

	// Apply only the recoverable prefix.
	state := ApplyRecoveryRecords(records[:4], 0, 4) // only first 4
	if state[4] != 4 {
		t.Fatalf("partial apply: block 4 should be 4, got %d", state[4])
	}
	if _, has5 := state[5]; has5 {
		t.Fatal("block 5 should NOT be in partial state — payload was lost")
	}
}

func TestP02_SmartWAL_MixedClassRecovery_FullSuccess(t *testing.T) {
	records := []RecoveryRecord{
		{Write: Write{LSN: 1, Block: 0, Value: 10}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 2, Block: 1, Value: 20}, Class: RecoveryClassExtentReferenced, PayloadResolvable: true},
		{Write: Write{LSN: 3, Block: 0, Value: 30}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 4, Block: 2, Value: 40}, Class: RecoveryClassExtentReferenced, PayloadResolvable: true},
	}

	if !FullyRecoverable(records) {
		t.Fatal("all resolvable — should be recoverable")
	}

	state := ApplyRecoveryRecords(records, 0, 4)
	// Block 0 overwritten: 10 then 30.
	if state[0] != 30 {
		t.Fatalf("block 0: got %d, want 30", state[0])
	}
	if state[1] != 20 {
		t.Fatalf("block 1: got %d, want 20", state[1])
	}
	if state[2] != 40 {
		t.Fatalf("block 2: got %d, want 40", state[2])
	}
}

func TestP02_SmartWAL_TimeVaryingAvailability(t *testing.T) {
	// Simulate time-varying payload availability:
	// At time T1, all records are recoverable.
	// At time T2, one becomes unrecoverable.
	// At time T3, it becomes recoverable again (re-pinned).

	records := []RecoveryRecord{
		{Write: Write{LSN: 1, Block: 0, Value: 1}, Class: RecoveryClassWALInline},
		{Write: Write{LSN: 2, Block: 1, Value: 2}, Class: RecoveryClassExtentReferenced, PayloadResolvable: true},
		{Write: Write{LSN: 3, Block: 2, Value: 3}, Class: RecoveryClassExtentReferenced, PayloadResolvable: true},
	}

	// T1: all recoverable.
	if !FullyRecoverable(records) {
		t.Fatal("T1: should be recoverable")
	}

	// T2: payload for LSN 2 lost.
	records[1].PayloadResolvable = false
	if FullyRecoverable(records) {
		t.Fatal("T2: should NOT be recoverable after payload loss")
	}

	// T3: payload re-pinned (e.g., operator restores snapshot).
	records[1].PayloadResolvable = true
	if !FullyRecoverable(records) {
		t.Fatal("T3: should be recoverable after re-pin")
	}
}

// ============================================================
// Phase 02: Item 5 — Strengthen S5 (flapping replica)
// ============================================================

// S5 strengthened: repeated disconnect/reconnect with catch-up
// state tracking. If flapping exceeds budget, escalate to NeedsRebuild.

func TestP02_S5_FlappingWithStateTracking(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.MaxCatchupAttempts = 10 // generous for flapping

	// Initial writes.
	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	r1 := c.Nodes["r1"]

	// 5 flapping cycles — each creates a small gap then catches up.
	for cycle := 0; cycle < 5; cycle++ {
		c.Disconnect("p", "r1")
		c.Disconnect("r1", "p")

		c.CommitWrite(uint64(3 + cycle*2))
		c.CommitWrite(uint64(4 + cycle*2))
		c.TickN(3)

		c.Connect("p", "r1")
		c.Connect("r1", "p")

		r1.ReplicaState = NodeStateCatchingUp
		converged := c.CatchUpWithEscalation("r1", 100)
		if !converged {
			t.Fatalf("cycle %d: catch-up should converge for small gap", cycle)
		}
		if r1.ReplicaState != NodeStateInSync {
			t.Fatalf("cycle %d: expected InSync, got %s", cycle, r1.ReplicaState)
		}
	}

	// After 5 successful flaps, CatchupAttempts should be 0 (reset on success).
	if r1.CatchupAttempts != 0 {
		t.Fatalf("CatchupAttempts should be 0 after successful catch-ups, got %d", r1.CatchupAttempts)
	}

	// No unnecessary rebuild — r1 should NOT have a base snapshot.
	if r1.Storage.BaseSnapshot != nil {
		t.Fatal("flapping replica should not have been rebuilt — only WAL catch-up")
	}

	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatal(err)
	}
}

func TestP02_S5_FlappingExceedsBudget_EscalatesToNeedsRebuild(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.MaxCatchupAttempts = 3 // tight budget

	c.CommitWrite(1)
	c.TickN(5)

	r1 := c.Nodes["r1"]

	// Each flap creates a gap, but primary writes a LOT during disconnect.
	// Catch-up recovers only 1 entry per attempt. After MaxCatchupAttempts
	// non-convergent attempts, escalate.
	for cycle := 0; cycle < 5; cycle++ {
		c.Disconnect("p", "r1")
		c.Disconnect("r1", "p")

		// Large writes during disconnect.
		for w := 0; w < 30; w++ {
			c.CommitWrite(uint64(cycle*30+w+2) % 8)
		}
		c.TickN(3)

		c.Connect("p", "r1")
		c.Connect("r1", "p")

		r1.ReplicaState = NodeStateCatchingUp

		// Try catch-up with small batch — will not converge.
		for attempt := 0; attempt < 5; attempt++ {
			c.Disconnect("p", "r1")
			c.Disconnect("r1", "p")
			for w := 0; w < 10; w++ {
				c.CommitWrite(uint64(200+cycle*50+attempt*10+w) % 8)
			}
			c.TickN(2)
			c.Connect("p", "r1")
			c.Connect("r1", "p")

			c.CatchUpWithEscalation("r1", 1)

			if r1.ReplicaState == NodeStateNeedsRebuild {
				t.Logf("flapping escalated to NeedsRebuild at cycle %d, attempt %d", cycle, attempt)
				// Verify: NeedsRebuild is sticky.
				c.CatchUpWithEscalation("r1", 100)
				if r1.ReplicaState != NodeStateNeedsRebuild {
					t.Fatal("NeedsRebuild should be sticky — catch-up should not reset it")
				}
				return
			}
		}
	}

	// If we got here, the budget wasn't reached. That's wrong.
	t.Fatalf("expected NeedsRebuild escalation, but state is %s with %d attempts",
		r1.ReplicaState, r1.CatchupAttempts)
}
