package distsim

import (
	"testing"
)

// Phase 4.5: Crash-consistency and recoverability tests.
// These validate invariants I1-I5 from the crash-consistency simulation plan.

// --- Invariant I1: ACK'd flush is recoverable after any crash ---

func TestI1_AckedFlush_RecoverableAfterPrimaryCrash(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r")

	// Write 3 entries and commit (sync_all = durable on both nodes).
	for i := 0; i < 3; i++ {
		c.CommitWrite(uint64(i + 1))
	}
	c.Tick()
	c.Tick()
	c.Tick()

	if c.Coordinator.CommittedLSN < 3 {
		t.Fatalf("expected CommittedLSN>=3, got %d", c.Coordinator.CommittedLSN)
	}

	committedLSN := c.Coordinator.CommittedLSN

	// Crash the primary.
	primary := c.Nodes["p"]
	primary.Storage.Crash()

	// Restart: recover from checkpoint + durable WAL.
	recoveredLSN := primary.Storage.Restart()

	// I1: all committed data must be recoverable.
	if recoveredLSN < committedLSN {
		t.Fatalf("I1 VIOLATED: recoveredLSN=%d < committedLSN=%d — acked data lost",
			recoveredLSN, committedLSN)
	}

	// Verify data correctness against reference.
	refState := c.Reference.StateAt(committedLSN)
	recState := primary.Storage.StateAt(committedLSN)
	for block, expected := range refState {
		if got := recState[block]; got != expected {
			t.Fatalf("I1 VIOLATED: block %d: reference=%d recovered=%d", block, expected, got)
		}
	}
}

// --- Invariant I2: No ghost visible state after crash ---

func TestI2_ExtentAheadOfCheckpoint_CrashRestart(t *testing.T) {
	s := NewStorage()

	// Write 5 entries to WAL.
	for i := uint64(1); i <= 5; i++ {
		s.AppendWrite(Write{Block: 10 + i, Value: i * 100, LSN: i})
	}

	// Make all 5 durable.
	s.AdvanceFlush(5)

	// Flusher materializes entries 1-3 to live extent.
	s.ApplyToExtent(3)

	// Checkpoint at LSN 1 only.
	s.AdvanceCheckpoint(1)

	// Crash.
	s.Crash()
	if s.LiveExtent != nil {
		t.Fatal("after crash, LiveExtent should be nil")
	}

	// Restart.
	recoveredLSN := s.Restart()
	if recoveredLSN != 5 {
		t.Fatalf("recoveredLSN should be 5, got %d", recoveredLSN)
	}

	// I2: all durable data recovered from checkpoint + WAL replay.
	for i := uint64(1); i <= 5; i++ {
		block := 10 + i
		expected := i * 100
		if got := s.LiveExtent[block]; got != expected {
			t.Fatalf("I2: block %d: expected %d, got %d", block, expected, got)
		}
	}
}

func TestI2_UnackedData_LostAfterCrash(t *testing.T) {
	s := NewStorage()

	for i := uint64(1); i <= 5; i++ {
		s.AppendWrite(Write{Block: i, Value: i * 10, LSN: i})
	}

	// Only fsync 1-3. Entries 4-5 are NOT durable.
	s.AdvanceFlush(3)
	s.ApplyToExtent(5) // should clamp to 3
	s.AdvanceCheckpoint(3)

	if s.ExtentAppliedLSN != 3 {
		t.Fatalf("ApplyToExtent should clamp to WALDurableLSN=3, got %d", s.ExtentAppliedLSN)
	}

	s.Crash()
	s.Restart()

	// Blocks 4,5 must NOT be in recovered extent.
	if val, ok := s.LiveExtent[4]; ok && val != 0 {
		t.Fatalf("I2 VIOLATED: block 4=%d survived crash — unfsynced data", val)
	}
	if val, ok := s.LiveExtent[5]; ok && val != 0 {
		t.Fatalf("I2 VIOLATED: block 5=%d survived crash — unfsynced data", val)
	}

	// Blocks 1-3 must be there.
	for i := uint64(1); i <= 3; i++ {
		if got := s.LiveExtent[i]; got != i*10 {
			t.Fatalf("block %d: expected %d, got %d", i, i*10, got)
		}
	}
}

// --- Invariant I3: CatchUp converges or escalates ---

func TestI3_CatchUpConvergesOrEscalates(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r")

	// Commit initial entry.
	c.CommitWrite(1)
	c.Tick()
	c.Tick()

	// Disconnect replica and write more.
	c.Nodes["r"].Running = false
	for i := uint64(2); i <= 10; i++ {
		c.CommitWrite(i)
		c.Tick()
	}

	// Reconnect.
	c.Nodes["r"].Running = true
	c.Nodes["r"].ReplicaState = NodeStateLagging

	// Catch-up with escalation.
	converged := c.CatchUpWithEscalation("r", 3)

	// I3: must resolve — either converged or escalated to NeedsRebuild.
	state := c.Nodes["r"].ReplicaState
	if !converged && state != NodeStateNeedsRebuild {
		t.Fatalf("I3 VIOLATED: catchup did not converge and state=%s (not NeedsRebuild)", state)
	}
}

// --- Invariant I4: Promoted replica has committed prefix ---

func TestI4_PromotedReplica_HasCommittedPrefix(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r")

	for i := uint64(1); i <= 5; i++ {
		c.CommitWrite(i)
	}
	c.Tick()
	c.Tick()
	c.Tick()

	committedLSN := c.Coordinator.CommittedLSN
	if committedLSN < 5 {
		t.Fatalf("expected CommittedLSN>=5, got %d", committedLSN)
	}

	// Promote replica.
	if err := c.Promote("r"); err != nil {
		t.Fatalf("promote: %v", err)
	}

	// I4: new primary must have recoverable committed prefix.
	newPrimary := c.Nodes["r"]
	recoverableLSN := newPrimary.Storage.RecoverableLSN()
	if recoverableLSN < committedLSN {
		t.Fatalf("I4 VIOLATED: promoted recoverableLSN=%d < committedLSN=%d",
			recoverableLSN, committedLSN)
	}

	// Verify data matches reference.
	refState := c.Reference.StateAt(committedLSN)
	recState := newPrimary.Storage.StateAt(committedLSN)
	for block, expected := range refState {
		if got := recState[block]; got != expected {
			t.Fatalf("I4 VIOLATED: block %d: ref=%d got=%d", block, expected, got)
		}
	}
}

// --- Direct test: checkpoint must not leak applied-but-uncheckpointed state ---

func TestI2_CheckpointDoesNotLeakAppliedState(t *testing.T) {
	s := NewStorage()

	// Write 5 entries, all durable.
	for i := uint64(1); i <= 5; i++ {
		s.AppendWrite(Write{Block: i, Value: i * 10, LSN: i})
	}
	s.AdvanceFlush(5)

	// Flusher applies all 5 to LiveExtent.
	s.ApplyToExtent(5)

	// But checkpoint only at LSN 2.
	s.AdvanceCheckpoint(2)

	// CheckpointExtent must contain ONLY blocks 1-2, not 3-5.
	for i := uint64(3); i <= 5; i++ {
		if val, ok := s.CheckpointExtent[i]; ok && val != 0 {
			t.Fatalf("CHECKPOINT LEAK: block %d=%d in checkpoint but CheckpointLSN=2", i, val)
		}
	}
	// Blocks 1-2 must be in checkpoint.
	for i := uint64(1); i <= 2; i++ {
		expected := i * 10
		if got := s.CheckpointExtent[i]; got != expected {
			t.Fatalf("block %d: checkpoint should have %d, got %d", i, expected, got)
		}
	}

	// Now crash: LiveExtent lost, entries 3-5 only in WAL.
	s.Crash()
	recoveredLSN := s.Restart()

	if recoveredLSN != 5 {
		t.Fatalf("recoveredLSN should be 5, got %d", recoveredLSN)
	}

	// All 5 blocks must be recovered: 1-2 from checkpoint, 3-5 from WAL replay.
	for i := uint64(1); i <= 5; i++ {
		expected := i * 10
		if got := s.LiveExtent[i]; got != expected {
			t.Fatalf("block %d: expected %d after crash+restart, got %d", i, expected, got)
		}
	}
}

// --- A7: Historical state before checkpoint is not fakeable ---

func TestA7_HistoricalState_NotReconstructableAfterGC(t *testing.T) {
	s := NewStorage()

	// Write 10 entries, all durable.
	for i := uint64(1); i <= 10; i++ {
		s.AppendWrite(Write{Block: i, Value: i * 10, LSN: i})
	}
	s.AdvanceFlush(10)
	s.ApplyToExtent(10)

	// Checkpoint at LSN 7.
	s.AdvanceCheckpoint(7)

	// GC WAL entries before checkpoint.
	retained := make([]Write, 0)
	for _, w := range s.WAL {
		if w.LSN > s.CheckpointLSN {
			retained = append(retained, w)
		}
	}
	s.WAL = retained

	// Can reconstruct at LSN 7 (checkpoint covers it).
	if !s.CanReconstructAt(7) {
		t.Fatal("should be reconstructable at checkpoint LSN")
	}

	// Can reconstruct at LSN 10 (checkpoint + WAL 8-10).
	if !s.CanReconstructAt(10) {
		t.Fatal("should be reconstructable at LSN 10 (checkpoint + WAL)")
	}

	// CANNOT accurately reconstruct at LSN 3 (WAL 1-6 has been GC'd).
	// The state at LSN 3 required WAL entries 1-3 which are gone.
	if s.CanReconstructAt(3) {
		t.Fatal("A7: should NOT be reconstructable at LSN 3 after WAL GC — history is lost")
	}

	// StateAt(3) returns checkpoint state (best-effort approximation, not exact).
	// This is fine for display but must NOT be treated as authoritative.
	state3 := s.StateAt(3)
	// The returned state includes blocks 1-7 (from checkpoint), which is MORE
	// than what was actually committed at LSN 3. This is the "current extent
	// cannot fake old history" problem from A7.
	if len(state3) == 3 {
		t.Fatal("StateAt(3) after GC should return checkpoint state (7 blocks), not exact 3-block state")
	}
}

// --- Invariant I5: Checkpoint GC preserves recovery proof ---

func TestI5_CheckpointGC_PreservesAckedBoundary(t *testing.T) {
	s := NewStorage()

	for i := uint64(1); i <= 10; i++ {
		s.AppendWrite(Write{Block: i, Value: i * 10, LSN: i})
	}
	s.AdvanceFlush(10)
	s.ApplyToExtent(7)
	s.AdvanceCheckpoint(7)

	// GC: remove WAL entries before checkpoint.
	retained := make([]Write, 0)
	for _, w := range s.WAL {
		if w.LSN > s.CheckpointLSN {
			retained = append(retained, w)
		}
	}
	s.WAL = retained

	// Crash + restart.
	s.Crash()
	recoveredLSN := s.Restart()

	if recoveredLSN != 10 {
		t.Fatalf("I5: recoveredLSN should be 10, got %d", recoveredLSN)
	}

	// All 10 blocks recoverable: 1-7 from checkpoint, 8-10 from WAL.
	for i := uint64(1); i <= 10; i++ {
		expected := i * 10
		if got := s.LiveExtent[i]; got != expected {
			t.Fatalf("I5 VIOLATED: block %d: expected %d, got %d", i, expected, got)
		}
	}
}
