package enginev2

import "testing"

// ============================================================
// Acceptance Criteria Traceability: A5-A8
//
// Each test explicitly traces back to an acceptance criterion,
// states the invariant, and provides prototype evidence.
// ============================================================

// --- A5: Non-Convergent Catch-Up Escalates Explicitly ---

// A5 invariant: catch-up that cannot converge must explicitly transition
// to NeedsRebuild. There is no silent failure or infinite retry.
//
// Prototype evidence chain:
//   1. WALHistory.IsRecoverable proves gap is unrecoverable
//   2. RecordHandshakeWithOutcome classifies as OutcomeNeedsRebuild
//   3. Session is invalidated, sender → NeedsRebuild
//   4. Budget enforcement: duration/entries/stall → NeedsRebuild
//   5. Frozen target: progress beyond H0 is rejected

func TestA5_Evidence_UnrecoverableGap_ExplicitEscalation(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}
	primary.Commit(100)
	primary.AdvanceTail(60)

	// Step 1: prove gap is unrecoverable.
	if primary.IsRecoverable(30, 100) {
		t.Fatal("A5: gap 30→100 should be provably unrecoverable")
	}

	// Step 2: handshake classifies correctly.
	hr := primary.MakeHandshakeResult(30)
	outcome := ClassifyRecoveryOutcome(hr)
	if outcome != OutcomeNeedsRebuild {
		t.Fatalf("A5: outcome=%s, want needs_rebuild", outcome)
	}

	// Step 3: sender execution escalates.
	s := NewSender("r1", Endpoint{DataAddr: "r1", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)
	o, _ := s.RecordHandshakeWithOutcome(sess.ID, hr)
	if o != OutcomeNeedsRebuild || s.State != StateNeedsRebuild {
		t.Fatal("A5: sender should escalate to NeedsRebuild")
	}
}

func TestA5_Evidence_BudgetExceeded_ExplicitEscalation(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	sess.Budget = &CatchUpBudget{MaxDurationTicks: 5}

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.BeginCatchUp(sess.ID, 0)
	s.RecordCatchUpProgress(sess.ID, 10)

	// Budget exceeded → explicit escalation.
	v, _ := s.CheckBudget(sess.ID, 10)
	if v != BudgetDurationExceeded {
		t.Fatalf("A5: budget=%s, want duration_exceeded", v)
	}
	if s.State != StateNeedsRebuild {
		t.Fatal("A5: should escalate to NeedsRebuild on budget violation")
	}
}

func TestA5_Evidence_FrozenTarget_RejectsChase(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 50)
	s.BeginCatchUp(sess.ID)

	if err := s.RecordCatchUpProgress(sess.ID, 51); err == nil {
		t.Fatal("A5: progress beyond frozen target should be rejected")
	}
}

// --- A6: Recoverability Boundary Is Explicit ---

// A6 invariant: the boundary between recoverable and unrecoverable gap
// is a provable, exact decision — not a heuristic.
//
// Prototype evidence chain:
//   1. WALHistory.IsRecoverable checks contiguity + range
//   2. ClassifyRecoveryOutcome uses CommittedLSN (not WAL head)
//   3. Exact boundary: tail±1 LSN flips the outcome

func TestA6_Evidence_ExactBoundary(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1})
	}
	primary.Commit(100)
	primary.AdvanceTail(50)

	// AT boundary: recoverable.
	if !primary.IsRecoverable(50, 100) {
		t.Fatal("A6: LSN 50 (at tail) should be recoverable")
	}
	if ClassifyRecoveryOutcome(primary.MakeHandshakeResult(50)) != OutcomeCatchUp {
		t.Fatal("A6: should classify as catchup at boundary")
	}

	// ONE BELOW boundary: unrecoverable.
	if primary.IsRecoverable(49, 100) {
		t.Fatal("A6: LSN 49 (below tail) should be unrecoverable")
	}
	if ClassifyRecoveryOutcome(primary.MakeHandshakeResult(49)) != OutcomeNeedsRebuild {
		t.Fatal("A6: should classify as needs_rebuild below boundary")
	}
}

func TestA6_Evidence_ContiguityRequired(t *testing.T) {
	w := NewWALHistory()
	w.Append(WALEntry{LSN: 1})
	w.Append(WALEntry{LSN: 2})
	w.Append(WALEntry{LSN: 3})
	w.entries = append(w.entries, WALEntry{LSN: 5}) // hole at 4
	w.entries = append(w.entries, WALEntry{LSN: 6})
	w.headLSN = 6

	if w.IsRecoverable(0, 6) {
		t.Fatal("A6: non-contiguous range should not be recoverable")
	}
}

// --- A7: Historical Data Correctness Holds ---

// A7 invariant: recovered data at target LSN is historically correct.
// Current extent cannot fake old history.
//
// Prototype evidence chain:
//   1. WALHistory.StateAt with snapshot survives tail advancement
//   2. Catch-up replay produces identical state to primary
//   3. Truncation removes divergent tail before InSync

func TestA7_Evidence_SnapshotPreservesHistory(t *testing.T) {
	w := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		w.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 5, Value: i * 100})
	}
	w.Commit(100)

	stateBefore := w.StateAt(100)
	w.AdvanceTail(50) // recycle half
	stateAfter := w.StateAt(100)

	if stateAfter == nil {
		t.Fatal("A7: state should be valid after tail advance")
	}
	for block, before := range stateBefore {
		if after := stateAfter[block]; after != before {
			t.Fatalf("A7: block %d: before=%d after=%d", block, before, after)
		}
	}
}

func TestA7_Evidence_CatchUpReplayMatchesPrimary(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 50; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}
	primary.Commit(50)

	replica := NewWALHistory()
	for i := uint64(1); i <= 30; i++ {
		replica.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}

	entries, _ := primary.EntriesInRange(30, 50)
	for _, e := range entries {
		replica.Append(e)
	}

	for block, pVal := range primary.StateAt(50) {
		if rVal := replica.StateAt(50)[block]; rVal != pVal {
			t.Fatalf("A7: block %d: primary=%d replica=%d", block, pVal, rVal)
		}
	}
}

func TestA7_Evidence_TruncationRemovesDivergentTail(t *testing.T) {
	w := NewWALHistory()
	for i := uint64(1); i <= 10; i++ {
		w.Append(WALEntry{LSN: i, Block: i, Value: i * 10})
	}
	w.Commit(7)
	w.Truncate(7)

	if w.HeadLSN() != 7 {
		t.Fatalf("A7: head=%d after truncate, want 7", w.HeadLSN())
	}
	entries, _ := w.EntriesInRange(7, 10)
	if len(entries) != 0 {
		t.Fatal("A7: divergent tail should be gone")
	}
}

// --- A8: Durability Mode Semantics (prototype evidence) ---

// A8 invariant: the prototype's bounded catch-up contract does not
// silently weaken durability guarantees.
//
// Prototype evidence: bounded catch-up + frozen target ensures that
// recovery never claims InSync status for data beyond what was
// actually replayed and verified.

func TestA8_Evidence_CompletionRequiresConvergence(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.BeginCatchUp(sess.ID)
	s.RecordCatchUpProgress(sess.ID, 50) // not converged

	// Cannot complete without convergence.
	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("A8: must not complete without convergence")
	}

	s.RecordCatchUpProgress(sess.ID, 100) // converged
	if !s.CompleteSessionByID(sess.ID) {
		t.Fatal("A8: should complete after convergence")
	}
}

func TestA8_Evidence_TruncationRequiredBeforeInSync(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(sess.ID)
	s.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 105, CommittedLSN: 100, RetentionStartLSN: 50,
	})

	// Cannot complete without truncation.
	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("A8: must not complete without truncation")
	}
}

// --- Rebuild-source decision evidence ---

func TestRebuildSource_SnapshotTail_WhenTrustedBaseExists(t *testing.T) {
	rs := NewRebuildState()
	rs.SelectSource(50, true, 100) // trusted snapshot at LSN 50

	if rs.Source != RebuildSnapshotTail {
		t.Fatalf("should select snapshot_tail when trusted base exists, got %s", rs.Source)
	}
	if rs.TailStartLSN != 50 || rs.TailTargetLSN != 100 {
		t.Fatalf("tail range: %d→%d", rs.TailStartLSN, rs.TailTargetLSN)
	}
}

func TestRebuildSource_FullBase_WhenNoSnapshot(t *testing.T) {
	rs := NewRebuildState()
	rs.SelectSource(0, false, 100)

	if rs.Source != RebuildFullBase {
		t.Fatalf("should select full_base when no snapshot, got %s", rs.Source)
	}
}

func TestRebuildSource_FullBase_WhenSnapshotUntrusted(t *testing.T) {
	rs := NewRebuildState()
	rs.SelectSource(50, false, 100) // snapshot exists but not trusted

	if rs.Source != RebuildFullBase {
		t.Fatalf("should select full_base when snapshot untrusted, got %s", rs.Source)
	}
}
