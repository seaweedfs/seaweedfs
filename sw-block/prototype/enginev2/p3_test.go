package enginev2

import "testing"

// ============================================================
// Phase 04 P3: Historical data model and recoverability proof
// ============================================================

// --- WALHistory basics ---

func TestWAL_AppendAndCommit(t *testing.T) {
	w := NewWALHistory()
	w.Append(WALEntry{LSN: 1, Epoch: 1, Block: 1, Value: 10})
	w.Append(WALEntry{LSN: 2, Epoch: 1, Block: 2, Value: 20})
	w.Commit(2)

	if w.HeadLSN() != 2 {
		t.Fatalf("head=%d, want 2", w.HeadLSN())
	}
	if w.CommittedLSN() != 2 {
		t.Fatalf("committed=%d, want 2", w.CommittedLSN())
	}
}

func TestWAL_AppendRejectsRegression(t *testing.T) {
	w := NewWALHistory()
	w.Append(WALEntry{LSN: 5})
	if err := w.Append(WALEntry{LSN: 3}); err == nil {
		t.Fatal("should reject LSN regression")
	}
}

func TestWAL_CommitRejectsAheadOfHead(t *testing.T) {
	w := NewWALHistory()
	w.Append(WALEntry{LSN: 5})
	if err := w.Commit(10); err == nil {
		t.Fatal("should reject commit ahead of head")
	}
}

func TestWAL_AdvanceTail_RecyclesEntries(t *testing.T) {
	w := NewWALHistory()
	for i := uint64(1); i <= 10; i++ {
		w.Append(WALEntry{LSN: i, Block: i, Value: i * 10})
	}
	w.Commit(10)

	w.AdvanceTail(5) // recycle LSNs 1-5

	if w.TailLSN() != 5 {
		t.Fatalf("tail=%d, want 5", w.TailLSN())
	}
	if w.Len() != 5 {
		t.Fatalf("retained=%d, want 5", w.Len())
	}

	// Entries 1-5 gone.
	_, err := w.EntriesInRange(0, 5)
	if err == nil {
		t.Fatal("recycled range should return error")
	}

	// Entries 6-10 available.
	entries, err := w.EntriesInRange(5, 10)
	if err != nil {
		t.Fatalf("retained range should work: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("entries=%d, want 5", len(entries))
	}
}

func TestWAL_Truncate_RemovesDivergentTail(t *testing.T) {
	w := NewWALHistory()
	for i := uint64(1); i <= 10; i++ {
		w.Append(WALEntry{LSN: i, Block: i, Value: i * 10})
	}
	w.Commit(7) // committed up to 7, entries 8-10 are uncommitted

	w.Truncate(7) // remove divergent tail

	if w.HeadLSN() != 7 {
		t.Fatalf("head=%d after truncate, want 7", w.HeadLSN())
	}
	if w.Len() != 7 {
		t.Fatalf("retained=%d after truncate, want 7", w.Len())
	}
}

func TestWAL_StateAt_HistoricalCorrectness(t *testing.T) {
	w := NewWALHistory()
	w.Append(WALEntry{LSN: 1, Block: 1, Value: 100})
	w.Append(WALEntry{LSN: 2, Block: 1, Value: 200}) // overwrites block 1
	w.Append(WALEntry{LSN: 3, Block: 2, Value: 300})

	// State at LSN 1: block 1 = 100.
	s1 := w.StateAt(1)
	if s1[1] != 100 {
		t.Fatalf("state@1: block 1 = %d, want 100", s1[1])
	}

	// State at LSN 2: block 1 = 200 (overwritten).
	s2 := w.StateAt(2)
	if s2[1] != 200 {
		t.Fatalf("state@2: block 1 = %d, want 200", s2[1])
	}

	// State at LSN 3: block 1 = 200, block 2 = 300.
	s3 := w.StateAt(3)
	if s3[1] != 200 || s3[2] != 300 {
		t.Fatalf("state@3: %v", s3)
	}
}

// --- IsRecoverable: strengthened checks ---

func TestWAL_IsRecoverable_FalseWhenEndBeyondHead(t *testing.T) {
	w := NewWALHistory()
	for i := uint64(1); i <= 50; i++ {
		w.Append(WALEntry{LSN: i})
	}
	// End is 100 but head is only 50.
	if w.IsRecoverable(0, 100) {
		t.Fatal("should be false when endInclusive > headLSN")
	}
}

func TestWAL_IsRecoverable_FalseWhenEntriesMissing(t *testing.T) {
	w := NewWALHistory()
	// Append non-contiguous: 1, 2, 3, 5, 6 (missing 4).
	w.Append(WALEntry{LSN: 1})
	w.Append(WALEntry{LSN: 2})
	w.Append(WALEntry{LSN: 3})
	w.entries = append(w.entries, WALEntry{LSN: 5}) // skip 4
	w.entries = append(w.entries, WALEntry{LSN: 6})
	w.headLSN = 6

	// Range 0→6 has a hole at LSN 4.
	if w.IsRecoverable(0, 6) {
		t.Fatal("should be false when entries are non-contiguous")
	}
	// Range 0→3 is contiguous.
	if !w.IsRecoverable(0, 3) {
		t.Fatal("range 0→3 should be recoverable (contiguous)")
	}
}

// --- StateAt: correctness after tail advancement ---

func TestWAL_StateAt_CorrectAfterTailAdvance(t *testing.T) {
	w := NewWALHistory()
	w.Append(WALEntry{LSN: 1, Block: 1, Value: 100})
	w.Append(WALEntry{LSN: 2, Block: 2, Value: 200})
	w.Append(WALEntry{LSN: 3, Block: 1, Value: 300}) // overwrites block 1
	w.Append(WALEntry{LSN: 4, Block: 3, Value: 400})
	w.Append(WALEntry{LSN: 5, Block: 2, Value: 500}) // overwrites block 2

	w.AdvanceTail(3) // recycle LSNs 1-3, snapshot captures block1=300, block2=200

	// StateAt(5): should include snapshot state + retained entries.
	s := w.StateAt(5)
	if s[1] != 300 { // from snapshot (LSN 3)
		t.Fatalf("block 1=%d, want 300", s[1])
	}
	if s[2] != 500 { // from retained entry (LSN 5)
		t.Fatalf("block 2=%d, want 500", s[2])
	}
	if s[3] != 400 { // from retained entry (LSN 4)
		t.Fatalf("block 3=%d, want 400", s[3])
	}
}

func TestWAL_StateAt_ReturnsNilBeforeSnapshot(t *testing.T) {
	w := NewWALHistory()
	for i := uint64(1); i <= 10; i++ {
		w.Append(WALEntry{LSN: i, Block: i, Value: i * 10})
	}
	w.AdvanceTail(5) // snapshot at LSN 5

	// StateAt(3) — before snapshot boundary, entries recycled.
	s := w.StateAt(3)
	if s != nil {
		t.Fatal("StateAt before snapshot should return nil")
	}

	// StateAt(5) — at snapshot boundary, valid.
	s = w.StateAt(5)
	if s == nil {
		t.Fatal("StateAt at snapshot boundary should work")
	}
}

func TestWAL_StateAt_BlockLastWrittenBeforeTail(t *testing.T) {
	w := NewWALHistory()
	w.Append(WALEntry{LSN: 1, Block: 99, Value: 42}) // block 99 only written once
	w.Append(WALEntry{LSN: 2, Block: 1, Value: 100})
	w.Append(WALEntry{LSN: 3, Block: 1, Value: 200})

	w.AdvanceTail(2) // snapshot captures block99=42, block1=100

	// StateAt(3): block 99's last write was LSN 1 (recycled), but
	// captured in snapshot. Must still be present.
	s := w.StateAt(3)
	if s[99] != 42 {
		t.Fatalf("block 99=%d, want 42 (from snapshot of recycled entry)", s[99])
	}
	if s[1] != 200 {
		t.Fatalf("block 1=%d, want 200 (from retained entry)", s[1])
	}
}

// --- Recoverability proof: "why is catch-up allowed?" ---

func TestRecoverability_Provable_GapWithinRetention(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 10, Value: i})
	}
	primary.Commit(100)
	primary.AdvanceTail(30) // retain LSNs 31-100

	// Replica at LSN 50 — gap is 51-100, within retention.
	if !primary.IsRecoverable(50, 100) {
		t.Fatal("gap 50→100 should be recoverable (tail=30)")
	}

	entries, err := primary.EntriesInRange(50, 100)
	if err != nil {
		t.Fatalf("entries should be available: %v", err)
	}
	if len(entries) != 50 {
		t.Fatalf("entries=%d, want 50", len(entries))
	}

	// Handshake result proves recoverability.
	hr := primary.MakeHandshakeResult(50)
	outcome := ClassifyRecoveryOutcome(hr)
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s, want catchup", outcome)
	}
}

func TestRecoverability_Unprovable_GapBeyondRetention(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 10, Value: i})
	}
	primary.Commit(100)
	primary.AdvanceTail(60) // retain only LSNs 61-100

	// Replica at LSN 50 — gap is 51-100, but 51-60 are recycled.
	if primary.IsRecoverable(50, 100) {
		t.Fatal("gap 50→100 should NOT be recoverable (tail=60)")
	}

	_, err := primary.EntriesInRange(50, 100)
	if err == nil {
		t.Fatal("recycled entries should return error")
	}

	hr := primary.MakeHandshakeResult(50)
	outcome := ClassifyRecoveryOutcome(hr)
	if outcome != OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s, want needs_rebuild", outcome)
	}
}

func TestRecoverability_ExactBoundary(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1})
	}
	primary.Commit(100)
	primary.AdvanceTail(50) // retain 51-100

	// Replica at LSN 50 — gap starts at exactly tailLSN. Recoverable.
	if !primary.IsRecoverable(50, 100) {
		t.Fatal("exact boundary should be recoverable")
	}

	// Replica at LSN 49 — gap starts before tailLSN. NOT recoverable.
	if primary.IsRecoverable(49, 100) {
		t.Fatal("one-below boundary should NOT be recoverable")
	}
}

// --- Truncation: divergent tail handling ---

func TestTruncation_ReplicaAhead_RequiresTruncateBeforeInSync(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	// Replica is ahead of committed (divergent uncommitted tail).
	outcome, err := s.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 105,
		CommittedLSN:      100,
		RetentionStartLSN: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome != OutcomeCatchUp {
		t.Fatalf("replica ahead should be catchup (needs truncation), got %s", outcome)
	}

	// Session requires truncation.
	if !sess.TruncateRequired {
		t.Fatal("session should require truncation")
	}
	if sess.TruncateToLSN != 100 {
		t.Fatalf("truncate to=%d, want 100", sess.TruncateToLSN)
	}

	// Completion WITHOUT truncation recorded — rejected.
	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion without truncation should be rejected")
	}

	// Record truncation.
	if err := s.RecordTruncation(sess.ID, 100); err != nil {
		t.Fatalf("truncation record: %v", err)
	}

	// Now completion succeeds (zero-gap after truncation).
	if !s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion after truncation should succeed")
	}
	if s.State != StateInSync {
		t.Fatalf("state=%s, want in_sync", s.State)
	}
}

func TestTruncation_WrongLSN_Rejected(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	s.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 105,
		CommittedLSN:      100,
		RetentionStartLSN: 50,
	})

	// Wrong truncation LSN.
	if err := s.RecordTruncation(sess.ID, 99); err == nil {
		t.Fatal("wrong truncation LSN should be rejected")
	}
}

func TestTruncation_NotRequired_Rejected(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	// Normal gap (replica behind, not ahead).
	s.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 80,
		CommittedLSN:      100,
		RetentionStartLSN: 50,
	})

	// Truncation not required.
	if err := s.RecordTruncation(sess.ID, 100); err == nil {
		t.Fatal("truncation on non-truncate session should be rejected")
	}
}

// --- End-to-end: WAL-backed recovery with data verification ---

func TestE2E_WALBacked_CatchUpRecovery(t *testing.T) {
	// Primary WAL with history.
	primary := NewWALHistory()
	for i := uint64(1); i <= 50; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}
	primary.Commit(50)
	primary.AdvanceTail(10) // retain 11-50

	// Replica WAL (simulates a replica that fell behind at LSN 30).
	replica := NewWALHistory()
	for i := uint64(1); i <= 30; i++ {
		replica.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}

	// Sender + session.
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	// Handshake with WAL-backed result.
	hr := primary.MakeHandshakeResult(30)
	outcome, _ := s.RecordHandshakeWithOutcome(sess.ID, hr)
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", outcome)
	}

	// Verify the gap is provably recoverable.
	if !primary.IsRecoverable(30, 50) {
		t.Fatal("gap should be provably recoverable from WAL")
	}

	// Get catch-up entries.
	entries, err := primary.EntriesInRange(30, 50)
	if err != nil {
		t.Fatalf("catch-up entries: %v", err)
	}

	// Apply to replica.
	s.BeginCatchUp(sess.ID)
	for _, e := range entries {
		replica.Append(e)
		s.RecordCatchUpProgress(sess.ID, e.LSN)
	}

	// Complete.
	if !s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion should succeed")
	}

	// Verify historical data correctness: replica state at committed LSN
	// matches primary state at committed LSN.
	primaryState := primary.StateAt(50)
	replicaState := replica.StateAt(50)
	for block, pVal := range primaryState {
		if rVal := replicaState[block]; rVal != pVal {
			t.Fatalf("block %d: primary=%d replica=%d", block, pVal, rVal)
		}
	}
	t.Logf("WAL-backed recovery: gap 30→50 proven recoverable, data verified")
}

func TestE2E_WALBacked_RebuildFallback(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}
	primary.Commit(100)
	primary.AdvanceTail(60) // only retain 61-100

	// Sender + session.
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	// Handshake: replica at LSN 30, but retention starts at 61. Unrecoverable.
	hr := primary.MakeHandshakeResult(30)
	outcome, _ := s.RecordHandshakeWithOutcome(sess.ID, hr)
	if outcome != OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s, want needs_rebuild", outcome)
	}

	// WAL proves WHY rebuild is needed.
	if primary.IsRecoverable(30, 100) {
		t.Fatal("gap should NOT be recoverable")
	}

	// Sender state.
	if s.State != StateNeedsRebuild {
		t.Fatalf("state=%s", s.State)
	}
	t.Logf("WAL-backed rebuild: gap 30→100 proven unrecoverable (tail=60)")
}

func TestE2E_WALBacked_TruncateThenInSync(t *testing.T) {
	// Replica is ahead of committed — has divergent tail.
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}
	primary.Commit(100)

	replica := NewWALHistory()
	for i := uint64(1); i <= 105; i++ {
		replica.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}
	// Entries 101-105 are divergent (uncommitted on primary).

	// Sender.
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	hr := primary.MakeHandshakeResult(105)
	outcome, _ := s.RecordHandshakeWithOutcome(sess.ID, hr)
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", outcome)
	}
	if !sess.TruncateRequired {
		t.Fatal("truncation should be required")
	}

	// Truncate replica's divergent tail.
	replica.Truncate(100)
	if replica.HeadLSN() != 100 {
		t.Fatalf("replica head after truncate=%d, want 100", replica.HeadLSN())
	}

	// Record truncation in session.
	s.RecordTruncation(sess.ID, 100)

	// Complete (zero-gap after truncation).
	if !s.CompleteSessionByID(sess.ID) {
		t.Fatal("completion after truncation should succeed")
	}

	// Verify data matches at committed boundary.
	primaryState := primary.StateAt(100)
	replicaState := replica.StateAt(100)
	for block, pVal := range primaryState {
		if rVal := replicaState[block]; rVal != pVal {
			t.Fatalf("block %d: primary=%d replica=%d", block, pVal, rVal)
		}
	}
	t.Logf("truncate-then-InSync: divergent tail removed, data verified at committed=100")
}
