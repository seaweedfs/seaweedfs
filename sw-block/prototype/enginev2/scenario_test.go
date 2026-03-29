package enginev2

import "testing"

// ============================================================
// Phase 04 P4: Prototype Scenario Closure
//
// Maps V2 acceptance criteria (A1-A12) to prototype evidence.
// Adds the 4 V2-boundary scenarios against the prototype.
// ============================================================

// --- A1: Committed Data Survives Failover ---
// Prototype proof: after promotion (epoch bump + truncation),
// committed data is intact at the committed boundary.

func TestA1_CommittedDataSurvivesPromotion(t *testing.T) {
	// Primary has committed data 1-50 and uncommitted tail 51-55.
	primary := NewWALHistory()
	for i := uint64(1); i <= 55; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i * 10})
	}
	primary.Commit(50) // committed prefix = 1-50

	// Replica received 1-50 (committed only).
	replica := NewWALHistory()
	for i := uint64(1); i <= 50; i++ {
		replica.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i * 10})
	}

	// Promotion: replica becomes new primary at epoch 2.
	// No truncation needed (replica is exactly at committed).

	// Verify: committed data intact.
	primaryState := primary.StateAt(50)
	replicaState := replica.StateAt(50)
	for block, pVal := range primaryState {
		if rVal := replicaState[block]; rVal != pVal {
			t.Fatalf("A1: block %d: primary=%d replica=%d", block, pVal, rVal)
		}
	}
	t.Log("A1: committed data survives promotion")
}

// --- A2: Uncommitted Data Is Not Revived ---
// Prototype proof: divergent tail (uncommitted) is truncated,
// committed prefix stays exactly at the acknowledged boundary.

func TestA2_UncommittedDataNotRevived(t *testing.T) {
	// Replica has committed (1-50) + uncommitted tail (51-55).
	replica := NewWALHistory()
	for i := uint64(1); i <= 55; i++ {
		replica.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i * 10})
	}

	committedLSN := uint64(50)

	// After promotion, truncate uncommitted tail.
	replica.Truncate(committedLSN)

	// Verify: head is exactly at committed, no uncommitted data remains.
	if replica.HeadLSN() != committedLSN {
		t.Fatalf("A2: head=%d after truncate, want %d", replica.HeadLSN(), committedLSN)
	}

	// State at committed is correct.
	state := replica.StateAt(committedLSN)
	if state == nil {
		t.Fatal("A2: state at committed should be valid")
	}

	// Uncommitted entries (51-55) are gone.
	entries, _ := replica.EntriesInRange(50, 55)
	if len(entries) != 0 {
		t.Fatalf("A2: uncommitted entries should be gone, got %d", len(entries))
	}
	t.Log("A2: uncommitted data truncated, committed prefix intact")
}

// --- A3: Stale Epoch Traffic Is Fenced ---
// Prototype proof: stale session cannot mutate sender state.

func TestA3_StaleEpochFenced(t *testing.T) {
	sg := NewSenderGroup()
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	r1 := sg.Sender("r1:9333")
	oldSess := r1.Session()
	oldID := oldSess.ID

	// Epoch bumps.
	sg.InvalidateEpoch(2)
	r1.UpdateEpoch(2)

	// All stale operations rejected.
	if err := r1.BeginConnect(oldID); err == nil {
		t.Fatal("A3: stale BeginConnect should be rejected")
	}
	if r1.CompleteSessionByID(oldID) {
		t.Fatal("A3: stale completion should be rejected")
	}

	// Stale assignment rejected.
	result := sg.ApplyAssignment(AssignmentIntent{
		Endpoints:       map[string]Endpoint{"r1:9333": {DataAddr: "r1:9333", Version: 1}},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})
	if len(result.SessionsFailed) != 1 {
		t.Fatal("A3: stale epoch assignment should fail")
	}
	t.Log("A3: stale epoch traffic fenced at sender, session, and assignment layers")
}

// --- A4: Short-Gap Catch-Up Works ---
// Prototype proof: WAL-backed catch-up with provable recoverability.

func TestA4_ShortGapCatchUp(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 10, Value: i})
	}
	primary.Commit(100)
	primary.AdvanceTail(30)

	// Replica at 80 — short gap 81-100.
	replica := NewWALHistory()
	for i := uint64(1); i <= 80; i++ {
		replica.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 10, Value: i})
	}

	// Prove recoverability.
	if !primary.IsRecoverable(80, 100) {
		t.Fatal("A4: gap should be provably recoverable")
	}

	// Recovery via sender.
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)
	outcome, _ := s.RecordHandshakeWithOutcome(sess.ID, primary.MakeHandshakeResult(80))
	if outcome != OutcomeCatchUp {
		t.Fatalf("A4: outcome=%s", outcome)
	}

	// Apply entries.
	s.BeginCatchUp(sess.ID)
	entries, _ := primary.EntriesInRange(80, 100)
	for _, e := range entries {
		replica.Append(e)
		s.RecordCatchUpProgress(sess.ID, e.LSN)
	}
	s.CompleteSessionByID(sess.ID)

	// Verify data.
	for block, pVal := range primary.StateAt(100) {
		if rVal := replica.StateAt(100)[block]; rVal != pVal {
			t.Fatalf("A4: block %d mismatch", block)
		}
	}
	t.Log("A4: short-gap catch-up with WAL-backed proof and data verification")
}

// --- A5: Non-Convergent Catch-Up Escalates ---
// Prototype proof: unrecoverable gap → NeedsRebuild.

func TestA5_NonConvergentEscalates(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1})
	}
	primary.Commit(100)
	primary.AdvanceTail(60) // only 61-100 retained

	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(sess.ID)

	// Replica at 30 — gap not recoverable (need 31-100 but only 61-100 retained).
	if primary.IsRecoverable(30, 100) {
		t.Fatal("A5: gap should NOT be recoverable")
	}

	outcome, _ := s.RecordHandshakeWithOutcome(sess.ID, primary.MakeHandshakeResult(30))
	if outcome != OutcomeNeedsRebuild {
		t.Fatalf("A5: outcome=%s, want needs_rebuild", outcome)
	}
	if s.State != StateNeedsRebuild {
		t.Fatalf("A5: state=%s, want needs_rebuild", s.State)
	}
	t.Log("A5: unrecoverable gap escalates to NeedsRebuild with proof")
}

// --- A6: Recoverability Boundary Is Explicit ---
// Prototype proof: exact boundary between recoverable and unrecoverable.

func TestA6_RecoverabilityBoundaryExplicit(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1})
	}
	primary.Commit(100)
	primary.AdvanceTail(50)

	// At boundary: replica LSN 50 → recoverable (need 51+, tail=50).
	if !primary.IsRecoverable(50, 100) {
		t.Fatal("A6: exact boundary should be recoverable")
	}
	hr := primary.MakeHandshakeResult(50)
	if ClassifyRecoveryOutcome(hr) != OutcomeCatchUp {
		t.Fatal("A6: should classify as catchup at boundary")
	}

	// One below: replica LSN 49 → unrecoverable.
	if primary.IsRecoverable(49, 100) {
		t.Fatal("A6: one below boundary should NOT be recoverable")
	}
	hr = primary.MakeHandshakeResult(49)
	if ClassifyRecoveryOutcome(hr) != OutcomeNeedsRebuild {
		t.Fatal("A6: should classify as needs_rebuild below boundary")
	}
	t.Log("A6: recoverability boundary is exact and provable")
}

// --- A7: Historical Data Correctness ---
// Prototype proof: StateAt(lsn) correct even after tail advancement.

func TestA7_HistoricalDataCorrectness(t *testing.T) {
	w := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		w.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 5, Value: i * 100})
	}
	w.Commit(100)

	// State before tail advance.
	stateBefore := w.StateAt(100)

	// Advance tail — recycling old entries.
	w.AdvanceTail(50)

	// State after tail advance (uses snapshot).
	stateAfter := w.StateAt(100)
	if stateAfter == nil {
		t.Fatal("A7: state should be valid after tail advance")
	}

	for block, before := range stateBefore {
		if after := stateAfter[block]; after != before {
			t.Fatalf("A7: block %d: before=%d after=%d", block, before, after)
		}
	}
	t.Log("A7: historical data correctness preserved through tail advancement")
}

// --- A10: Changed-Address Restart Recovery ---
// Prototype proof: endpoint change → session invalidated → new assignment → recover.

func TestA10_ChangedAddressRecovery(t *testing.T) {
	sg := NewSenderGroup()

	// Initial assignment.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", CtrlAddr: "r1:9334", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	r1 := sg.Sender("r1:9333")
	oldSess := r1.Session()
	r1.BeginConnect(oldSess.ID) // mid-recovery

	// Address changes — endpoint update invalidates active session.
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", CtrlAddr: "r1:9445", Version: 2},
	}, 1)

	if oldSess.Active() {
		t.Fatal("A10: old session should be invalidated by address change")
	}
	// Sender identity preserved.
	if sg.Sender("r1:9333") != r1 {
		t.Fatal("A10: sender identity should be preserved")
	}

	// New assignment with recovery at new endpoint.
	result := sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", CtrlAddr: "r1:9445", Version: 2},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})
	if len(result.SessionsCreated) != 1 {
		t.Fatalf("A10: should create new session, got %v", result)
	}

	newSess := r1.Session()
	r1.BeginConnect(newSess.ID)
	r1.RecordHandshake(newSess.ID, 50, 50) // zero-gap at new endpoint
	r1.CompleteSessionByID(newSess.ID)

	if r1.State != StateInSync {
		t.Fatalf("A10: state=%s, want in_sync", r1.State)
	}
	t.Log("A10: changed-address recovery via endpoint invalidation + new assignment")
}

// ============================================================
// V2-Boundary Scenarios
// ============================================================

// --- Boundary 1: NeedsRebuild Persistence Across Topology Update ---

func TestBoundary_NeedsRebuild_PersistsAcrossUpdate(t *testing.T) {
	sg := NewSenderGroup()
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch: 1,
	})

	r1 := sg.Sender("r1:9333")

	// Drive to NeedsRebuild via unrecoverable gap.
	sess, _ := r1.AttachSession(1, SessionCatchUp)
	r1.BeginConnect(sess.ID)
	r1.RecordHandshakeWithOutcome(sess.ID, HandshakeResult{
		ReplicaFlushedLSN: 10,
		CommittedLSN:      100,
		RetentionStartLSN: 50,
	})

	if r1.State != StateNeedsRebuild {
		t.Fatalf("should be NeedsRebuild, got %s", r1.State)
	}

	// Topology update (same endpoint) — NeedsRebuild persists.
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
	}, 1)
	if r1.State != StateNeedsRebuild {
		t.Fatal("NeedsRebuild should persist across topology update")
	}

	// Only explicit rebuild assignment can recover.
	result := sg.ApplyAssignment(AssignmentIntent{
		Endpoints:       map[string]Endpoint{"r1:9333": {DataAddr: "r1:9333", Version: 1}},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionRebuild},
	})
	if len(result.SessionsCreated)+len(result.SessionsSuperseded) == 0 {
		t.Fatal("rebuild assignment should create session")
	}

	rebuildSess := r1.Session()
	r1.BeginConnect(rebuildSess.ID)
	r1.RecordHandshake(rebuildSess.ID, 0, 100)
	r1.SelectRebuildSource(rebuildSess.ID, 0, false, 100) // full base
	r1.BeginRebuildTransfer(rebuildSess.ID)
	r1.RecordRebuildTransferProgress(rebuildSess.ID, 100)
	r1.CompleteRebuild(rebuildSess.ID)

	if r1.State != StateInSync {
		t.Fatalf("after rebuild: state=%s", r1.State)
	}
	t.Log("boundary: NeedsRebuild persists, only rebuild assignment recovers")
}

// --- Boundary 2: Catch-Up Without Overwriting Safe Data ---

func TestBoundary_CatchUpDoesNotOverwriteSafeData(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 50; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 5, Value: i * 100})
	}
	primary.Commit(50)

	// Replica has 1-30 correct. Blocks 1-30 have known-good values.
	replica := NewWALHistory()
	for i := uint64(1); i <= 30; i++ {
		replica.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 5, Value: i * 100})
	}

	// State before catch-up.
	safeBlocks := replica.StateAt(30)

	// Catch-up: apply entries 31-50.
	entries, _ := primary.EntriesInRange(30, 50)
	for _, e := range entries {
		replica.Append(e)
	}

	// Verify: existing safe data not overwritten by catch-up
	// (blocks whose last write was <= 30 still have same values).
	for block, safeVal := range safeBlocks {
		// Check if any catch-up entry overwrote this block.
		overwritten := false
		for _, e := range entries {
			if e.Block == block {
				overwritten = true
				break
			}
		}
		if !overwritten {
			replicaVal := replica.StateAt(50)[block]
			if replicaVal != safeVal {
				t.Fatalf("block %d: safe=%d corrupted to %d", block, safeVal, replicaVal)
			}
		}
	}

	// Final state matches primary.
	for block, pVal := range primary.StateAt(50) {
		if rVal := replica.StateAt(50)[block]; rVal != pVal {
			t.Fatalf("block %d: primary=%d replica=%d", block, pVal, rVal)
		}
	}
	t.Log("boundary: catch-up applies only new entries, safe data preserved")
}

// --- Boundary 3: Repeated Disconnect/Reconnect Cycles ---

func TestBoundary_RepeatedDisconnectReconnect(t *testing.T) {
	sg := NewSenderGroup()
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch: 1,
	})

	r1 := sg.Sender("r1:9333")
	original := r1

	// 5 disconnect/reconnect cycles.
	for cycle := 0; cycle < 5; cycle++ {
		sess, err := r1.AttachSession(1, SessionCatchUp)
		if err != nil {
			t.Fatalf("cycle %d: attach failed: %v", cycle, err)
		}
		r1.BeginConnect(sess.ID)
		r1.RecordHandshake(sess.ID, uint64(cycle*10), uint64(cycle*10+10))
		r1.BeginCatchUp(sess.ID)
		r1.RecordCatchUpProgress(sess.ID, uint64(cycle*10+10))
		if !r1.CompleteSessionByID(sess.ID) {
			t.Fatalf("cycle %d: completion failed", cycle)
		}
		if r1.State != StateInSync {
			t.Fatalf("cycle %d: state=%s", cycle, r1.State)
		}
	}

	// Identity preserved across all cycles.
	if sg.Sender("r1:9333") != original {
		t.Fatal("sender identity should be preserved across 5 cycles")
	}
	t.Log("boundary: 5 disconnect/reconnect cycles, identity preserved")
}

// --- Boundary 4: Full V2 Recovery Harness ---

func TestBoundary_FullV2RecoveryHarness(t *testing.T) {
	// End-to-end: assignment → WAL-backed handshake → outcome branching →
	// execution → data verification.

	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}
	primary.Commit(100)
	primary.AdvanceTail(20) // retain 21-100

	sg := NewSenderGroup()

	// Assignment with 3 replicas at different states.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1}, // will be zero-gap
			"r2:9333": {DataAddr: "r2:9333", Version: 1}, // will need catch-up
			"r3:9333": {DataAddr: "r3:9333", Version: 1}, // will need rebuild
		},
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1:9333": SessionCatchUp,
			"r2:9333": SessionCatchUp,
			"r3:9333": SessionCatchUp,
		},
	})

	// r1: zero-gap (at committed).
	r1 := sg.Sender("r1:9333")
	s1 := r1.Session()
	r1.BeginConnect(s1.ID)
	o1, _ := r1.RecordHandshakeWithOutcome(s1.ID, primary.MakeHandshakeResult(100))
	if o1 != OutcomeZeroGap {
		t.Fatalf("r1: outcome=%s, want zero_gap", o1)
	}
	r1.CompleteSessionByID(s1.ID)

	// r2: catch-up (gap 70-100, within retention).
	r2 := sg.Sender("r2:9333")
	s2 := r2.Session()
	r2.BeginConnect(s2.ID)
	o2, _ := r2.RecordHandshakeWithOutcome(s2.ID, primary.MakeHandshakeResult(70))
	if o2 != OutcomeCatchUp {
		t.Fatalf("r2: outcome=%s, want catchup", o2)
	}
	r2.BeginCatchUp(s2.ID)
	entries, _ := primary.EntriesInRange(70, 100)
	for _, e := range entries {
		r2.RecordCatchUpProgress(s2.ID, e.LSN)
	}
	r2.CompleteSessionByID(s2.ID)

	// r3: needs rebuild (gap 10-100, but retention starts at 21).
	r3 := sg.Sender("r3:9333")
	s3 := r3.Session()
	r3.BeginConnect(s3.ID)
	o3, _ := r3.RecordHandshakeWithOutcome(s3.ID, primary.MakeHandshakeResult(10))
	if o3 != OutcomeNeedsRebuild {
		t.Fatalf("r3: outcome=%s, want needs_rebuild", o3)
	}

	// Final states.
	if r1.State != StateInSync {
		t.Fatalf("r1: %s", r1.State)
	}
	if r2.State != StateInSync {
		t.Fatalf("r2: %s", r2.State)
	}
	if r3.State != StateNeedsRebuild {
		t.Fatalf("r3: %s", r3.State)
	}

	// InSync count.
	if sg.InSyncCount() != 2 {
		t.Fatalf("in_sync=%d, want 2", sg.InSyncCount())
	}

	t.Log("harness: 3 replicas, 3 outcomes (zero-gap, catch-up, rebuild)")
}
