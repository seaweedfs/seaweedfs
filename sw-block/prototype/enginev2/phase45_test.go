package enginev2

import "testing"

// ============================================================
// Phase 4.5 P0: Bounded CatchUp + Rebuild mode state machine
// ============================================================

// --- CatchUp Budget ---

func TestBudget_DurationExceeded(t *testing.T) {
	b := &CatchUpBudget{MaxDurationTicks: 10}
	tracker := BudgetCheck{StartTick: 0}

	if v := b.Check(tracker, 5); v != BudgetOK {
		t.Fatalf("tick 5: %s", v)
	}
	if v := b.Check(tracker, 11); v != BudgetDurationExceeded {
		t.Fatalf("tick 11: got %s, want duration_exceeded", v)
	}
}

func TestBudget_EntriesExceeded(t *testing.T) {
	b := &CatchUpBudget{MaxEntries: 100}
	tracker := BudgetCheck{EntriesReplayed: 50}

	if v := b.Check(tracker, 0); v != BudgetOK {
		t.Fatalf("50 entries: %s", v)
	}
	tracker.EntriesReplayed = 101
	if v := b.Check(tracker, 0); v != BudgetEntriesExceeded {
		t.Fatalf("101 entries: got %s, want entries_exceeded", v)
	}
}

func TestBudget_ProgressStalled(t *testing.T) {
	b := &CatchUpBudget{ProgressDeadlineTicks: 5}
	tracker := BudgetCheck{LastProgressTick: 10}

	if v := b.Check(tracker, 14); v != BudgetOK {
		t.Fatalf("tick 14: %s", v)
	}
	if v := b.Check(tracker, 16); v != BudgetProgressStalled {
		t.Fatalf("tick 16: got %s, want progress_stalled", v)
	}
}

func TestBudget_NoBudget_AlwaysOK(t *testing.T) {
	b := &CatchUpBudget{} // all zeros = no limits
	tracker := BudgetCheck{EntriesReplayed: 999999, StartTick: 0}
	if v := b.Check(tracker, 999999); v != BudgetOK {
		t.Fatalf("no budget: %s", v)
	}
}

// --- Sender budget integration ---

func TestSender_Budget_EscalatesOnViolation(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)

	// Set budget.
	sess.Budget = &CatchUpBudget{MaxDurationTicks: 10}

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.BeginCatchUp(sess.ID, 0) // start at tick 0

	s.RecordCatchUpProgress(sess.ID, 10)

	// Within budget at tick 5.
	v, _ := s.CheckBudget(sess.ID, 5)
	if v != BudgetOK {
		t.Fatalf("tick 5: %s", v)
	}

	// Exceeded at tick 11.
	v, _ = s.CheckBudget(sess.ID, 11)
	if v != BudgetDurationExceeded {
		t.Fatalf("tick 11: got %s, want duration_exceeded", v)
	}

	// Session invalidated, sender at NeedsRebuild.
	if sess.Active() {
		t.Fatal("session should be invalidated after budget violation")
	}
	if s.State != StateNeedsRebuild {
		t.Fatalf("state=%s, want needs_rebuild", s.State)
	}
}

func TestSender_Budget_ProgressStall_Escalates(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	sess.Budget = &CatchUpBudget{ProgressDeadlineTicks: 5}

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.BeginCatchUp(sess.ID, 10)

	s.RecordCatchUpProgressAt(sess.ID, 20, 12) // progress at tick 12

	// Stalled: no progress for 6 ticks (12→18 > deadline 5).
	v, _ := s.CheckBudget(sess.ID, 18)
	if v != BudgetProgressStalled {
		t.Fatalf("tick 18: got %s, want progress_stalled", v)
	}
	if s.State != StateNeedsRebuild {
		t.Fatalf("state=%s", s.State)
	}
}

func TestSender_NoBudget_NeverEscalates(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	// No budget set.

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.BeginCatchUp(sess.ID, 0)

	v, _ := s.CheckBudget(sess.ID, 999999)
	if v != BudgetOK {
		t.Fatalf("no budget: %s", v)
	}
	if s.State != StateCatchingUp {
		t.Fatalf("state=%s", s.State)
	}
}

// --- Rebuild state machine ---

func TestRebuild_SnapshotTail_FullLifecycle(t *testing.T) {
	rs := NewRebuildState()

	// Source select: snapshot available.
	rs.SelectSource(50, true, 100)
	if rs.Source != RebuildSnapshotTail {
		t.Fatalf("source=%s, want snapshot_tail", rs.Source)
	}
	if rs.TailStartLSN != 50 || rs.TailTargetLSN != 100 {
		t.Fatalf("tail: start=%d target=%d", rs.TailStartLSN, rs.TailTargetLSN)
	}

	// Transfer base (snapshot copy).
	rs.BeginTransfer()
	rs.RecordTransferProgress(50)

	// Tail replay.
	rs.BeginTailReplay()
	rs.RecordTailReplayProgress(75)
	rs.RecordTailReplayProgress(100)

	if !rs.ReadyToComplete() {
		t.Fatal("should be ready to complete")
	}
	rs.Complete()
	if rs.Phase != RebuildPhaseCompleted {
		t.Fatalf("phase=%s", rs.Phase)
	}
}

func TestRebuild_FullBase_Lifecycle(t *testing.T) {
	rs := NewRebuildState()

	// No valid snapshot.
	rs.SelectSource(0, false, 100)
	if rs.Source != RebuildFullBase {
		t.Fatalf("source=%s, want full_base", rs.Source)
	}

	rs.BeginTransfer()
	rs.RecordTransferProgress(50)
	rs.RecordTransferProgress(100)

	// Full base: no tail replay needed.
	if err := rs.BeginTailReplay(); err == nil {
		t.Fatal("full base should not allow tail replay")
	}

	if !rs.ReadyToComplete() {
		t.Fatal("should be ready to complete")
	}
	rs.Complete()
}

func TestRebuild_Abort(t *testing.T) {
	rs := NewRebuildState()
	rs.SelectSource(50, true, 100)
	rs.BeginTransfer()

	rs.Abort("epoch_bump")
	if rs.Phase != RebuildPhaseAborted {
		t.Fatalf("phase=%s", rs.Phase)
	}
	if rs.AbortReason != "epoch_bump" {
		t.Fatalf("reason=%s", rs.AbortReason)
	}

	// Cannot complete after abort.
	if err := rs.Complete(); err == nil {
		t.Fatal("complete after abort should fail")
	}
}

func TestRebuild_PhaseOrderEnforced(t *testing.T) {
	rs := NewRebuildState()

	// Cannot transfer before source select.
	if err := rs.BeginTransfer(); err == nil {
		t.Fatal("transfer before source select should fail")
	}

	rs.SelectSource(50, true, 100)

	// Cannot tail replay before transfer.
	if err := rs.BeginTailReplay(); err == nil {
		t.Fatal("tail replay before transfer should fail")
	}

	rs.BeginTransfer()

	// Cannot complete before reaching target.
	if rs.ReadyToComplete() {
		t.Fatal("should not be ready before reaching target")
	}
}

func TestRebuild_TransferRegression_Rejected(t *testing.T) {
	rs := NewRebuildState()
	rs.SelectSource(0, false, 100)
	rs.BeginTransfer()
	rs.RecordTransferProgress(50)

	if err := rs.RecordTransferProgress(30); err == nil {
		t.Fatal("transfer regression should be rejected")
	}
}

// --- E2E: Bounded catch-up → budget exceeded → rebuild ---

func TestE2E_BoundedCatchUp_EscalatesToRebuild(t *testing.T) {
	primary := NewWALHistory()
	for i := uint64(1); i <= 100; i++ {
		primary.Append(WALEntry{LSN: i, Epoch: 1, Block: i % 8, Value: i})
	}
	primary.Commit(100)

	sg := NewSenderGroup()
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionCatchUp},
	})

	r1 := sg.Sender("r1:9333")
	sess := r1.Session()

	// Set tight budget: max 20 entries.
	sess.Budget = &CatchUpBudget{MaxEntries: 20}

	r1.BeginConnect(sess.ID)
	outcome, _ := r1.RecordHandshakeWithOutcome(sess.ID, primary.MakeHandshakeResult(50))
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", outcome)
	}

	r1.BeginCatchUp(sess.ID, 0)

	// Replay 21 entries — exceeds budget.
	entries, _ := primary.EntriesInRange(50, 71)
	for _, e := range entries {
		r1.RecordCatchUpProgress(sess.ID, e.LSN)
	}

	v, _ := r1.CheckBudget(sess.ID, 0)
	if v != BudgetEntriesExceeded {
		t.Fatalf("budget: %s", v)
	}
	if r1.State != StateNeedsRebuild {
		t.Fatalf("state=%s", r1.State)
	}

	// New rebuild assignment.
	sg.ApplyAssignment(AssignmentIntent{
		Endpoints: map[string]Endpoint{
			"r1:9333": {DataAddr: "r1:9333", Version: 1},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1:9333": SessionRebuild},
	})

	rebuildSess := r1.Session()
	r1.BeginConnect(rebuildSess.ID)
	r1.RecordHandshake(rebuildSess.ID, 0, 100)
	r1.BeginCatchUp(rebuildSess.ID)
	r1.RecordCatchUpProgress(rebuildSess.ID, 100)
	r1.CompleteSessionByID(rebuildSess.ID)

	if r1.State != StateInSync {
		t.Fatalf("after rebuild: state=%s", r1.State)
	}
	t.Logf("bounded catch-up → budget exceeded → rebuild → InSync")
}
