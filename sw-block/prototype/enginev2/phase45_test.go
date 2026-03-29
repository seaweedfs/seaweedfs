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

	s.RecordCatchUpProgress(sess.ID, 20, 12) // progress at tick 12

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

// --- Target-frozen enforcement ---

func TestSender_TargetFrozen_RejectsProgressBeyond(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	sess.Budget = &CatchUpBudget{} // budget present = target freezes

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 50, 100) // target = 100
	s.BeginCatchUp(sess.ID)

	// Budget.TargetLSNAtStart should be frozen to 100.
	if sess.Budget.TargetLSNAtStart != 100 {
		t.Fatalf("frozen target=%d, want 100", sess.Budget.TargetLSNAtStart)
	}

	// Progress within target works.
	if err := s.RecordCatchUpProgress(sess.ID, 80); err != nil {
		t.Fatalf("progress to 80: %v", err)
	}

	// Progress AT target works.
	if err := s.RecordCatchUpProgress(sess.ID, 100); err != nil {
		t.Fatalf("progress to 100: %v", err)
	}

	// Progress BEYOND frozen target — rejected.
	if err := s.RecordCatchUpProgress(sess.ID, 101); err == nil {
		t.Fatal("progress beyond frozen target should be rejected")
	}
}

func TestSender_NoBudget_TargetStillFrozen(t *testing.T) {
	// Target freeze is intrinsic to catch-up, not budget-dependent.
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 50, 100)
	s.BeginCatchUp(sess.ID)

	// Frozen target enforced even without budget.
	if sess.FrozenTargetLSN != 100 {
		t.Fatalf("frozen target=%d, want 100", sess.FrozenTargetLSN)
	}
	if err := s.RecordCatchUpProgress(sess.ID, 100); err != nil {
		t.Fatalf("at target: %v", err)
	}
	if err := s.RecordCatchUpProgress(sess.ID, 101); err == nil {
		t.Fatal("beyond frozen target should be rejected even without budget")
	}
}

// --- Sender-level rebuild test ---

func TestSender_RebuildViaRebuildAPIs(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionRebuild)

	if sess.Rebuild == nil {
		t.Fatal("rebuild session should auto-initialize RebuildState")
	}

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)

	// Select source via sender.
	if err := s.SelectRebuildSource(sess.ID, 40, true, 100); err != nil {
		t.Fatalf("select source: %v", err)
	}
	if sess.Rebuild.Source != RebuildSnapshotTail {
		t.Fatalf("source=%s", sess.Rebuild.Source)
	}

	// Transfer via sender.
	s.BeginRebuildTransfer(sess.ID)
	s.RecordRebuildTransferProgress(sess.ID, 40)

	// Tail replay via sender.
	s.BeginRebuildTailReplay(sess.ID)
	s.RecordRebuildTailProgress(sess.ID, 100)

	// Complete via sender.
	if err := s.CompleteRebuild(sess.ID); err != nil {
		t.Fatalf("complete rebuild: %v", err)
	}
	if s.State != StateInSync {
		t.Fatalf("state=%s, want in_sync", s.State)
	}
	if s.Session() != nil {
		t.Fatal("session should be nil after rebuild completion")
	}
}

func TestSender_RebuildAPIs_RejectNonRebuild(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp) // NOT rebuild
	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)

	if err := s.SelectRebuildSource(sess.ID, 40, true, 100); err == nil {
		t.Fatal("rebuild API should reject non-rebuild session")
	}
}

func TestSender_RebuildAPIs_RejectStaleID(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionRebuild)
	oldID := sess.ID
	s.UpdateEpoch(2)
	s.AttachSession(2, SessionRebuild)

	if err := s.SelectRebuildSource(oldID, 40, true, 100); err == nil {
		t.Fatal("stale sessionID should be rejected by rebuild APIs")
	}
}

// --- Rebuild exclusivity ---

func TestSender_RebuildExclusive_CatchUpAPIsRejected(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionRebuild)
	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)

	// BeginCatchUp rejects rebuild session.
	if err := s.BeginCatchUp(sess.ID); err == nil {
		t.Fatal("BeginCatchUp should reject rebuild session")
	}

	// RecordCatchUpProgress rejects rebuild session (even if we could somehow reach catchup phase).
	if err := s.RecordCatchUpProgress(sess.ID, 50); err == nil {
		t.Fatal("RecordCatchUpProgress should reject rebuild session")
	}

	// CompleteSessionByID rejects (no catch-up convergence possible for rebuild).
	if s.CompleteSessionByID(sess.ID) {
		t.Fatal("catch-up completion should not work for rebuild session")
	}
}

func TestSender_RebuildSourceSelect_RequiresHandshakePhase(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionRebuild)

	// Skip BeginConnect + RecordHandshake → should fail at SelectRebuildSource.
	if err := s.SelectRebuildSource(sess.ID, 40, true, 100); err == nil {
		t.Fatal("source select should require PhaseHandshake")
	}

	// After proper phase entry.
	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)

	// Now it works.
	if err := s.SelectRebuildSource(sess.ID, 40, true, 100); err != nil {
		t.Fatalf("source select after handshake: %v", err)
	}
}

func TestSender_StallBudget_RequiresTick(t *testing.T) {
	s := NewSender("r1:9333", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	sess, _ := s.AttachSession(1, SessionCatchUp)
	sess.Budget = &CatchUpBudget{ProgressDeadlineTicks: 5}

	s.BeginConnect(sess.ID)
	s.RecordHandshake(sess.ID, 0, 100)
	s.BeginCatchUp(sess.ID, 0)

	// Without tick, progress is rejected when stall budget is configured.
	if err := s.RecordCatchUpProgress(sess.ID, 10); err == nil {
		t.Fatal("progress without tick should be rejected when ProgressDeadlineTicks > 0")
	}

	// With tick, progress works.
	if err := s.RecordCatchUpProgress(sess.ID, 10, 1); err != nil {
		t.Fatalf("progress with tick: %v", err)
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
	if rebuildSess.Rebuild == nil {
		t.Fatal("rebuild session should have RebuildState initialized")
	}

	// Drive rebuild through the sender-owned rebuild path.
	r1.BeginConnect(rebuildSess.ID)
	r1.RecordHandshake(rebuildSess.ID, 0, 100)

	// Select snapshot+tail source.
	r1.SelectRebuildSource(rebuildSess.ID, 30, true, 100)
	r1.BeginRebuildTransfer(rebuildSess.ID)
	r1.RecordRebuildTransferProgress(rebuildSess.ID, 30)
	r1.BeginRebuildTailReplay(rebuildSess.ID)
	r1.RecordRebuildTailProgress(rebuildSess.ID, 100)

	if err := r1.CompleteRebuild(rebuildSess.ID); err != nil {
		t.Fatalf("rebuild completion: %v", err)
	}

	if r1.State != StateInSync {
		t.Fatalf("after rebuild: state=%s", r1.State)
	}
	t.Logf("bounded catch-up → budget exceeded → rebuild → InSync")
}
