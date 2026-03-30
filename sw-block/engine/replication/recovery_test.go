package replication

import "testing"

// ============================================================
// Phase 05 Slice 2: Engine Recovery Execution Core
//
// Tests validate the connect → handshake → outcome → execution → complete
// flow at the engine level, including bounded catch-up, stale rejection
// during active recovery, and rebuild exclusivity.
// ============================================================

// --- Zero-gap / catch-up / needs-rebuild branching ---

func TestRecovery_ZeroGap_FastComplete(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(id)
	outcome, _ := s.RecordHandshakeWithOutcome(id, HandshakeResult{
		ReplicaFlushedLSN: 100, CommittedLSN: 100, RetentionStartLSN: 50,
	})

	if outcome != OutcomeZeroGap {
		t.Fatalf("outcome=%s", outcome)
	}
	if !s.CompleteSessionByID(id) {
		t.Fatal("zero-gap fast completion should succeed")
	}
	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

func TestRecovery_CatchUp_FullExecution(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(id)
	outcome, _ := s.RecordHandshakeWithOutcome(id, HandshakeResult{
		ReplicaFlushedLSN: 70, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", outcome)
	}

	s.BeginCatchUp(id)

	// Progress within frozen target.
	s.RecordCatchUpProgress(id, 85)
	s.RecordCatchUpProgress(id, 100) // converged

	if !s.CompleteSessionByID(id) {
		t.Fatal("catch-up completion should succeed")
	}
	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

func TestRecovery_NeedsRebuild_Escalation(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(id)
	outcome, _ := s.RecordHandshakeWithOutcome(id, HandshakeResult{
		ReplicaFlushedLSN: 10, CommittedLSN: 100, RetentionStartLSN: 50,
	})

	if outcome != OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s", outcome)
	}
	if s.State() != StateNeedsRebuild {
		t.Fatalf("state=%s", s.State())
	}
	if s.HasActiveSession() {
		t.Fatal("session should be invalidated")
	}
}

// --- Stale execution rejection during active recovery ---

func TestRecovery_StaleID_DuringCatchUp(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id1, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(id1)
	s.RecordHandshake(id1, 0, 100)
	s.BeginCatchUp(id1)
	s.RecordCatchUpProgress(id1, 30)

	// Epoch bumps mid-recovery → session invalidated.
	s.UpdateEpoch(2)

	// All further execution on old session rejected.
	if err := s.RecordCatchUpProgress(id1, 50); err == nil {
		t.Fatal("progress on stale session should be rejected")
	}
	if s.CompleteSessionByID(id1) {
		t.Fatal("completion on stale session should be rejected")
	}

	// New session at new epoch.
	id2, _ := s.AttachSession(2, SessionCatchUp)
	if err := s.BeginConnect(id2); err != nil {
		t.Fatalf("new session should work: %v", err)
	}
}

func TestRecovery_EndpointChange_DuringCatchUp(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.BeginCatchUp(id)

	// Endpoint changes mid-recovery.
	s.UpdateEndpoint(Endpoint{DataAddr: "r1:9444", Version: 2})

	if err := s.RecordCatchUpProgress(id, 50); err == nil {
		t.Fatal("progress after endpoint change should be rejected")
	}
	if s.State() != StateDisconnected {
		t.Fatalf("state=%s", s.State())
	}
}

// --- Bounded catch-up escalation ---

func TestRecovery_FrozenTarget_ExactBoundary(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 50)
	s.BeginCatchUp(id)

	// AT frozen target: OK.
	if err := s.RecordCatchUpProgress(id, 50); err != nil {
		t.Fatalf("at target: %v", err)
	}
	// BEYOND frozen target: rejected.
	if err := s.RecordCatchUpProgress(id, 51); err == nil {
		t.Fatal("beyond frozen target should be rejected")
	}
}

func TestRecovery_BudgetDuration_Escalates(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp, WithBudget(CatchUpBudget{MaxDurationTicks: 10}))

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.BeginCatchUp(id, 0)
	s.RecordCatchUpProgress(id, 10)

	v, _ := s.CheckBudget(id, 15) // 15 > 10
	if v != BudgetDurationExceeded {
		t.Fatalf("budget=%s", v)
	}
	if s.State() != StateNeedsRebuild {
		t.Fatalf("state=%s", s.State())
	}
}

func TestRecovery_BudgetEntries_Escalates(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp, WithBudget(CatchUpBudget{MaxEntries: 20}))

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.BeginCatchUp(id)

	// Replay 21 entries (LSN delta = 21).
	s.RecordCatchUpProgress(id, 21)

	v, _ := s.CheckBudget(id, 0)
	if v != BudgetEntriesExceeded {
		t.Fatalf("budget=%s", v)
	}
}

func TestRecovery_BudgetStall_Escalates(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp, WithBudget(CatchUpBudget{ProgressDeadlineTicks: 5}))

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.BeginCatchUp(id, 10)

	s.RecordCatchUpProgress(id, 20, 12) // progress at tick 12

	v, _ := s.CheckBudget(id, 18) // 18 - 12 = 6 > 5
	if v != BudgetProgressStalled {
		t.Fatalf("budget=%s", v)
	}
}

func TestRecovery_BudgetEntries_NonZeroStart_CountsOnlyDelta(t *testing.T) {
	// Replica starts at LSN 70. Catching up to 100 = 30 entries.
	// With MaxEntries=50, this should NOT exceed budget.
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp, WithBudget(CatchUpBudget{MaxEntries: 50}))

	s.BeginConnect(id)
	s.RecordHandshakeWithOutcome(id, HandshakeResult{
		ReplicaFlushedLSN: 70, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	s.BeginCatchUp(id)

	// Progress from 70 → 100 = 30 entries (not 100).
	s.RecordCatchUpProgress(id, 100)

	v, _ := s.CheckBudget(id, 0)
	if v != BudgetOK {
		t.Fatalf("30 entries should be within 50-entry budget, got %s", v)
	}

	// Verify: only 30 entries counted, not 100.
	snap := s.SessionSnapshot()
	if snap == nil {
		t.Fatal("session should exist")
	}
}

func TestRecovery_BudgetEntries_NonZeroStart_ExceedsBudget(t *testing.T) {
	// Replica starts at LSN 70. Catching up to 100 = 30 entries.
	// With MaxEntries=20, this SHOULD exceed budget.
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp, WithBudget(CatchUpBudget{MaxEntries: 20}))

	s.BeginConnect(id)
	s.RecordHandshakeWithOutcome(id, HandshakeResult{
		ReplicaFlushedLSN: 70, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	s.BeginCatchUp(id)
	s.RecordCatchUpProgress(id, 100) // 30 entries > 20 limit

	v, _ := s.CheckBudget(id, 0)
	if v != BudgetEntriesExceeded {
		t.Fatalf("30 entries should exceed 20-entry budget, got %s", v)
	}
}

func TestRecovery_Rebuild_PartialTransfer_BlocksTailReplay(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionRebuild)

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.SelectRebuildSource(id, 50, true, 100) // snapshot at LSN 50

	s.BeginRebuildTransfer(id)
	s.RecordRebuildTransferProgress(id, 30) // only transferred to 30 < snapshot 50

	// Tail replay should be blocked: base transfer incomplete.
	if err := s.BeginRebuildTailReplay(id); err == nil {
		t.Fatal("tail replay should be blocked when transfer < snapshotLSN")
	}

	// Complete transfer to snapshot LSN.
	s.RecordRebuildTransferProgress(id, 50)

	// Now tail replay allowed.
	if err := s.BeginRebuildTailReplay(id); err != nil {
		t.Fatalf("tail replay should work after full transfer: %v", err)
	}
}

func TestRecovery_CompletionBeforeConvergence_Rejected(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp)

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)
	s.BeginCatchUp(id)
	s.RecordCatchUpProgress(id, 50) // not converged

	if s.CompleteSessionByID(id) {
		t.Fatal("completion before convergence should be rejected")
	}
}

// --- Rebuild exclusivity ---

func TestRecovery_Rebuild_CatchUpAPIsExcluded(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionRebuild)
	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)

	if err := s.BeginCatchUp(id); err == nil {
		t.Fatal("rebuild: BeginCatchUp should be rejected")
	}
	if err := s.RecordCatchUpProgress(id, 50); err == nil {
		t.Fatal("rebuild: RecordCatchUpProgress should be rejected")
	}
	if s.CompleteSessionByID(id) {
		t.Fatal("rebuild: catch-up completion should be rejected")
	}
}

func TestRecovery_Rebuild_SnapshotTailLifecycle(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionRebuild)

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)

	s.SelectRebuildSource(id, 50, true, 100) // snapshot+tail
	s.BeginRebuildTransfer(id)
	s.RecordRebuildTransferProgress(id, 50)
	s.BeginRebuildTailReplay(id)
	s.RecordRebuildTailProgress(id, 100)

	if err := s.CompleteRebuild(id); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

func TestRecovery_Rebuild_FullBaseLifecycle(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionRebuild)

	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)

	s.SelectRebuildSource(id, 0, false, 100) // full base (no snapshot)
	s.BeginRebuildTransfer(id)
	s.RecordRebuildTransferProgress(id, 100)

	if err := s.CompleteRebuild(id); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

func TestRecovery_Rebuild_StaleIDRejected(t *testing.T) {
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id1, _ := s.AttachSession(1, SessionRebuild)

	s.UpdateEpoch(2)
	s.AttachSession(2, SessionRebuild)

	if err := s.SelectRebuildSource(id1, 50, true, 100); err == nil {
		t.Fatal("stale ID should be rejected in rebuild path")
	}
}

// --- Assignment-driven recovery ---

func TestRecovery_AssignmentDrivenFlow(t *testing.T) {
	r := NewRegistry()

	// Initial assignment with recovery intent.
	result := r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", Version: 1}},
			{ReplicaID: "r2", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", Version: 1}},
		},
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1": SessionCatchUp,
		},
	})

	if len(result.Added) != 2 {
		t.Fatalf("added=%d", len(result.Added))
	}
	if len(result.SessionsCreated) != 1 {
		t.Fatalf("sessions=%d", len(result.SessionsCreated))
	}

	// r1 has session, r2 does not.
	if !r.Sender("r1").HasActiveSession() {
		t.Fatal("r1 should have session")
	}
	if r.Sender("r2").HasActiveSession() {
		t.Fatal("r2 should not have session")
	}

	// Execute r1 recovery.
	r1 := r.Sender("r1")
	id := r1.SessionID()
	r1.BeginConnect(id)
	r1.RecordHandshakeWithOutcome(id, HandshakeResult{
		ReplicaFlushedLSN: 80, CommittedLSN: 100, RetentionStartLSN: 50,
	})
	r1.BeginCatchUp(id)
	r1.RecordCatchUpProgress(id, 100)
	r1.CompleteSessionByID(id)

	if r1.State() != StateInSync {
		t.Fatalf("r1: state=%s", r1.State())
	}
}
