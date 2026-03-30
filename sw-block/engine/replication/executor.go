package replication

import "fmt"

// === Phase 06 P2: Stepwise Executor ===
//
// Replaces CompleteCatchUp/CompleteRebuild convenience wrappers with
// explicit stepwise execution that owns resource lifecycle.
// All exit paths (success, failure, cancellation) release resources.

// CatchUpExecutor drives stepwise catch-up execution with resource lifecycle.
// Created from a RecoveryPlan. Releases all resources on every exit path.
type CatchUpExecutor struct {
	driver    *RecoveryDriver
	plan      *RecoveryPlan
	replicaID string
	sessID    uint64
	released  bool
}

// NewCatchUpExecutor creates an executor from a plan. The plan's resources
// are now owned by the executor — do not call ReleasePlan separately.
func NewCatchUpExecutor(driver *RecoveryDriver, plan *RecoveryPlan) *CatchUpExecutor {
	return &CatchUpExecutor{
		driver:    driver,
		plan:      plan,
		replicaID: plan.ReplicaID,
		sessID:    plan.SessionID,
	}
}

// Execute runs the full catch-up lifecycle stepwise:
//   1. BeginCatchUp (with startTick)
//   2. For each progress step: RecordCatchUpProgress + CheckBudget
//   3. RecordTruncation (if required)
//   4. CompleteSessionByID
//   5. Release resources
//
// On any failure or cancellation, resources are released before returning.
func (e *CatchUpExecutor) Execute(progressLSNs []uint64, startTick, completeTick uint64) error {
	s := e.driver.Orchestrator.Registry.Sender(e.replicaID)
	if s == nil {
		e.release("sender_not_found")
		return fmt.Errorf("sender %q not found", e.replicaID)
	}

	// Step 1: begin catch-up.
	if err := s.BeginCatchUp(e.sessID, startTick); err != nil {
		e.release(fmt.Sprintf("begin_catchup_failed: %s", err))
		return err
	}
	e.driver.Orchestrator.Log.Record(e.replicaID, e.sessID, "exec_catchup_started",
		fmt.Sprintf("steps=%d tick=%d", len(progressLSNs), startTick))

	// Step 2: stepwise progress.
	for i, lsn := range progressLSNs {
		// Check if session was invalidated (epoch bump / endpoint change).
		if !s.HasActiveSession() || s.SessionID() != e.sessID {
			e.release("session_invalidated_mid_execution")
			return fmt.Errorf("session invalidated during catch-up step %d", i)
		}

		tick := startTick + uint64(i+1)
		if err := s.RecordCatchUpProgress(e.sessID, lsn, tick); err != nil {
			e.release(fmt.Sprintf("progress_failed_step_%d: %s", i, err))
			return err
		}

		// Check budget after each step.
		v, err := s.CheckBudget(e.sessID, tick)
		if err != nil {
			e.release(fmt.Sprintf("budget_check_failed: %s", err))
			return err
		}
		if v != BudgetOK {
			e.release(fmt.Sprintf("budget_escalated: %s", v))
			return fmt.Errorf("budget violation at step %d: %s", i, v)
		}
	}

	// Step 3: truncation (if required).
	if e.plan.TruncateLSN > 0 {
		if err := s.RecordTruncation(e.sessID, e.plan.TruncateLSN); err != nil {
			e.release(fmt.Sprintf("truncation_failed: %s", err))
			return err
		}
		e.driver.Orchestrator.Log.Record(e.replicaID, e.sessID, "exec_truncation",
			fmt.Sprintf("truncated_to=%d", e.plan.TruncateLSN))
	}

	// Step 4: complete.
	if !s.CompleteSessionByID(e.sessID) {
		e.release("completion_rejected")
		return fmt.Errorf("completion rejected")
	}

	// Step 5: release resources on success.
	e.release("")
	e.driver.Orchestrator.Log.Record(e.replicaID, e.sessID, "exec_completed", "in_sync")
	return nil
}

// Cancel aborts the executor and releases all resources.
func (e *CatchUpExecutor) Cancel(reason string) {
	e.release(fmt.Sprintf("cancelled: %s", reason))
}

func (e *CatchUpExecutor) release(reason string) {
	if e.released {
		return
	}
	e.released = true
	e.driver.ReleasePlan(e.plan)
	if reason != "" {
		e.driver.Orchestrator.Log.Record(e.replicaID, e.sessID, "exec_resources_released", reason)
	}
}

// RebuildExecutor drives stepwise rebuild execution with resource lifecycle.
type RebuildExecutor struct {
	driver    *RecoveryDriver
	plan      *RecoveryPlan
	replicaID string
	sessID    uint64
	released  bool
}

// NewRebuildExecutor creates a rebuild executor from a plan.
func NewRebuildExecutor(driver *RecoveryDriver, plan *RecoveryPlan) *RebuildExecutor {
	return &RebuildExecutor{
		driver:    driver,
		plan:      plan,
		replicaID: plan.ReplicaID,
		sessID:    plan.SessionID,
	}
}

// Execute runs the full rebuild lifecycle stepwise:
//   1. BeginConnect + RecordHandshake
//   2. SelectRebuildFromHistory
//   3. BeginRebuildTransfer + progress steps
//   4. BeginRebuildTailReplay + progress steps (snapshot+tail only)
//   5. CompleteRebuild
//   6. Release resources
func (e *RebuildExecutor) Execute(history *RetainedHistory) error {
	s := e.driver.Orchestrator.Registry.Sender(e.replicaID)
	if s == nil {
		e.release("sender_not_found")
		return fmt.Errorf("sender %q not found", e.replicaID)
	}

	// Step 1: connect + handshake.
	if err := s.BeginConnect(e.sessID); err != nil {
		e.release(fmt.Sprintf("connect_failed: %s", err))
		return err
	}
	if err := s.RecordHandshake(e.sessID, 0, history.CommittedLSN); err != nil {
		e.release(fmt.Sprintf("handshake_failed: %s", err))
		return err
	}

	// Step 2: select source from history.
	if err := s.SelectRebuildFromHistory(e.sessID, history); err != nil {
		e.release(fmt.Sprintf("source_select_failed: %s", err))
		return err
	}

	source, snapLSN := history.RebuildSourceDecision()
	e.driver.Orchestrator.Log.Record(e.replicaID, e.sessID, "exec_rebuild_started",
		fmt.Sprintf("source=%s", source))

	// Step 3: transfer.
	if err := s.BeginRebuildTransfer(e.sessID); err != nil {
		e.release(fmt.Sprintf("transfer_failed: %s", err))
		return err
	}

	if source == RebuildSnapshotTail {
		// Transfer snapshot base.
		if err := s.RecordRebuildTransferProgress(e.sessID, snapLSN); err != nil {
			e.release(fmt.Sprintf("transfer_progress_failed: %s", err))
			return err
		}

		// Check invalidation before tail replay.
		if !s.HasActiveSession() || s.SessionID() != e.sessID {
			e.release("session_invalidated_mid_rebuild")
			return fmt.Errorf("session invalidated during rebuild")
		}

		// Step 4: tail replay.
		if err := s.BeginRebuildTailReplay(e.sessID); err != nil {
			e.release(fmt.Sprintf("tail_replay_failed: %s", err))
			return err
		}
		if err := s.RecordRebuildTailProgress(e.sessID, history.CommittedLSN); err != nil {
			e.release(fmt.Sprintf("tail_progress_failed: %s", err))
			return err
		}
	} else {
		// Full base transfer.
		if err := s.RecordRebuildTransferProgress(e.sessID, history.CommittedLSN); err != nil {
			e.release(fmt.Sprintf("transfer_progress_failed: %s", err))
			return err
		}
	}

	// Step 5: complete.
	if err := s.CompleteRebuild(e.sessID); err != nil {
		e.release(fmt.Sprintf("rebuild_completion_failed: %s", err))
		return err
	}

	// Step 6: release on success.
	e.release("")
	e.driver.Orchestrator.Log.Record(e.replicaID, e.sessID, "exec_rebuild_completed", "in_sync")
	return nil
}

// Cancel aborts the rebuild executor and releases all resources.
func (e *RebuildExecutor) Cancel(reason string) {
	e.release(fmt.Sprintf("cancelled: %s", reason))
}

func (e *RebuildExecutor) release(reason string) {
	if e.released {
		return
	}
	e.released = true
	e.driver.ReleasePlan(e.plan)
	if reason != "" {
		e.driver.Orchestrator.Log.Record(e.replicaID, e.sessID, "exec_resources_released", reason)
	}
}
