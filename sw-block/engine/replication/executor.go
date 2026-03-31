package replication

import "fmt"

// === Phase 06 P2 / Phase 08 P2: Stepwise Executor ===

// CatchUpIO is the I/O interface that the catch-up executor calls to
// perform real WAL streaming. Implemented by the v2bridge executor.
// The engine defines this interface; it does NOT import weed/.
type CatchUpIO interface {
	// StreamWALEntries reads WAL entries from startExclusive+1 to endInclusive.
	// Returns the highest LSN successfully transferred.
	StreamWALEntries(startExclusive, endInclusive uint64) (transferredTo uint64, err error)
}

// RebuildIO is the I/O interface that the rebuild executor calls to
// perform real data transfer. Implemented by the v2bridge executor.
type RebuildIO interface {
	// TransferFullBase transfers the full extent image at committedLSN.
	TransferFullBase(committedLSN uint64) error
	// TransferSnapshot transfers a checkpoint/snapshot at snapshotLSN.
	TransferSnapshot(snapshotLSN uint64) error
	// StreamWALEntries for tail replay after snapshot transfer.
	StreamWALEntries(startExclusive, endInclusive uint64) (transferredTo uint64, err error)
}

// CatchUpExecutor drives stepwise catch-up execution with resource lifecycle.
// When IO is set, the executor calls real bridge I/O for WAL streaming.
// When IO is nil, the executor uses caller-supplied progress LSNs (test mode).
type CatchUpExecutor struct {
	driver    *RecoveryDriver
	plan      *RecoveryPlan
	replicaID string
	sessID    uint64
	released  bool

	// IO performs real WAL streaming. If nil, uses progressLSNs from Execute().
	IO CatchUpIO

	// OnStep is an optional callback invoked between executor-managed steps.
	OnStep func(step int)
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
// progressLSNs are the stepwise LSN targets. startTick is the time when
// catch-up begins. Each step is assigned tick = startTick + stepIndex + 1.
// On any failure or cancellation, resources are released before returning.
func (e *CatchUpExecutor) Execute(progressLSNs []uint64, startTick uint64) error {
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
		fmt.Sprintf("target=%d tick=%d io=%v", e.plan.CatchUpTarget, startTick, e.IO != nil))

	// Step 2: progress — either via real IO bridge or caller-supplied LSNs.
	if e.IO != nil {
		// Real I/O path: stream WAL entries through the bridge.
		transferred, err := e.IO.StreamWALEntries(e.plan.CatchUpStartLSN, e.plan.CatchUpTarget)
		if err != nil {
			e.release(fmt.Sprintf("io_stream_failed: %s", err))
			return err
		}
		tick := startTick + 1
		if err := s.RecordCatchUpProgress(e.sessID, transferred, tick); err != nil {
			e.release(fmt.Sprintf("progress_after_io: %s", err))
			return err
		}
	} else {
		// Test path: caller-supplied progress LSNs.
		for i, lsn := range progressLSNs {
			if !s.HasActiveSession() || s.SessionID() != e.sessID {
				e.release("session_invalidated_mid_execution")
				return fmt.Errorf("session invalidated during catch-up step %d", i)
			}

			tick := startTick + uint64(i+1)
			if err := s.RecordCatchUpProgress(e.sessID, lsn, tick); err != nil {
				e.release(fmt.Sprintf("progress_failed_step_%d: %s", i, err))
				return err
			}

			if e.OnStep != nil {
				e.OnStep(i)
			}

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
// When IO is set, the executor calls real bridge I/O for data transfer.
// When IO is nil, the executor only advances sender/session state (test mode).
type RebuildExecutor struct {
	driver    *RecoveryDriver
	plan      *RecoveryPlan
	replicaID string
	sessID    uint64
	released  bool

	// IO performs real rebuild data transfer. If nil, only advances FSM (test mode).
	IO RebuildIO
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

// Execute runs the full rebuild lifecycle from the bound plan.
// Does NOT re-derive policy from caller-supplied history — uses
// plan.RebuildSource, plan.RebuildSnapshotLSN, plan.RebuildTargetLSN.
//
// Steps:
//   1. BeginConnect + RecordHandshake (target from plan)
//   2. SelectRebuildSource (source/snapshot from plan)
//   3. BeginRebuildTransfer + progress
//   4. BeginRebuildTailReplay + progress (snapshot+tail only)
//   5. CompleteRebuild
//   6. Release resources
func (e *RebuildExecutor) Execute() error {
	s := e.driver.Orchestrator.Registry.Sender(e.replicaID)
	if s == nil {
		e.release("sender_not_found")
		return fmt.Errorf("sender %q not found", e.replicaID)
	}

	plan := e.plan

	// Step 1: connect + handshake with plan-bound target.
	if err := s.BeginConnect(e.sessID); err != nil {
		e.release(fmt.Sprintf("connect_failed: %s", err))
		return err
	}
	if err := s.RecordHandshake(e.sessID, 0, plan.RebuildTargetLSN); err != nil {
		e.release(fmt.Sprintf("handshake_failed: %s", err))
		return err
	}

	// Step 2: select source from plan-bound values (not re-derived).
	valid := plan.RebuildSource == RebuildSnapshotTail
	if err := s.SelectRebuildSource(e.sessID, plan.RebuildSnapshotLSN, valid, plan.RebuildTargetLSN); err != nil {
		e.release(fmt.Sprintf("source_select_failed: %s", err))
		return err
	}

	e.driver.Orchestrator.Log.Record(e.replicaID, e.sessID, "exec_rebuild_started",
		fmt.Sprintf("source=%s target=%d io=%v", plan.RebuildSource, plan.RebuildTargetLSN, e.IO != nil))

	// Step 3: transfer — with real I/O if bridge is wired.
	if err := s.BeginRebuildTransfer(e.sessID); err != nil {
		e.release(fmt.Sprintf("transfer_failed: %s", err))
		return err
	}

	if plan.RebuildSource == RebuildSnapshotTail {
		// Real I/O: transfer snapshot through bridge.
		if e.IO != nil {
			if err := e.IO.TransferSnapshot(plan.RebuildSnapshotLSN); err != nil {
				e.release(fmt.Sprintf("io_snapshot_transfer_failed: %s", err))
				return err
			}
		}
		if err := s.RecordRebuildTransferProgress(e.sessID, plan.RebuildSnapshotLSN); err != nil {
			e.release(fmt.Sprintf("transfer_progress_failed: %s", err))
			return err
		}

		if !s.HasActiveSession() || s.SessionID() != e.sessID {
			e.release("session_invalidated_mid_rebuild")
			return fmt.Errorf("session invalidated during rebuild")
		}

		if err := s.BeginRebuildTailReplay(e.sessID); err != nil {
			e.release(fmt.Sprintf("tail_replay_failed: %s", err))
			return err
		}
		// Real I/O: stream WAL tail through bridge.
		if e.IO != nil {
			_, ioErr := e.IO.StreamWALEntries(plan.RebuildSnapshotLSN, plan.RebuildTargetLSN)
			if ioErr != nil {
				e.release(fmt.Sprintf("io_tail_replay_failed: %s", ioErr))
				return ioErr
			}
		}
		if err := s.RecordRebuildTailProgress(e.sessID, plan.RebuildTargetLSN); err != nil {
			e.release(fmt.Sprintf("tail_progress_failed: %s", err))
			return err
		}
	} else {
		// Real I/O: transfer full base through bridge.
		if e.IO != nil {
			if err := e.IO.TransferFullBase(plan.RebuildTargetLSN); err != nil {
				e.release(fmt.Sprintf("io_full_base_failed: %s", err))
				return err
			}
		}
		if err := s.RecordRebuildTransferProgress(e.sessID, plan.RebuildTargetLSN); err != nil {
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
