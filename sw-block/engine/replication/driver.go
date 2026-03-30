package replication

import "fmt"

// === Phase 06: Execution Driver ===
//
// Convenience flow classification (Phase 06 P0):
//
//   ProcessAssignment      → stepwise engine task (real entry point)
//   ExecuteRecovery        → planner (connect + classify outcome)
//   CompleteCatchUp        → TEST-ONLY convenience (bundles plan+execute+complete)
//   CompleteRebuild        → TEST-ONLY convenience (bundles plan+execute+complete)
//   UpdateSenderEpoch      → stepwise engine task
//   InvalidateEpoch        → stepwise engine task
//
// The real engine flow splits catch-up and rebuild into:
//   1. Plan: acquire resources (pin WAL or snapshot)
//   2. Execute: stream entries stepwise (not one-shot)
//   3. Complete: release resources, transition to InSync
//
// RecoveryDriver is the Phase 06 replacement for the synchronous
// convenience helpers. It plans, acquires resources, and provides
// a stepwise execution interface.

// RecoveryPlan represents a planned recovery operation with acquired resources.
type RecoveryPlan struct {
	ReplicaID    string
	SessionID    uint64
	Outcome      RecoveryOutcome
	Proof        *RecoverabilityProof

	// Resource pins (non-nil when resources are acquired).
	RetentionPin *RetentionPin // for catch-up or snapshot+tail rebuild
	SnapshotPin  *SnapshotPin  // for snapshot+tail rebuild
	FullBasePin  *FullBasePin  // for full-base rebuild

	// Targets.
	CatchUpTarget uint64 // for catch-up: target LSN
	TruncateLSN   uint64 // non-zero if truncation required
	RebuildSource RebuildSource
}

// RecoveryDriver plans and executes recovery operations using real
// storage adapter inputs. It replaces the synchronous convenience
// helpers (CompleteCatchUp, CompleteRebuild) with a resource-aware,
// stepwise execution model.
type RecoveryDriver struct {
	Orchestrator *RecoveryOrchestrator
	Storage      StorageAdapter
}

// NewRecoveryDriver creates a driver with a fresh orchestrator.
func NewRecoveryDriver(storage StorageAdapter) *RecoveryDriver {
	return &RecoveryDriver{
		Orchestrator: NewRecoveryOrchestrator(),
		Storage:      storage,
	}
}

// PlanRecovery connects, handshakes from real storage state, classifies
// the outcome, and acquires the necessary resources (WAL pin or snapshot pin).
// Returns a RecoveryPlan that the caller can execute stepwise.
func (d *RecoveryDriver) PlanRecovery(replicaID string, replicaFlushedLSN uint64) (*RecoveryPlan, error) {
	history := d.Storage.GetRetainedHistory()

	result := d.Orchestrator.ExecuteRecovery(replicaID, replicaFlushedLSN, &history)
	if result.Error != nil {
		return nil, result.Error
	}

	s := d.Orchestrator.Registry.Sender(replicaID)
	plan := &RecoveryPlan{
		ReplicaID: replicaID,
		SessionID: s.SessionID(),
		Outcome:   result.Outcome,
		Proof:     result.Proof,
	}

	switch result.Outcome {
	case OutcomeZeroGap:
		// Already completed by ExecuteRecovery.
		return plan, nil

	case OutcomeCatchUp:
		// Acquire WAL retention pin BEFORE continuing execution.
		// If pin fails, invalidate the session to avoid a dangling live session.
		pin, err := d.Storage.PinWALRetention(replicaFlushedLSN)
		if err != nil {
			s.InvalidateSession("wal_pin_failed", StateDisconnected)
			d.Orchestrator.Log.Record(replicaID, plan.SessionID, "wal_pin_failed", err.Error())
			return nil, fmt.Errorf("WAL retention pin failed: %w", err)
		}
		plan.RetentionPin = &pin
		plan.CatchUpTarget = history.CommittedLSN

		// Check if truncation is needed (replica ahead).
		proof := history.ProveRecoverability(replicaFlushedLSN)
		if proof.Reason == "replica_ahead_needs_truncation" {
			plan.TruncateLSN = history.CommittedLSN
		}

		d.Orchestrator.Log.Record(replicaID, plan.SessionID, "plan_catchup",
			fmt.Sprintf("target=%d pin=%d truncate=%d", plan.CatchUpTarget, pin.PinID, plan.TruncateLSN))
		return plan, nil

	case OutcomeNeedsRebuild:
		// No resource acquisition — needs rebuild assignment first.
		return plan, nil
	}

	return plan, nil
}

// PlanRebuild acquires rebuild resources (snapshot pin + optional WAL pin)
// from real storage state. Called after a rebuild assignment.
func (d *RecoveryDriver) PlanRebuild(replicaID string) (*RecoveryPlan, error) {
	history := d.Storage.GetRetainedHistory()
	source, snapLSN := history.RebuildSourceDecision()

	plan := &RecoveryPlan{
		ReplicaID:     replicaID,
		SessionID:     d.Orchestrator.Registry.Sender(replicaID).SessionID(),
		Outcome:       OutcomeNeedsRebuild,
		RebuildSource: source,
	}

	if source == RebuildSnapshotTail {
		// Pin snapshot.
		snapPin, err := d.Storage.PinSnapshot(snapLSN)
		if err != nil {
			d.Orchestrator.Log.Record(replicaID, plan.SessionID, "snapshot_pin_failed", err.Error())
			return nil, fmt.Errorf("snapshot pin failed: %w", err)
		}
		plan.SnapshotPin = &snapPin

		// Pin WAL retention for tail replay.
		retPin, err := d.Storage.PinWALRetention(snapLSN)
		if err != nil {
			d.Storage.ReleaseSnapshot(snapPin)
			d.Orchestrator.Log.Record(replicaID, plan.SessionID, "wal_pin_failed", err.Error())
			return nil, fmt.Errorf("WAL retention pin failed: %w", err)
		}
		plan.RetentionPin = &retPin

		d.Orchestrator.Log.Record(replicaID, plan.SessionID, "plan_rebuild_snapshot_tail",
			fmt.Sprintf("snapshot=%d", snapLSN))
	} else {
		// Full-base rebuild: pin a consistent full-extent base image.
		basePin, err := d.Storage.PinFullBase(history.CommittedLSN)
		if err != nil {
			d.Orchestrator.Log.Record(replicaID, plan.SessionID, "full_base_pin_failed", err.Error())
			return nil, fmt.Errorf("full base pin failed: %w", err)
		}
		plan.FullBasePin = &basePin

		d.Orchestrator.Log.Record(replicaID, plan.SessionID, "plan_rebuild_full_base",
			fmt.Sprintf("committed=%d", history.CommittedLSN))
	}

	return plan, nil
}

// ReleasePlan releases any resources acquired by a plan.
func (d *RecoveryDriver) ReleasePlan(plan *RecoveryPlan) {
	if plan.RetentionPin != nil {
		d.Storage.ReleaseWALRetention(*plan.RetentionPin)
		plan.RetentionPin = nil
	}
	if plan.SnapshotPin != nil {
		d.Storage.ReleaseSnapshot(*plan.SnapshotPin)
		plan.SnapshotPin = nil
	}
	if plan.FullBasePin != nil {
		d.Storage.ReleaseFullBase(*plan.FullBasePin)
		plan.FullBasePin = nil
	}
}
