package replication

import "fmt"

// RecoveryOrchestrator drives the recovery lifecycle from assignment intent
// through execution to completion/escalation. It is the integrated entry
// path above raw Sender APIs — callers interact with the orchestrator,
// not with individual sender execution methods.
//
// The orchestrator owns:
//   - assignment processing (reconcile + session creation)
//   - handshake evaluation (from RetainedHistory)
//   - recovery execution (catch-up or rebuild through completion)
//   - automatic event logging at every lifecycle transition
type RecoveryOrchestrator struct {
	Registry *Registry
	Log      *RecoveryLog
}

// NewRecoveryOrchestrator creates an orchestrator with a fresh registry and log.
func NewRecoveryOrchestrator() *RecoveryOrchestrator {
	return &RecoveryOrchestrator{
		Registry: NewRegistry(),
		Log:      NewRecoveryLog(),
	}
}

// ProcessAssignment applies an assignment intent and logs the result.
// Detects endpoint-change invalidations automatically.
func (o *RecoveryOrchestrator) ProcessAssignment(intent AssignmentIntent) AssignmentResult {
	// Snapshot pre-assignment session state for invalidation detection.
	type preState struct {
		hadSession bool
		sessionID  uint64
	}
	pre := map[string]preState{}
	for _, ra := range intent.Replicas {
		if s := o.Registry.Sender(ra.ReplicaID); s != nil {
			pre[ra.ReplicaID] = preState{
				hadSession: s.HasActiveSession(),
				sessionID:  s.SessionID(),
			}
		}
	}

	result := o.Registry.ApplyAssignment(intent)

	for _, id := range result.Added {
		o.Log.Record(id, 0, "sender_added", "")
	}
	for _, id := range result.Removed {
		o.Log.Record(id, 0, "sender_removed", "")
	}

	// Detect endpoint-change invalidations: if the session ID changed
	// (old session was invalidated and possibly replaced), log the old one.
	for id, p := range pre {
		if p.hadSession {
			s := o.Registry.Sender(id)
			if s != nil && s.SessionID() != p.sessionID {
				o.Log.Record(id, p.sessionID, "session_invalidated", "endpoint_changed")
			}
		}
	}

	for _, id := range result.SessionsCreated {
		s := o.Registry.Sender(id)
		o.Log.Record(id, s.SessionID(), "session_created", "")
	}
	for _, id := range result.SessionsSuperseded {
		s := o.Registry.Sender(id)
		o.Log.Record(id, s.SessionID(), "session_superseded", "")
	}
	for _, id := range result.SessionsFailed {
		o.Log.Record(id, 0, "session_failed", "")
	}
	return result
}

// RecoveryResult captures the outcome of a single replica recovery attempt.
type RecoveryResult struct {
	ReplicaID string
	Outcome   RecoveryOutcome
	Proof     *RecoverabilityProof
	FinalState ReplicaState
	Error     error
}

// ExecuteRecovery runs the full recovery flow for a single replica:
// connect → handshake (from history) → catch-up or escalate.
//
// For catch-up outcomes, the caller provides entries via the returned
// CatchUpHandle. For rebuild outcomes, the sender is left at NeedsRebuild
// and requires a separate rebuild assignment.
func (o *RecoveryOrchestrator) ExecuteRecovery(replicaID string, replicaFlushedLSN uint64, history *RetainedHistory) RecoveryResult {
	s := o.Registry.Sender(replicaID)
	if s == nil {
		return RecoveryResult{ReplicaID: replicaID, Error: fmt.Errorf("sender not found")}
	}

	sessID := s.SessionID()
	if sessID == 0 {
		return RecoveryResult{ReplicaID: replicaID, Error: fmt.Errorf("no session")}
	}

	// Connect.
	if err := s.BeginConnect(sessID); err != nil {
		o.Log.Record(replicaID, sessID, "connect_failed", err.Error())
		return RecoveryResult{ReplicaID: replicaID, FinalState: s.State(), Error: err}
	}
	o.Log.Record(replicaID, sessID, "connected", "")

	// Handshake from history.
	outcome, proof, err := s.RecordHandshakeFromHistory(sessID, replicaFlushedLSN, history)
	if err != nil {
		o.Log.Record(replicaID, sessID, "handshake_failed", err.Error())
		return RecoveryResult{ReplicaID: replicaID, Outcome: outcome, Proof: proof, FinalState: s.State(), Error: err}
	}
	o.Log.Record(replicaID, sessID, "handshake", fmt.Sprintf("outcome=%s", outcome))

	if outcome == OutcomeNeedsRebuild {
		o.Log.Record(replicaID, sessID, "escalated", fmt.Sprintf("needs_rebuild: %s", proof.Reason))
		return RecoveryResult{ReplicaID: replicaID, Outcome: outcome, Proof: proof, FinalState: StateNeedsRebuild}
	}

	// Zero-gap: complete immediately (no catch-up needed).
	if outcome == OutcomeZeroGap {
		if s.CompleteSessionByID(sessID) {
			o.Log.Record(replicaID, sessID, "completed", "zero_gap")
			return RecoveryResult{ReplicaID: replicaID, Outcome: outcome, Proof: proof, FinalState: StateInSync}
		}
	}

	return RecoveryResult{ReplicaID: replicaID, Outcome: outcome, Proof: proof, FinalState: s.State()}
}

// CompleteCatchUp drives catch-up from startLSN to targetLSN and completes.
// Called after ExecuteRecovery returns OutcomeCatchUp.
func (o *RecoveryOrchestrator) CompleteCatchUp(replicaID string, targetLSN uint64) error {
	s := o.Registry.Sender(replicaID)
	if s == nil {
		return fmt.Errorf("sender not found")
	}
	sessID := s.SessionID()

	if err := s.BeginCatchUp(sessID); err != nil {
		o.Log.Record(replicaID, sessID, "catchup_failed", err.Error())
		return err
	}
	o.Log.Record(replicaID, sessID, "catchup_started", "")

	if err := s.RecordCatchUpProgress(sessID, targetLSN); err != nil {
		o.Log.Record(replicaID, sessID, "catchup_progress_failed", err.Error())
		return err
	}

	if !s.CompleteSessionByID(sessID) {
		o.Log.Record(replicaID, sessID, "completion_rejected", "")
		return fmt.Errorf("completion rejected")
	}
	o.Log.Record(replicaID, sessID, "completed", "in_sync")
	return nil
}

// CompleteRebuild drives the rebuild from history and completes.
// Called after a rebuild assignment when the sender is at NeedsRebuild.
func (o *RecoveryOrchestrator) CompleteRebuild(replicaID string, history *RetainedHistory) error {
	s := o.Registry.Sender(replicaID)
	if s == nil {
		return fmt.Errorf("sender not found")
	}
	sessID := s.SessionID()

	if err := s.BeginConnect(sessID); err != nil {
		o.Log.Record(replicaID, sessID, "rebuild_connect_failed", err.Error())
		return err
	}
	o.Log.Record(replicaID, sessID, "rebuild_connected", "")

	if err := s.RecordHandshake(sessID, 0, history.CommittedLSN); err != nil {
		return err
	}

	if err := s.SelectRebuildFromHistory(sessID, history); err != nil {
		o.Log.Record(replicaID, sessID, "rebuild_source_failed", err.Error())
		return err
	}

	snap := s.SessionSnapshot()
	o.Log.Record(replicaID, sessID, "rebuild_source_selected", fmt.Sprintf("kind=%s", snap.Kind))

	if err := s.BeginRebuildTransfer(sessID); err != nil {
		return err
	}

	// Determine transfer target based on rebuild source.
	source, snapLSN := history.RebuildSourceDecision()
	if source == RebuildSnapshotTail {
		s.RecordRebuildTransferProgress(sessID, snapLSN)
		if err := s.BeginRebuildTailReplay(sessID); err != nil {
			return err
		}
		s.RecordRebuildTailProgress(sessID, history.CommittedLSN)
	} else {
		s.RecordRebuildTransferProgress(sessID, history.CommittedLSN)
	}

	if err := s.CompleteRebuild(sessID); err != nil {
		o.Log.Record(replicaID, sessID, "rebuild_failed", err.Error())
		return err
	}
	o.Log.Record(replicaID, sessID, "rebuild_completed", "in_sync")
	return nil
}

// UpdateSenderEpoch advances a specific sender's epoch via the orchestrator.
// Logs the transition and any session invalidation.
func (o *RecoveryOrchestrator) UpdateSenderEpoch(replicaID string, newEpoch uint64) {
	s := o.Registry.Sender(replicaID)
	if s == nil {
		return
	}
	hadSession := s.HasActiveSession()
	oldSessID := s.SessionID()
	s.UpdateEpoch(newEpoch)
	if hadSession && !s.HasActiveSession() {
		o.Log.Record(replicaID, oldSessID, "session_invalidated",
			fmt.Sprintf("epoch_advanced_to_%d", newEpoch))
	}
}

// InvalidateEpoch invalidates all stale sessions with per-replica logging.
func (o *RecoveryOrchestrator) InvalidateEpoch(newEpoch uint64) int {
	// Collect per-replica state before invalidation.
	type pre struct {
		id    string
		sess  uint64
		had   bool
	}
	var senders []pre
	for _, s := range o.Registry.All() {
		senders = append(senders, pre{
			id:   s.ReplicaID(),
			sess: s.SessionID(),
			had:  s.HasActiveSession(),
		})
	}

	count := o.Registry.InvalidateEpoch(newEpoch)

	// Log per-replica invalidations.
	for _, p := range senders {
		s := o.Registry.Sender(p.id)
		if s != nil && p.had && !s.HasActiveSession() {
			o.Log.Record(p.id, p.sess, "session_invalidated",
				fmt.Sprintf("epoch_bump_to_%d", newEpoch))
		}
	}
	return count
}
