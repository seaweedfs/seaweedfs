package replication

import (
	"fmt"
	"sync"
)

// Sender owns the replication channel to one replica. It is the authority
// boundary for all execution operations — every API validates the session
// ID before mutating state.
type Sender struct {
	mu sync.Mutex

	ReplicaID string
	Endpoint  Endpoint
	Epoch     uint64
	State     ReplicaState

	session *Session
	stopped bool
}

// NewSender creates a sender for a replica.
func NewSender(replicaID string, endpoint Endpoint, epoch uint64) *Sender {
	return &Sender{
		ReplicaID: replicaID,
		Endpoint:  endpoint,
		Epoch:     epoch,
		State:     StateDisconnected,
	}
}

// UpdateEpoch advances the sender's epoch. Invalidates stale sessions.
func (s *Sender) UpdateEpoch(epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || epoch <= s.Epoch {
		return
	}
	oldEpoch := s.Epoch
	s.Epoch = epoch
	if s.session != nil && s.session.Epoch < epoch {
		s.session.invalidate(fmt.Sprintf("epoch_advanced_%d_to_%d", oldEpoch, epoch))
		s.session = nil
		s.State = StateDisconnected
	}
}

// UpdateEndpoint updates the target address. Invalidates session on change.
func (s *Sender) UpdateEndpoint(ep Endpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	if s.Endpoint.Changed(ep) && s.session != nil {
		s.session.invalidate("endpoint_changed")
		s.session = nil
		s.State = StateDisconnected
	}
	s.Endpoint = ep
}

// AttachSession creates a new recovery session. Epoch must match sender epoch.
func (s *Sender) AttachSession(epoch uint64, kind SessionKind) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, fmt.Errorf("sender stopped")
	}
	if epoch != s.Epoch {
		return nil, fmt.Errorf("epoch mismatch: sender=%d session=%d", s.Epoch, epoch)
	}
	if s.session != nil && s.session.Active() {
		return nil, fmt.Errorf("session already active (id=%d)", s.session.ID)
	}
	sess := newSession(s.ReplicaID, epoch, kind)
	s.session = sess
	return sess, nil
}

// SupersedeSession invalidates current session and attaches new at sender epoch.
func (s *Sender) SupersedeSession(kind SessionKind, reason string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	if s.session != nil {
		s.session.invalidate(reason)
	}
	sess := newSession(s.ReplicaID, s.Epoch, kind)
	s.session = sess
	return sess
}

// Session returns the current session, or nil.
func (s *Sender) Session() *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.session
}

// Stopped returns true if the sender has been stopped.
func (s *Sender) Stopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

// Stop shuts down the sender.
func (s *Sender) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	if s.session != nil {
		s.session.invalidate("sender_stopped")
		s.session = nil
	}
}

// InvalidateSession invalidates current session with target state.
func (s *Sender) InvalidateSession(reason string, targetState ReplicaState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != nil {
		s.session.invalidate(reason)
		s.session = nil
	}
	s.State = targetState
}

// === Catch-up execution APIs ===

// BeginConnect transitions init → connecting.
func (s *Sender) BeginConnect(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if !s.session.Advance(PhaseConnecting) {
		return fmt.Errorf("cannot begin connect: phase=%s", s.session.Phase)
	}
	s.State = StateConnecting
	return nil
}

// RecordHandshake records handshake result and sets catch-up range.
func (s *Sender) RecordHandshake(sessionID uint64, startLSN, targetLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if targetLSN < startLSN {
		return fmt.Errorf("invalid range: target=%d < start=%d", targetLSN, startLSN)
	}
	if !s.session.Advance(PhaseHandshake) {
		return fmt.Errorf("cannot record handshake: phase=%s", s.session.Phase)
	}
	s.session.SetRange(startLSN, targetLSN)
	return nil
}

// RecordHandshakeWithOutcome records handshake and classifies the recovery outcome.
func (s *Sender) RecordHandshakeWithOutcome(sessionID uint64, result HandshakeResult) (RecoveryOutcome, error) {
	outcome := ClassifyRecoveryOutcome(result)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return outcome, err
	}
	if s.session.Phase != PhaseConnecting {
		return outcome, fmt.Errorf("handshake requires connecting, got %s", s.session.Phase)
	}
	if outcome == OutcomeNeedsRebuild {
		s.session.invalidate("gap_exceeds_retention")
		s.session = nil
		s.State = StateNeedsRebuild
		return outcome, nil
	}
	if !s.session.Advance(PhaseHandshake) {
		return outcome, fmt.Errorf("cannot advance to handshake: phase=%s", s.session.Phase)
	}
	switch outcome {
	case OutcomeZeroGap:
		s.session.SetRange(result.ReplicaFlushedLSN, result.ReplicaFlushedLSN)
	case OutcomeCatchUp:
		if result.ReplicaFlushedLSN > result.CommittedLSN {
			s.session.TruncateRequired = true
			s.session.TruncateToLSN = result.CommittedLSN
			s.session.SetRange(result.CommittedLSN, result.CommittedLSN)
		} else {
			s.session.SetRange(result.ReplicaFlushedLSN, result.CommittedLSN)
		}
	}
	return outcome, nil
}

// BeginCatchUp transitions to catch-up phase. Rejects rebuild sessions.
// Freezes the target unconditionally.
func (s *Sender) BeginCatchUp(sessionID uint64, startTick ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Kind == SessionRebuild {
		return fmt.Errorf("rebuild sessions must use rebuild APIs")
	}
	if !s.session.Advance(PhaseCatchUp) {
		return fmt.Errorf("cannot begin catch-up: phase=%s", s.session.Phase)
	}
	s.State = StateCatchingUp
	s.session.FrozenTargetLSN = s.session.TargetLSN
	if len(startTick) > 0 {
		s.session.Tracker.StartTick = startTick[0]
		s.session.Tracker.LastProgressTick = startTick[0]
	}
	return nil
}

// RecordCatchUpProgress records catch-up progress. Rejects rebuild sessions.
// Entry counting uses LSN delta. Tick is required when ProgressDeadlineTicks > 0.
func (s *Sender) RecordCatchUpProgress(sessionID uint64, recoveredTo uint64, tick ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Kind == SessionRebuild {
		return fmt.Errorf("rebuild sessions must use rebuild APIs")
	}
	if s.session.Phase != PhaseCatchUp {
		return fmt.Errorf("progress requires catchup phase, got %s", s.session.Phase)
	}
	if recoveredTo <= s.session.RecoveredTo {
		return fmt.Errorf("progress regression: %d <= %d", recoveredTo, s.session.RecoveredTo)
	}
	if s.session.FrozenTargetLSN > 0 && recoveredTo > s.session.FrozenTargetLSN {
		return fmt.Errorf("progress %d exceeds frozen target %d", recoveredTo, s.session.FrozenTargetLSN)
	}
	if s.session.Budget != nil && s.session.Budget.ProgressDeadlineTicks > 0 && len(tick) == 0 {
		return fmt.Errorf("tick required when ProgressDeadlineTicks > 0")
	}
	delta := recoveredTo - s.session.RecoveredTo
	s.session.Tracker.EntriesReplayed += delta
	s.session.UpdateProgress(recoveredTo)
	if len(tick) > 0 {
		s.session.Tracker.LastProgressTick = tick[0]
	}
	return nil
}

// RecordTruncation confirms divergent tail cleanup.
func (s *Sender) RecordTruncation(sessionID uint64, truncatedToLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if !s.session.TruncateRequired {
		return fmt.Errorf("truncation not required")
	}
	if truncatedToLSN != s.session.TruncateToLSN {
		return fmt.Errorf("truncation LSN mismatch: expected %d, got %d", s.session.TruncateToLSN, truncatedToLSN)
	}
	s.session.TruncateRecorded = true
	return nil
}

// CompleteSessionByID completes catch-up sessions. Rejects rebuild sessions.
func (s *Sender) CompleteSessionByID(sessionID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkAuthority(sessionID) != nil {
		return false
	}
	sess := s.session
	if sess.Kind == SessionRebuild {
		return false
	}
	if sess.TruncateRequired && !sess.TruncateRecorded {
		return false
	}
	switch sess.Phase {
	case PhaseCatchUp:
		if !sess.Converged() {
			return false
		}
	case PhaseHandshake:
		if sess.TargetLSN != sess.StartLSN {
			return false
		}
	default:
		return false
	}
	sess.complete()
	s.session = nil
	s.State = StateInSync
	return true
}

// CheckBudget evaluates catch-up budget. Auto-escalates on violation.
func (s *Sender) CheckBudget(sessionID uint64, currentTick uint64) (BudgetViolation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return BudgetOK, err
	}
	if s.session.Budget == nil {
		return BudgetOK, nil
	}
	v := s.session.Budget.Check(s.session.Tracker, currentTick)
	if v != BudgetOK {
		s.session.invalidate(fmt.Sprintf("budget_%s", v))
		s.session = nil
		s.State = StateNeedsRebuild
	}
	return v, nil
}

// === Rebuild execution APIs ===

// SelectRebuildSource chooses rebuild source. Requires PhaseHandshake.
func (s *Sender) SelectRebuildSource(sessionID uint64, snapshotLSN uint64, snapshotValid bool, committedLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Kind != SessionRebuild {
		return fmt.Errorf("not a rebuild session")
	}
	if s.session.Phase != PhaseHandshake {
		return fmt.Errorf("requires PhaseHandshake, got %s", s.session.Phase)
	}
	if s.session.Rebuild == nil {
		return fmt.Errorf("rebuild state not initialized")
	}
	return s.session.Rebuild.SelectSource(snapshotLSN, snapshotValid, committedLSN)
}

func (s *Sender) BeginRebuildTransfer(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	return s.session.Rebuild.BeginTransfer()
}

func (s *Sender) RecordRebuildTransferProgress(sessionID uint64, transferredTo uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	return s.session.Rebuild.RecordTransferProgress(transferredTo)
}

func (s *Sender) BeginRebuildTailReplay(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	return s.session.Rebuild.BeginTailReplay()
}

func (s *Sender) RecordRebuildTailProgress(sessionID uint64, replayedTo uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	return s.session.Rebuild.RecordTailReplayProgress(replayedTo)
}

// CompleteRebuild completes a rebuild session. Requires ReadyToComplete.
func (s *Sender) CompleteRebuild(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	if err := s.session.Rebuild.Complete(); err != nil {
		return err
	}
	s.session.complete()
	s.session = nil
	s.State = StateInSync
	return nil
}

// checkAuthority validates session ownership.
func (s *Sender) checkAuthority(sessionID uint64) error {
	if s.stopped {
		return fmt.Errorf("sender stopped")
	}
	if s.session == nil {
		return fmt.Errorf("no active session")
	}
	if s.session.ID != sessionID {
		return fmt.Errorf("session ID mismatch: active=%d requested=%d", s.session.ID, sessionID)
	}
	if !s.session.Active() {
		return fmt.Errorf("session %d not active (phase=%s)", sessionID, s.session.Phase)
	}
	return nil
}
