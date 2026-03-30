package replication

import (
	"fmt"
	"sync"
)

// Sender owns the replication channel to one replica. It is the authority
// boundary for all execution operations. All mutable state is unexported —
// external code reads state through accessors and mutates through execution APIs.
type Sender struct {
	mu sync.Mutex

	replicaID string
	endpoint  Endpoint
	epoch     uint64
	state     ReplicaState

	session *Session
	stopped bool
}

// NewSender creates a sender for a replica.
func NewSender(replicaID string, endpoint Endpoint, epoch uint64) *Sender {
	return &Sender{
		replicaID: replicaID,
		endpoint:  endpoint,
		epoch:     epoch,
		state:     StateDisconnected,
	}
}

// Read-only accessors.

func (s *Sender) ReplicaID() string    { s.mu.Lock(); defer s.mu.Unlock(); return s.replicaID }
func (s *Sender) Endpoint() Endpoint   { s.mu.Lock(); defer s.mu.Unlock(); return s.endpoint }
func (s *Sender) Epoch() uint64        { s.mu.Lock(); defer s.mu.Unlock(); return s.epoch }
func (s *Sender) State() ReplicaState  { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *Sender) Stopped() bool        { s.mu.Lock(); defer s.mu.Unlock(); return s.stopped }

// SessionSnapshot returns a read-only copy of the current session state.
// Returns nil if no session is active. The returned snapshot is disconnected
// from the live session — mutations to the Sender do not affect it.
func (s *Sender) SessionSnapshot() *SessionSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session == nil {
		return nil
	}
	return &SessionSnapshot{
		ID:               s.session.id,
		ReplicaID:        s.session.replicaID,
		Epoch:            s.session.epoch,
		Kind:             s.session.kind,
		Phase:            s.session.phase,
		InvalidateReason: s.session.invalidateReason,
		StartLSN:         s.session.startLSN,
		TargetLSN:        s.session.targetLSN,
		FrozenTargetLSN:  s.session.frozenTargetLSN,
		RecoveredTo:      s.session.recoveredTo,
		Active:           s.session.Active(),
	}
}

// SessionSnapshot is a read-only copy of session state for external inspection.
type SessionSnapshot struct {
	ID               uint64
	ReplicaID        string
	Epoch            uint64
	Kind             SessionKind
	Phase            SessionPhase
	InvalidateReason string
	StartLSN         uint64
	TargetLSN        uint64
	FrozenTargetLSN  uint64
	RecoveredTo      uint64
	Active           bool
}

// SessionID returns the current session ID, or 0 if no session.
func (s *Sender) SessionID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session == nil {
		return 0
	}
	return s.session.id
}

// HasActiveSession returns true if a session is currently active.
func (s *Sender) HasActiveSession() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.session != nil && s.session.Active()
}

// === Lifecycle APIs ===

// UpdateEpoch advances the sender's epoch. Invalidates stale sessions.
func (s *Sender) UpdateEpoch(epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || epoch <= s.epoch {
		return
	}
	oldEpoch := s.epoch
	s.epoch = epoch
	if s.session != nil && s.session.epoch < epoch {
		s.session.invalidate(fmt.Sprintf("epoch_advanced_%d_to_%d", oldEpoch, epoch))
		s.session = nil
		s.state = StateDisconnected
	}
}

// UpdateEndpoint updates the target address. Invalidates session on change.
func (s *Sender) UpdateEndpoint(ep Endpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	if s.endpoint.Changed(ep) && s.session != nil {
		s.session.invalidate("endpoint_changed")
		s.session = nil
		s.state = StateDisconnected
	}
	s.endpoint = ep
}

// SessionOption configures a newly created session.
type SessionOption func(s *Session)

// WithBudget attaches a catch-up budget to the session.
func WithBudget(budget CatchUpBudget) SessionOption {
	return func(s *Session) {
		b := budget // copy
		s.budget = &b
	}
}

// AttachSession creates a new recovery session. Epoch must match sender epoch.
func (s *Sender) AttachSession(epoch uint64, kind SessionKind, opts ...SessionOption) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return 0, fmt.Errorf("sender stopped")
	}
	if epoch != s.epoch {
		return 0, fmt.Errorf("epoch mismatch: sender=%d session=%d", s.epoch, epoch)
	}
	if s.session != nil && s.session.Active() {
		return 0, fmt.Errorf("session already active (id=%d)", s.session.id)
	}
	sess := newSession(s.replicaID, epoch, kind)
	for _, opt := range opts {
		opt(sess)
	}
	s.session = sess
	return sess.id, nil
}

// SupersedeSession invalidates current session and attaches new at sender epoch.
func (s *Sender) SupersedeSession(kind SessionKind, reason string, opts ...SessionOption) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return 0
	}
	if s.session != nil {
		s.session.invalidate(reason)
	}
	sess := newSession(s.replicaID, s.epoch, kind)
	for _, opt := range opts {
		opt(sess)
	}
	s.session = sess
	return sess.id
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
	s.state = targetState
}

// === Catch-up execution APIs ===

func (s *Sender) BeginConnect(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if !s.session.advance(PhaseConnecting) {
		return fmt.Errorf("cannot begin connect: phase=%s", s.session.phase)
	}
	s.state = StateConnecting
	return nil
}

func (s *Sender) RecordHandshake(sessionID uint64, startLSN, targetLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if targetLSN < startLSN {
		return fmt.Errorf("invalid range: target=%d < start=%d", targetLSN, startLSN)
	}
	if !s.session.advance(PhaseHandshake) {
		return fmt.Errorf("cannot record handshake: phase=%s", s.session.phase)
	}
	s.session.setRange(startLSN, targetLSN)
	return nil
}

func (s *Sender) RecordHandshakeWithOutcome(sessionID uint64, result HandshakeResult) (RecoveryOutcome, error) {
	outcome := ClassifyRecoveryOutcome(result)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return outcome, err
	}
	if s.session.phase != PhaseConnecting {
		return outcome, fmt.Errorf("handshake requires connecting, got %s", s.session.phase)
	}
	if outcome == OutcomeNeedsRebuild {
		s.session.invalidate("gap_exceeds_retention")
		s.session = nil
		s.state = StateNeedsRebuild
		return outcome, nil
	}
	if !s.session.advance(PhaseHandshake) {
		return outcome, fmt.Errorf("cannot advance to handshake: phase=%s", s.session.phase)
	}
	switch outcome {
	case OutcomeZeroGap:
		s.session.setRange(result.ReplicaFlushedLSN, result.ReplicaFlushedLSN)
	case OutcomeCatchUp:
		if result.ReplicaFlushedLSN > result.CommittedLSN {
			s.session.truncateRequired = true
			s.session.truncateToLSN = result.CommittedLSN
			s.session.setRange(result.CommittedLSN, result.CommittedLSN)
		} else {
			s.session.setRange(result.ReplicaFlushedLSN, result.CommittedLSN)
		}
	}
	return outcome, nil
}

// RecordHandshakeFromHistory records the handshake using the primary's
// RetainedHistory as the authoritative recoverability source. This is the
// preferred engine-level API — it ensures recovery decisions are backed
// by actual retention state, not caller-supplied values.
func (s *Sender) RecordHandshakeFromHistory(sessionID uint64, replicaFlushedLSN uint64, history *RetainedHistory) (RecoveryOutcome, *RecoverabilityProof, error) {
	proof := history.ProveRecoverability(replicaFlushedLSN)
	hr := history.MakeHandshakeResult(replicaFlushedLSN)
	outcome, err := s.RecordHandshakeWithOutcome(sessionID, hr)
	return outcome, &proof, err
}

// SelectRebuildFromHistory selects the rebuild source using the primary's
// RetainedHistory. This is the preferred engine-level API — it ensures
// the rebuild-source decision accounts for both checkpoint trust AND
// tail replayability.
func (s *Sender) SelectRebuildFromHistory(sessionID uint64, history *RetainedHistory) error {
	source, snapLSN := history.RebuildSourceDecision()
	valid := source == RebuildSnapshotTail
	return s.SelectRebuildSource(sessionID, snapLSN, valid, history.CommittedLSN)
}

func (s *Sender) BeginCatchUp(sessionID uint64, startTick ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.kind == SessionRebuild {
		return fmt.Errorf("rebuild sessions must use rebuild APIs")
	}
	if !s.session.advance(PhaseCatchUp) {
		return fmt.Errorf("cannot begin catch-up: phase=%s", s.session.phase)
	}
	s.state = StateCatchingUp
	s.session.frozenTargetLSN = s.session.targetLSN
	if len(startTick) > 0 {
		s.session.tracker.StartTick = startTick[0]
		s.session.tracker.LastProgressTick = startTick[0]
	}
	return nil
}

func (s *Sender) RecordCatchUpProgress(sessionID uint64, recoveredTo uint64, tick ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.kind == SessionRebuild {
		return fmt.Errorf("rebuild sessions must use rebuild APIs")
	}
	if s.session.phase != PhaseCatchUp {
		return fmt.Errorf("progress requires catchup phase, got %s", s.session.phase)
	}
	if recoveredTo <= s.session.recoveredTo {
		return fmt.Errorf("progress regression: %d <= %d", recoveredTo, s.session.recoveredTo)
	}
	if s.session.frozenTargetLSN > 0 && recoveredTo > s.session.frozenTargetLSN {
		return fmt.Errorf("progress %d exceeds frozen target %d", recoveredTo, s.session.frozenTargetLSN)
	}
	if s.session.budget != nil && s.session.budget.ProgressDeadlineTicks > 0 && len(tick) == 0 {
		return fmt.Errorf("tick required when ProgressDeadlineTicks > 0")
	}
	delta := recoveredTo - s.session.recoveredTo
	s.session.tracker.EntriesReplayed += delta
	s.session.updateProgress(recoveredTo)
	if len(tick) > 0 {
		s.session.tracker.LastProgressTick = tick[0]
	}
	return nil
}

func (s *Sender) RecordTruncation(sessionID uint64, truncatedToLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if !s.session.truncateRequired {
		return fmt.Errorf("truncation not required")
	}
	if truncatedToLSN != s.session.truncateToLSN {
		return fmt.Errorf("truncation LSN mismatch: expected %d, got %d", s.session.truncateToLSN, truncatedToLSN)
	}
	s.session.truncateRecorded = true
	return nil
}

func (s *Sender) CompleteSessionByID(sessionID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkAuthority(sessionID) != nil {
		return false
	}
	sess := s.session
	if sess.kind == SessionRebuild {
		return false
	}
	if sess.truncateRequired && !sess.truncateRecorded {
		return false
	}
	switch sess.phase {
	case PhaseCatchUp:
		if !sess.Converged() {
			return false
		}
	case PhaseHandshake:
		if sess.targetLSN != sess.startLSN {
			return false
		}
	default:
		return false
	}
	sess.complete()
	s.session = nil
	s.state = StateInSync
	return true
}

func (s *Sender) CheckBudget(sessionID uint64, currentTick uint64) (BudgetViolation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return BudgetOK, err
	}
	if s.session.budget == nil {
		return BudgetOK, nil
	}
	v := s.session.budget.Check(s.session.tracker, currentTick)
	if v != BudgetOK {
		s.session.invalidate(fmt.Sprintf("budget_%s", v))
		s.session = nil
		s.state = StateNeedsRebuild
	}
	return v, nil
}

// === Rebuild execution APIs ===

func (s *Sender) SelectRebuildSource(sessionID uint64, snapshotLSN uint64, snapshotValid bool, committedLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.kind != SessionRebuild {
		return fmt.Errorf("not a rebuild session")
	}
	if s.session.phase != PhaseHandshake {
		return fmt.Errorf("requires PhaseHandshake, got %s", s.session.phase)
	}
	if s.session.rebuild == nil {
		return fmt.Errorf("rebuild state not initialized")
	}
	return s.session.rebuild.SelectSource(snapshotLSN, snapshotValid, committedLSN)
}

func (s *Sender) BeginRebuildTransfer(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	return s.session.rebuild.BeginTransfer()
}

func (s *Sender) RecordRebuildTransferProgress(sessionID uint64, transferredTo uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	return s.session.rebuild.RecordTransferProgress(transferredTo)
}

func (s *Sender) BeginRebuildTailReplay(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	return s.session.rebuild.BeginTailReplay()
}

func (s *Sender) RecordRebuildTailProgress(sessionID uint64, replayedTo uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	return s.session.rebuild.RecordTailReplayProgress(replayedTo)
}

func (s *Sender) CompleteRebuild(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkAuthority(sessionID); err != nil {
		return err
	}
	if s.session.rebuild == nil {
		return fmt.Errorf("no rebuild state")
	}
	if err := s.session.rebuild.Complete(); err != nil {
		return err
	}
	s.session.complete()
	s.session = nil
	s.state = StateInSync
	return nil
}

func (s *Sender) checkAuthority(sessionID uint64) error {
	if s.stopped {
		return fmt.Errorf("sender stopped")
	}
	if s.session == nil {
		return fmt.Errorf("no active session")
	}
	if s.session.id != sessionID {
		return fmt.Errorf("session ID mismatch: active=%d requested=%d", s.session.id, sessionID)
	}
	if !s.session.Active() {
		return fmt.Errorf("session %d not active (phase=%s)", sessionID, s.session.phase)
	}
	return nil
}
