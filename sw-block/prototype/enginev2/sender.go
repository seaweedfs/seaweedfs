// Package enginev2 implements V2 per-replica sender/session ownership.
//
// Each replica has exactly one Sender that owns its identity (canonical address)
// and at most one active RecoverySession per epoch. The Sender survives topology
// changes; the session does not survive epoch bumps.
package enginev2

import (
	"fmt"
	"sync"
)

// ReplicaState tracks the per-replica replication state machine.
type ReplicaState string

const (
	StateDisconnected ReplicaState = "disconnected"
	StateConnecting   ReplicaState = "connecting"
	StateCatchingUp   ReplicaState = "catching_up"
	StateInSync       ReplicaState = "in_sync"
	StateDegraded     ReplicaState = "degraded"
	StateNeedsRebuild ReplicaState = "needs_rebuild"
)

// Endpoint represents a replica's network identity.
type Endpoint struct {
	DataAddr string
	CtrlAddr string
	Version  uint64 // bumped on address change
}

// Sender owns the replication channel to one replica. It is identified
// by ReplicaID (canonical data address at creation time) and survives
// topology changes as long as the replica stays in the set.
//
// A Sender holds at most one active RecoverySession. Normal in-sync
// operation does not require a session — Ship/Barrier work directly.
type Sender struct {
	mu sync.Mutex

	ReplicaID string   // canonical identity — stable across reconnects
	Endpoint  Endpoint // current network address (may change via UpdateEndpoint)
	Epoch     uint64   // current epoch
	State     ReplicaState

	session *RecoverySession // nil when in-sync or disconnected without recovery
	stopped bool
}

// NewSender creates a sender for a replica at the given endpoint and epoch.
func NewSender(replicaID string, endpoint Endpoint, epoch uint64) *Sender {
	return &Sender{
		ReplicaID: replicaID,
		Endpoint:  endpoint,
		Epoch:     epoch,
		State:     StateDisconnected,
	}
}

// UpdateEpoch updates the sender's epoch. If a recovery session is active
// at a stale epoch, it is invalidated.
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

// UpdateEndpoint updates the sender's target address after a control-plane
// assignment refresh. If a recovery session is active and the address changed,
// the session is invalidated (the new address needs a fresh session).
func (s *Sender) UpdateEndpoint(ep Endpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	addrChanged := s.Endpoint.DataAddr != ep.DataAddr || s.Endpoint.CtrlAddr != ep.CtrlAddr || s.Endpoint.Version != ep.Version
	s.Endpoint = ep
	if addrChanged && s.session != nil {
		s.session.invalidate("endpoint_changed")
		s.session = nil
		s.State = StateDisconnected
	}
}

// AttachSession creates and attaches a new recovery session for this sender.
// The session epoch must match the sender's current epoch — stale or future
// epoch sessions are rejected. Returns an error if a session is already active,
// the sender is stopped, or the epoch doesn't match.
func (s *Sender) AttachSession(epoch uint64, kind SessionKind) (*RecoverySession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, fmt.Errorf("sender stopped")
	}
	if epoch != s.Epoch {
		return nil, fmt.Errorf("epoch mismatch: sender=%d session=%d", s.Epoch, epoch)
	}
	if s.session != nil && s.session.Active() {
		return nil, fmt.Errorf("session already active (epoch=%d kind=%s)", s.session.Epoch, s.session.Kind)
	}
	sess := newRecoverySession(s.ReplicaID, epoch, kind)
	s.session = sess
	// Ownership established but execution not started.
	// BeginConnect() is the first execution-state transition.
	return sess, nil
}

// SupersedeSession invalidates the current session (if any) and attaches
// a new one at the sender's current epoch. Used when an assignment change
// requires a fresh recovery path. The old session is invalidated with the
// given reason. Always uses s.Epoch — does not accept an epoch parameter
// to prevent epoch coherence drift.
//
// Establishes ownership only — does not mutate sender state.
// BeginConnect() starts execution.
func (s *Sender) SupersedeSession(kind SessionKind, reason string) *RecoverySession {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil
	}
	if s.session != nil {
		s.session.invalidate(reason)
	}
	sess := newRecoverySession(s.ReplicaID, s.Epoch, kind)
	s.session = sess
	return sess
}

// Session returns the current recovery session, or nil if none.
func (s *Sender) Session() *RecoverySession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.session
}

// CompleteSessionByID marks the session as completed and transitions the
// sender to InSync. Requires:
//   - sessionID matches the current active session
//   - session is in PhaseCatchUp and has Converged (normal path)
//   - OR session is in PhaseHandshake and gap is zero (fast path: already in sync)
//
// Returns false if any check fails (stale ID, wrong phase, not converged).
func (s *Sender) CompleteSessionByID(sessionID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return false
	}
	sess := s.session
	// Truncation gate: if truncation was required, it must be recorded.
	if sess.TruncateRequired && !sess.TruncateRecorded {
		return false
	}
	switch sess.Phase {
	case PhaseCatchUp:
		if !sess.Converged() {
			return false // not converged yet
		}
	case PhaseHandshake:
		if sess.TargetLSN != sess.StartLSN {
			return false // has a gap — must catch up first
		}
		// Zero-gap fast path: handshake showed replica already at target.
	default:
		return false // not at a completion-ready phase
	}
	sess.complete()
	s.session = nil
	s.State = StateInSync
	return true
}

// === Execution APIs — sender-owned authority gate ===
//
// All execution APIs validate the sessionID against the current active session.
// This prevents stale results from old/superseded sessions from mutating state.
// The sender is the authority boundary, not the session object.

// BeginConnect transitions the session from init to connecting.
// Mutates: session.Phase → PhaseConnecting. Sender.State → StateConnecting.
// Rejects: wrong sessionID, stopped sender, session not in PhaseInit.
func (s *Sender) BeginConnect(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return err
	}
	if !s.session.Advance(PhaseConnecting) {
		return fmt.Errorf("cannot begin connect: session phase=%s", s.session.Phase)
	}
	s.State = StateConnecting
	return nil
}

// RecordHandshake records a successful handshake result and sets the catch-up range.
// Mutates: session.Phase → PhaseHandshake, session.StartLSN/TargetLSN.
// Rejects: wrong sessionID, wrong phase, invalid range.
func (s *Sender) RecordHandshake(sessionID uint64, startLSN, targetLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return err
	}
	if targetLSN < startLSN {
		return fmt.Errorf("invalid handshake range: target=%d < start=%d", targetLSN, startLSN)
	}
	if !s.session.Advance(PhaseHandshake) {
		return fmt.Errorf("cannot record handshake: session phase=%s", s.session.Phase)
	}
	s.session.SetRange(startLSN, targetLSN)
	return nil
}

// RecordHandshakeWithOutcome records the handshake AND classifies the recovery
// outcome. This is the preferred handshake API — it determines the recovery
// path in one step:
//   - OutcomeZeroGap:      sets zero range, ready for fast completion
//   - OutcomeCatchUp:       sets catch-up range, ready for BeginCatchUp
//   - OutcomeNeedsRebuild: invalidates session, transitions sender to NeedsRebuild
//
// Returns the outcome. On NeedsRebuild, the session is dead and the caller
// should not attempt further execution.
func (s *Sender) RecordHandshakeWithOutcome(sessionID uint64, result HandshakeResult) (RecoveryOutcome, error) {
	outcome := ClassifyRecoveryOutcome(result)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return outcome, err
	}
	// Must be in PhaseConnecting — require valid execution entry point.
	if s.session.Phase != PhaseConnecting {
		return outcome, fmt.Errorf("handshake requires PhaseConnecting, got %s", s.session.Phase)
	}

	if outcome == OutcomeNeedsRebuild {
		s.session.invalidate("gap_exceeds_retention")
		s.session = nil
		s.State = StateNeedsRebuild
		return outcome, nil
	}

	if !s.session.Advance(PhaseHandshake) {
		return outcome, fmt.Errorf("cannot record handshake: session phase=%s", s.session.Phase)
	}

	switch outcome {
	case OutcomeZeroGap:
		s.session.SetRange(result.ReplicaFlushedLSN, result.ReplicaFlushedLSN)
	case OutcomeCatchUp:
		if result.ReplicaFlushedLSN > result.CommittedLSN {
			// Replica ahead of committed — divergent tail needs truncation.
			s.session.TruncateRequired = true
			s.session.TruncateToLSN = result.CommittedLSN
			// Catch-up range is zero after truncation (already has committed prefix).
			s.session.SetRange(result.CommittedLSN, result.CommittedLSN)
		} else {
			s.session.SetRange(result.ReplicaFlushedLSN, result.CommittedLSN)
		}
	}
	return outcome, nil
}

// RecordTruncation confirms that the replica's divergent tail has been truncated.
// Must be called before completion when session.TruncateRequired is true.
// Rejects: wrong sessionID, truncation not required, already recorded.
func (s *Sender) RecordTruncation(sessionID uint64, truncatedToLSN uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return err
	}
	if !s.session.TruncateRequired {
		return fmt.Errorf("truncation not required for this session")
	}
	if truncatedToLSN != s.session.TruncateToLSN {
		return fmt.Errorf("truncation LSN mismatch: expected %d, got %d",
			s.session.TruncateToLSN, truncatedToLSN)
	}
	s.session.TruncateRecorded = true
	return nil
}

// BeginCatchUp transitions the session from handshake to catch-up phase.
// Mutates: session.Phase → PhaseCatchUp. Sender.State → StateCatchingUp.
// Initializes budget tracker with startTick (pass 0 if no budget enforcement).
// Rejects: wrong sessionID, wrong phase.
func (s *Sender) BeginCatchUp(sessionID uint64, startTick ...uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return err
	}
	if !s.session.Advance(PhaseCatchUp) {
		return fmt.Errorf("cannot begin catch-up: session phase=%s", s.session.Phase)
	}
	s.State = StateCatchingUp
	if len(startTick) > 0 {
		s.session.Tracker.StartTick = startTick[0]
		s.session.Tracker.LastProgressTick = startTick[0]
	}
	return nil
}

// RecordCatchUpProgress records catch-up progress (highest LSN recovered).
// Mutates: session.RecoveredTo (monotonic only), session.Tracker.EntriesReplayed.
// Rejects: wrong sessionID, wrong phase, progress regression, invalidated session.
func (s *Sender) RecordCatchUpProgress(sessionID uint64, recoveredTo uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Phase != PhaseCatchUp {
		return fmt.Errorf("cannot record progress: session phase=%s, want catchup", s.session.Phase)
	}
	if recoveredTo <= s.session.RecoveredTo {
		return fmt.Errorf("progress regression: current=%d proposed=%d", s.session.RecoveredTo, recoveredTo)
	}
	s.session.UpdateProgress(recoveredTo)
	s.session.Tracker.EntriesReplayed++
	return nil
}

// RecordCatchUpProgressAt is like RecordCatchUpProgress but also records the tick
// for budget stall detection.
func (s *Sender) RecordCatchUpProgressAt(sessionID uint64, recoveredTo uint64, tick uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return err
	}
	if s.session.Phase != PhaseCatchUp {
		return fmt.Errorf("cannot record progress: session phase=%s, want catchup", s.session.Phase)
	}
	if recoveredTo <= s.session.RecoveredTo {
		return fmt.Errorf("progress regression: current=%d proposed=%d", s.session.RecoveredTo, recoveredTo)
	}
	s.session.UpdateProgress(recoveredTo)
	s.session.Tracker.EntriesReplayed++
	s.session.Tracker.LastProgressTick = tick
	return nil
}

// CheckBudget evaluates the session's catch-up budget at the current tick.
// Returns BudgetOK if within bounds, or the specific violation.
// If a violation is detected, the session is invalidated and the sender
// transitions to NeedsRebuild.
func (s *Sender) CheckBudget(sessionID uint64, currentTick uint64) (BudgetViolation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkSessionAuthority(sessionID); err != nil {
		return BudgetOK, err
	}
	if s.session.Budget == nil {
		return BudgetOK, nil // no budget enforcement
	}
	violation := s.session.Budget.Check(s.session.Tracker, currentTick)
	if violation != BudgetOK {
		s.session.invalidate(fmt.Sprintf("budget_%s", violation))
		s.session = nil
		s.State = StateNeedsRebuild
	}
	return violation, nil
}

// checkSessionAuthority validates that the sender has an active session
// matching the given ID. Must be called with s.mu held.
func (s *Sender) checkSessionAuthority(sessionID uint64) error {
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
		return fmt.Errorf("session %d is no longer active (phase=%s)", sessionID, s.session.Phase)
	}
	return nil
}

// InvalidateSession invalidates the current session with a reason.
// Transitions the sender to the given target state.
func (s *Sender) InvalidateSession(reason string, targetState ReplicaState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != nil {
		s.session.invalidate(reason)
		s.session = nil
	}
	s.State = targetState
}

// Stop shuts down the sender and any active session.
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

// Stopped returns true if the sender has been stopped.
func (s *Sender) Stopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}
