package replication

import "sync/atomic"

// sessionIDCounter generates globally unique session IDs.
var sessionIDCounter atomic.Uint64

// Session represents one recovery attempt for a specific replica at a
// specific epoch. All mutable state is unexported — external code interacts
// through Sender execution APIs only.
type Session struct {
	id               uint64
	replicaID        string
	epoch            uint64
	kind             SessionKind
	phase            SessionPhase
	invalidateReason string

	startLSN        uint64
	targetLSN       uint64
	frozenTargetLSN uint64
	recoveredTo     uint64

	truncateRequired bool
	truncateToLSN    uint64
	truncateRecorded bool

	budget  *CatchUpBudget
	tracker BudgetCheck

	rebuild *RebuildState
}

func newSession(replicaID string, epoch uint64, kind SessionKind) *Session {
	s := &Session{
		id:        sessionIDCounter.Add(1),
		replicaID: replicaID,
		epoch:     epoch,
		kind:      kind,
		phase:     PhaseInit,
	}
	if kind == SessionRebuild {
		s.rebuild = NewRebuildState()
	}
	return s
}

// Read-only accessors.

func (s *Session) ID() uint64              { return s.id }
func (s *Session) ReplicaID() string       { return s.replicaID }
func (s *Session) Epoch() uint64           { return s.epoch }
func (s *Session) Kind() SessionKind       { return s.kind }
func (s *Session) Phase() SessionPhase     { return s.phase }
func (s *Session) InvalidateReason() string { return s.invalidateReason }
func (s *Session) StartLSN() uint64        { return s.startLSN }
func (s *Session) TargetLSN() uint64       { return s.targetLSN }
func (s *Session) FrozenTargetLSN() uint64 { return s.frozenTargetLSN }
func (s *Session) RecoveredTo() uint64     { return s.recoveredTo }

// Active returns true if the session is not completed or invalidated.
func (s *Session) Active() bool {
	return s.phase != PhaseCompleted && s.phase != PhaseInvalidated
}

// Converged returns true if recovery reached the target.
func (s *Session) Converged() bool {
	return s.targetLSN > 0 && s.recoveredTo >= s.targetLSN
}

// Internal mutation methods — called by Sender under its lock.

func (s *Session) advance(phase SessionPhase) bool {
	if !s.Active() {
		return false
	}
	if !validTransitions[s.phase][phase] {
		return false
	}
	s.phase = phase
	return true
}

func (s *Session) setRange(start, target uint64) {
	s.startLSN = start
	s.targetLSN = target
	// Initialize recoveredTo to startLSN so that delta-based entry counting
	// in RecordCatchUpProgress measures only the actual catch-up work,
	// not the replica's pre-existing prefix.
	s.recoveredTo = start
}

func (s *Session) updateProgress(recoveredTo uint64) {
	if recoveredTo > s.recoveredTo {
		s.recoveredTo = recoveredTo
	}
}

func (s *Session) complete() {
	s.phase = PhaseCompleted
}

func (s *Session) invalidate(reason string) {
	if !s.Active() {
		return
	}
	s.phase = PhaseInvalidated
	s.invalidateReason = reason
}
