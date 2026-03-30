package replication

import "sync/atomic"

// sessionIDCounter generates globally unique session IDs.
var sessionIDCounter atomic.Uint64

// Session represents one recovery attempt for a specific replica at a
// specific epoch. It is owned by a Sender and gated by session ID at
// every execution step.
//
// Lifecycle:
//   - Created via Sender.AttachSession or Sender.SupersedeSession
//   - Advanced through phases: init → connecting → handshake → catchup → completed
//   - Invalidated by: epoch bump, endpoint change, sender stop, timeout
//   - Stale sessions (wrong ID) are rejected at every execution API
type Session struct {
	ID               uint64
	ReplicaID        string
	Epoch            uint64
	Kind             SessionKind
	Phase            SessionPhase
	InvalidateReason string

	// Progress tracking.
	StartLSN        uint64 // gap start (exclusive)
	TargetLSN       uint64 // gap end (inclusive)
	FrozenTargetLSN uint64 // frozen at BeginCatchUp — catch-up will not chase beyond
	RecoveredTo     uint64 // highest LSN recovered so far

	// Truncation.
	TruncateRequired bool
	TruncateToLSN    uint64
	TruncateRecorded bool

	// Budget (nil = no enforcement).
	Budget  *CatchUpBudget
	Tracker BudgetCheck

	// Rebuild state (non-nil when Kind == SessionRebuild).
	Rebuild *RebuildState
}

func newSession(replicaID string, epoch uint64, kind SessionKind) *Session {
	s := &Session{
		ID:        sessionIDCounter.Add(1),
		ReplicaID: replicaID,
		Epoch:     epoch,
		Kind:      kind,
		Phase:     PhaseInit,
	}
	if kind == SessionRebuild {
		s.Rebuild = NewRebuildState()
	}
	return s
}

// Active returns true if the session is not completed or invalidated.
func (s *Session) Active() bool {
	return s.Phase != PhaseCompleted && s.Phase != PhaseInvalidated
}

// Advance moves to the next phase. Returns false if the transition is invalid.
func (s *Session) Advance(phase SessionPhase) bool {
	if !s.Active() {
		return false
	}
	if !validTransitions[s.Phase][phase] {
		return false
	}
	s.Phase = phase
	return true
}

// SetRange sets the recovery LSN range.
func (s *Session) SetRange(start, target uint64) {
	s.StartLSN = start
	s.TargetLSN = target
}

// UpdateProgress records catch-up progress (monotonic).
func (s *Session) UpdateProgress(recoveredTo uint64) {
	if recoveredTo > s.RecoveredTo {
		s.RecoveredTo = recoveredTo
	}
}

// Converged returns true if recovery reached the target.
func (s *Session) Converged() bool {
	return s.TargetLSN > 0 && s.RecoveredTo >= s.TargetLSN
}

func (s *Session) complete() {
	s.Phase = PhaseCompleted
}

func (s *Session) invalidate(reason string) {
	if !s.Active() {
		return
	}
	s.Phase = PhaseInvalidated
	s.InvalidateReason = reason
}
