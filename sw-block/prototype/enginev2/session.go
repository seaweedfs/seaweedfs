package enginev2

import (
	"sync"
	"sync/atomic"
)

// SessionKind identifies how the recovery session was created.
type SessionKind string

const (
	SessionBootstrap  SessionKind = "bootstrap"  // fresh replica, no prior state
	SessionCatchUp    SessionKind = "catchup"     // WAL gap recovery
	SessionRebuild    SessionKind = "rebuild"     // full extent + WAL rebuild
	SessionReassign   SessionKind = "reassign"    // address change recovery
)

// SessionPhase tracks progress within a recovery session.
type SessionPhase string

const (
	PhaseInit       SessionPhase = "init"
	PhaseConnecting SessionPhase = "connecting"
	PhaseHandshake  SessionPhase = "handshake"
	PhaseCatchUp    SessionPhase = "catchup"
	PhaseCompleted  SessionPhase = "completed"
	PhaseInvalidated SessionPhase = "invalidated"
)

// sessionIDCounter generates unique session IDs across all senders.
var sessionIDCounter atomic.Uint64

// RecoverySession represents one recovery attempt for a specific replica
// at a specific epoch. It is owned by a Sender and has exclusive authority
// to transition the replica through connecting → handshake → catchup → complete.
//
// Each session has a unique ID. Stale completions are rejected by ID, not
// by pointer comparison. This prevents old sessions from mutating state
// even if they retain a reference to the sender.
//
// Lifecycle rules:
//   - At most one active session per Sender
//   - Session is bound to an epoch; epoch bump invalidates it
//   - Session is bound to an endpoint; address change invalidates it
//   - Completed sessions release ownership back to the Sender
//   - Invalidated sessions are dead and cannot be reused
type RecoverySession struct {
	mu sync.Mutex

	ID               uint64 // unique, monotonic, never reused
	ReplicaID        string
	Epoch            uint64
	Kind             SessionKind
	Phase            SessionPhase
	InvalidateReason string // non-empty when invalidated

	// Progress tracking.
	StartLSN        uint64 // gap start (exclusive)
	TargetLSN       uint64 // gap end (inclusive)
	FrozenTargetLSN uint64 // frozen at BeginCatchUp — catch-up will not chase beyond this
	RecoveredTo     uint64 // highest LSN recovered so far

	// Truncation tracking: set when replica has divergent tail beyond committed.
	TruncateRequired bool   // true if replica FlushedLSN > CommittedLSN at handshake
	TruncateToLSN    uint64 // truncate entries beyond this LSN
	TruncateRecorded bool   // true after truncation is confirmed

	// Bounded CatchUp budget (Phase 4.5).
	Budget  *CatchUpBudget // nil = no budget enforcement
	Tracker BudgetCheck    // runtime consumption

	// Rebuild state (Phase 4.5). Non-nil when Kind == SessionRebuild.
	Rebuild *RebuildState
}

func newRecoverySession(replicaID string, epoch uint64, kind SessionKind) *RecoverySession {
	rs := &RecoverySession{
		ID:        sessionIDCounter.Add(1),
		ReplicaID: replicaID,
		Epoch:     epoch,
		Kind:      kind,
		Phase:     PhaseInit,
	}
	if kind == SessionRebuild {
		rs.Rebuild = NewRebuildState()
	}
	return rs
}

// Active returns true if the session has not been completed or invalidated.
func (rs *RecoverySession) Active() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.Phase != PhaseCompleted && rs.Phase != PhaseInvalidated
}

// validTransitions defines the allowed phase transitions.
// Each phase maps to the set of phases it can transition to.
var validTransitions = map[SessionPhase]map[SessionPhase]bool{
	PhaseInit:       {PhaseConnecting: true, PhaseInvalidated: true},
	PhaseConnecting: {PhaseHandshake: true, PhaseInvalidated: true},
	PhaseHandshake:  {PhaseCatchUp: true, PhaseCompleted: true, PhaseInvalidated: true},
	PhaseCatchUp:    {PhaseCompleted: true, PhaseInvalidated: true},
}

// Advance moves the session to the next phase. Returns false if the
// transition is not valid (wrong source phase, already terminal, or
// illegal jump). Enforces the lifecycle:
//
//	init → connecting → handshake → catchup → completed
//	                                        ↘ invalidated (from any non-terminal)
func (rs *RecoverySession) Advance(phase SessionPhase) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.Phase == PhaseCompleted || rs.Phase == PhaseInvalidated {
		return false
	}
	allowed := validTransitions[rs.Phase]
	if !allowed[phase] {
		return false
	}
	rs.Phase = phase
	return true
}

// UpdateProgress records catch-up progress. Returns false if stale.
func (rs *RecoverySession) UpdateProgress(recoveredTo uint64) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.Phase == PhaseCompleted || rs.Phase == PhaseInvalidated {
		return false
	}
	if recoveredTo > rs.RecoveredTo {
		rs.RecoveredTo = recoveredTo
	}
	return true
}

// SetRange sets the recovery LSN range.
func (rs *RecoverySession) SetRange(start, target uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.StartLSN = start
	rs.TargetLSN = target
}

// Converged returns true if recovery has reached the target.
func (rs *RecoverySession) Converged() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.TargetLSN > 0 && rs.RecoveredTo >= rs.TargetLSN
}

func (rs *RecoverySession) complete() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.Phase = PhaseCompleted
}

func (rs *RecoverySession) invalidate(reason string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.Phase == PhaseCompleted || rs.Phase == PhaseInvalidated {
		return
	}
	rs.Phase = PhaseInvalidated
	rs.InvalidateReason = reason
}
