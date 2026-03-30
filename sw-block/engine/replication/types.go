package replication

// Endpoint represents a replica's network identity. Version is bumped on
// address change; the Sender uses version comparison (not string comparison
// alone) to detect endpoint changes.
type Endpoint struct {
	DataAddr string
	CtrlAddr string
	Version  uint64
}

// Changed reports whether ep differs from other in any address or version field.
func (ep Endpoint) Changed(other Endpoint) bool {
	return ep.DataAddr != other.DataAddr ||
		ep.CtrlAddr != other.CtrlAddr ||
		ep.Version != other.Version
}

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

// SessionKind identifies how the recovery session was created.
type SessionKind string

const (
	SessionBootstrap SessionKind = "bootstrap"
	SessionCatchUp   SessionKind = "catchup"
	SessionRebuild   SessionKind = "rebuild"
	SessionReassign  SessionKind = "reassign"
)

// SessionPhase tracks progress within a recovery session.
type SessionPhase string

const (
	PhaseInit        SessionPhase = "init"
	PhaseConnecting  SessionPhase = "connecting"
	PhaseHandshake   SessionPhase = "handshake"
	PhaseCatchUp     SessionPhase = "catchup"
	PhaseCompleted   SessionPhase = "completed"
	PhaseInvalidated SessionPhase = "invalidated"
)

// validTransitions defines the allowed phase transitions.
var validTransitions = map[SessionPhase]map[SessionPhase]bool{
	PhaseInit:       {PhaseConnecting: true, PhaseInvalidated: true},
	PhaseConnecting: {PhaseHandshake: true, PhaseInvalidated: true},
	PhaseHandshake:  {PhaseCatchUp: true, PhaseCompleted: true, PhaseInvalidated: true},
	PhaseCatchUp:    {PhaseCompleted: true, PhaseInvalidated: true},
}
