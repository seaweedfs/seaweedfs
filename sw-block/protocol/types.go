// Package protocol implements the v2 sync/recovery protocol engine.
//
// Design principles:
//   - Deterministic, side-effect free: event in → state + commands + projection out
//   - Primary decides everything: catchup vs rebuild based on replica-reported facts
//   - Replica only reports facts and executes contracts
//   - One threshold: applied_lsn >= wal_tail → WAL catch-up, otherwise → rebuild
//
// Three authority layers:
//   - Assignment: master → identity (who is primary, replica set, epoch)
//   - Session: primary → per-replica recovery contract (keepup/catchup/rebuild)
//   - Projection: primary → derived volume mode/health
//
// Reference: Ceph peering (log-recovery vs backfill on last_update >= log_tail),
// Mayastor (control-plane-driven rebuild, nexus never self-escalates),
// Longhorn (controller-driven PrepareRebuild, replica is passive).
package protocol

// --- Roles and Modes ---

type Role string

const (
	RolePrimary Role = "primary"
	RoleReplica Role = "replica"
	RoleNone    Role = ""
)

type ModeName string

const (
	ModeAllocatedOnly    ModeName = "allocated_only"
	ModeBootstrapPending ModeName = "bootstrap_pending"
	ModePublishHealthy   ModeName = "publish_healthy"
	ModeReplicaReady     ModeName = "replica_ready"
	ModeDegraded         ModeName = "degraded"
	ModeNeedsRebuild     ModeName = "needs_rebuild"
)

// --- Session ---

type SessionKind string

const (
	SessionNone    SessionKind = ""
	SessionKeepUp  SessionKind = "keepup"
	SessionCatchUp SessionKind = "catchup"
	SessionRebuild SessionKind = "rebuild"
)

type SessionState string

const (
	SessionStateIdle      SessionState = "idle"
	SessionStateIssued    SessionState = "issued"
	SessionStateRunning   SessionState = "running"
	SessionStateCompleted SessionState = "completed"
	SessionStateFailed    SessionState = "failed"
)

// --- Sync Ack ---

// SyncAck is what the replica returns in response to a sync request.
// The replica only reports facts. The primary decides what to do.
type SyncAck struct {
	DurableLSN  uint64 // barrier-confirmed durable boundary
	AppliedLSN  uint64 // last WAL entry applied locally
	ReceivedLSN uint64 // last WAL entry received (may not be applied yet)
	WALTail     uint64 // oldest retained WAL entry on replica
	Recoverable bool   // replica's self-assessment: can it still catch up?
	Reason      string // if not recoverable, why
}

// --- Replica State (primary's view) ---

type ReplicaView struct {
	ReplicaID   string
	Endpoint    Endpoint
	Session     ReplicaSession
	LastSyncAck SyncAck
}

type ReplicaSession struct {
	Kind      SessionKind
	State     SessionState
	StartLSN  uint64
	TargetLSN uint64
	PinLSN    uint64
	Progress  uint64 // last reported progress during session
	Reason    string // failure reason if failed
}

type Endpoint struct {
	DataAddr string
	CtrlAddr string
}

// --- Volume State ---

type VolumeState struct {
	VolumeID string
	Epoch    uint64
	Role     Role

	// Assignment-level.
	Replicas []ReplicaAssignment

	// Readiness (host-observed).
	Readiness Readiness

	// Per-replica state (primary-owned).
	ReplicaStates map[string]*ReplicaView

	// Boundaries.
	DurableLSN    uint64 // highest barrier-confirmed LSN
	WALTail       uint64 // primary's oldest retained WAL entry
	WALHead       uint64 // primary's newest WAL entry

	// Derived.
	Mode        ModeName
	ModeReason  string
	Healthy     bool
}

type ReplicaAssignment struct {
	ReplicaID string
	Endpoint  Endpoint
}

type Readiness struct {
	Assigned          bool
	RoleApplied       bool
	ReceiverReady     bool
	ShipperConfigured bool
	ShipperConnected  bool
}

// --- Commands (emitted by engine, executed by host) ---

type Command interface{ commandMarker() }

type ApplyRoleCommand struct {
	VolumeID string
	Epoch    uint64
	Role     Role
}

type ConfigureShipperCommand struct {
	VolumeID string
	Replicas []ReplicaAssignment
}

type StartReceiverCommand struct {
	VolumeID string
}

type IssueCatchUpCommand struct {
	VolumeID  string
	ReplicaID string
	StartLSN  uint64
	TargetLSN uint64
	PinLSN    uint64
}

type IssueRebuildCommand struct {
	VolumeID  string
	ReplicaID string
	TargetLSN uint64
}

type PublishProjectionCommand struct {
	VolumeID string
	Mode     ModeName
	Reason   string
	Healthy  bool
}

func (ApplyRoleCommand) commandMarker()          {}
func (ConfigureShipperCommand) commandMarker()    {}
func (StartReceiverCommand) commandMarker()       {}
func (IssueCatchUpCommand) commandMarker()        {}
func (IssueRebuildCommand) commandMarker()        {}
func (PublishProjectionCommand) commandMarker()    {}

// --- Events (fed into engine by host) ---

type Event interface{ volumeID() string }

// AssignmentDelivered: master assigned identity.
type AssignmentDelivered struct {
	VolumeID string
	Epoch    uint64
	Role     Role
	Replicas []ReplicaAssignment
}

// ReadinessObserved: host reports a readiness fact.
type ReadinessObserved struct {
	VolumeID          string
	RoleApplied       *bool
	ReceiverReady     *bool
	ShipperConfigured *bool
	ShipperConnected  *bool
}

// SyncAckReceived: replica responded to a sync request.
// Primary uses this to decide keepup/catchup/rebuild.
type SyncAckReceived struct {
	VolumeID string
	ReplicaID string
	Ack       SyncAck
	PrimaryWALTail uint64 // primary's WAL tail at the time of sync
	PrimaryWALHead uint64 // primary's WAL head at the time of sync
}

// SessionProgress: replica reports progress during catchup/rebuild.
type SessionProgress struct {
	VolumeID  string
	ReplicaID string
	Progress  uint64
}

// SessionCompleted: replica finished its recovery session.
type SessionCompleted struct {
	VolumeID   string
	ReplicaID  string
	DurableLSN uint64
}

// SessionFailed: replica's recovery session failed.
type SessionFailed struct {
	VolumeID  string
	ReplicaID string
	Reason    string
}

// BarrierConfirmed: durability fence succeeded (from SyncCache path).
type BarrierConfirmed struct {
	VolumeID   string
	DurableLSN uint64
}

func (e AssignmentDelivered) volumeID() string { return e.VolumeID }
func (e ReadinessObserved) volumeID() string   { return e.VolumeID }
func (e SyncAckReceived) volumeID() string     { return e.VolumeID }
func (e SessionProgress) volumeID() string     { return e.VolumeID }
func (e SessionCompleted) volumeID() string    { return e.VolumeID }
func (e SessionFailed) volumeID() string       { return e.VolumeID }
func (e BarrierConfirmed) volumeID() string    { return e.VolumeID }
