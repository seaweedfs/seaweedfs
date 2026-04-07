package replication

// RuntimeAuthority names which runtime currently owns the integrated path.
// Phase 14 starts by making the V2 core explicit, but integrated tests still
// evaluate the constrained current runtime until a live cutover exists.
type RuntimeAuthority string

const (
	RuntimeAuthorityConstrainedV1 RuntimeAuthority = "constrained_v1"
	RuntimeAuthorityV2Core        RuntimeAuthority = "v2_core"
)

// VolumeRole tracks the local role the core is normalizing.
type VolumeRole string

const (
	RoleUnknown VolumeRole = "unknown"
	RolePrimary VolumeRole = "primary"
	RoleReplica VolumeRole = "replica"
)

// ModeName is the normalized outward mode vocabulary for the bounded chosen path.
type ModeName string

const (
	ModeAllocatedOnly    ModeName = "allocated_only"
	ModeBootstrapPending ModeName = "bootstrap_pending"
	ModeReplicaReady     ModeName = "replica_ready"
	ModePublishHealthy   ModeName = "publish_healthy"
	ModeDegraded         ModeName = "degraded"
	ModeNeedsRebuild     ModeName = "needs_rebuild"
)

// ReadinessView captures bounded readiness truth for the current chosen path.
type ReadinessView struct {
	Assigned          bool
	RoleApplied       bool
	ReceiverReady     bool
	ShipperConfigured bool
	ShipperConnected  bool
	ReplicaReady      bool
}

// BoundaryView captures durable/publication boundary truth. DurableLSN is the
// authority for replicated durability; diagnostic shipped progress is separate.
type BoundaryView struct {
	CommittedLSN         uint64
	DurableLSN           uint64
	CheckpointLSN        uint64
	TargetLSN            uint64
	AchievedLSN          uint64
	DiagnosticShippedLSN uint64
	LastBarrierOK        bool
	LastBarrierReason    string
}

// ModeView is the bounded external mode meaning derived from the current state.
type ModeView struct {
	Name      ModeName
	Reason    string
	Authority RuntimeAuthority
}

// PublicationView is the semantic owner for outward publication truth. Other
// projections may derive convenience fields from it, but they must not become
// parallel authorities.
type PublicationView struct {
	Healthy bool
	Reason  string
}

type RecoveryPhase string

const (
	RecoveryIdle         RecoveryPhase = "idle"
	RecoveryCatchingUp   RecoveryPhase = "catching_up"
	RecoveryNeedsRebuild RecoveryPhase = "needs_rebuild"
	RecoveryRebuilding   RecoveryPhase = "rebuilding"
)

// RecoveryView captures the bounded recovery truth the core owns directly.
type RecoveryView struct {
	Phase       RecoveryPhase
	TargetLSN   uint64
	AchievedLSN uint64
	Reason      string
}

// SyncAckKind captures the transport/control result of one sync request. The
// primary derives recovery action from this ack plus the attached facts.
type SyncAckKind string

const (
	SyncAckUnknown       SyncAckKind = ""
	SyncAckQuorum        SyncAckKind = "quorum"
	SyncAckTimedOut      SyncAckKind = "timed_out"
	SyncAckTransportLost SyncAckKind = "transport_lost"
	SyncAckEpochMismatch SyncAckKind = "epoch_mismatch"
)

// SyncAction captures the primary-owned session decision derived from sync ack
// facts. It is not replica-owned protocol input.
type SyncAction string

const (
	SyncActionKeepUp  SyncAction = "keepup"
	SyncActionCatchUp SyncAction = "catchup"
	SyncActionRebuild SyncAction = "rebuild"
)

// SyncView keeps the latest sync ack facts plus the primary-owned session
// decision derived from those facts. It remains distinct from durable boundary
// truth and recovery execution progress.
type SyncView struct {
	AckKind        SyncAckKind
	Action         SyncAction
	TargetLSN      uint64
	PrimaryTailLSN uint64
	DurableLSN     uint64
	AppliedLSN     uint64
	Reason         string
}

// ReplicaSyncView stores the latest sync ack facts for each replica the primary
// is currently tracking.
type ReplicaSyncView map[string]SyncView

type commandState struct {
	RoleEpoch             uint64
	Role                  VolumeRole
	ReceiverStartEpoch    uint64
	ShipperConfigEpoch    uint64
	ShipperConfigReplicas []ReplicaAssignment
	RecoveryTaskEpoch     uint64
	RecoveryTaskTargets   map[string]SessionKind
	SessionTargets        map[string]sessionCommandTarget
	InvalidationIssued    bool
	InvalidationReason    string
}

type sessionCommandTarget struct {
	Kind      SessionKind
	TargetLSN uint64
}

type sessionObservation struct {
	Kind        SessionKind
	Phase       RecoveryPhase
	TargetLSN   uint64
	AchievedLSN uint64
	Reason      string
	Completed   bool
}

// VolumeState is the minimal V2-core-owned state for one volume.
//
// Ownership split:
//   - Assignment fields normalize master-owned identity truth.
//   - Sync/ReplicaSync plus catch-up observations normalize primary-owned
//     session truth.
//   - Mode/Publication are derived projection only; they are never assigned by
//     master or replica.
type VolumeState struct {
	VolumeID string
	Epoch    uint64
	Role     VolumeRole

	DesiredReplicas []ReplicaAssignment
	Readiness       ReadinessView
	Boundary        BoundaryView
	Mode            ModeView
	Publication     PublicationView
	Recovery        RecoveryView
	Sync            SyncView
	ReplicaSync     ReplicaSyncView

	degraded       bool
	degradeReason  string
	recoveryTarget SessionKind
	commands       commandState
	sessions       map[string]sessionObservation
}

func newVolumeState(volumeID string) *VolumeState {
	return &VolumeState{
		VolumeID: volumeID,
		Role:     RoleUnknown,
		Mode: ModeView{
			Name:      ModeAllocatedOnly,
			Authority: RuntimeAuthorityConstrainedV1,
		},
		Recovery: RecoveryView{
			Phase: RecoveryIdle,
		},
	}
}

// Snapshot returns a detached copy of the state for external inspection/tests.
func (s *VolumeState) Snapshot() VolumeState {
	out := *s
	if s.DesiredReplicas != nil {
		out.DesiredReplicas = append([]ReplicaAssignment(nil), s.DesiredReplicas...)
	}
	if s.commands.ShipperConfigReplicas != nil {
		out.commands.ShipperConfigReplicas = append([]ReplicaAssignment(nil), s.commands.ShipperConfigReplicas...)
	}
	if s.commands.RecoveryTaskTargets != nil {
		out.commands.RecoveryTaskTargets = make(map[string]SessionKind, len(s.commands.RecoveryTaskTargets))
		for replicaID, kind := range s.commands.RecoveryTaskTargets {
			out.commands.RecoveryTaskTargets[replicaID] = kind
		}
	}
	if s.commands.SessionTargets != nil {
		out.commands.SessionTargets = make(map[string]sessionCommandTarget, len(s.commands.SessionTargets))
		for replicaID, target := range s.commands.SessionTargets {
			out.commands.SessionTargets[replicaID] = target
		}
	}
	if s.sessions != nil {
		out.sessions = make(map[string]sessionObservation, len(s.sessions))
		for replicaID, obs := range s.sessions {
			out.sessions[replicaID] = obs
		}
	}
	if s.ReplicaSync != nil {
		out.ReplicaSync = make(ReplicaSyncView, len(s.ReplicaSync))
		for replicaID, syncView := range s.ReplicaSync {
			out.ReplicaSync[replicaID] = syncView
		}
	}
	return out
}
