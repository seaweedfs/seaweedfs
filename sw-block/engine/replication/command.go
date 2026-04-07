package replication

// Command is one side-effect-free decision emitted by the Phase 14 core skeleton.
// Adapters execute commands later; the core only decides them.
type Command interface {
	commandName() string
}

type ApplyRoleCommand struct {
	VolumeID string
	Epoch    uint64
	Role     VolumeRole
}

func (ApplyRoleCommand) commandName() string { return "apply_role" }

type StartReceiverCommand struct {
	VolumeID string
}

func (StartReceiverCommand) commandName() string { return "start_receiver" }

type ConfigureShipperCommand struct {
	VolumeID string
	Replicas []ReplicaAssignment
}

func (ConfigureShipperCommand) commandName() string { return "configure_shipper" }

type StartRecoveryTaskCommand struct {
	VolumeID  string
	ReplicaID string
	Kind      SessionKind
}

// StartRecoveryTaskCommand realizes the primary-owned session executor for one
// replica after assignment has established membership.
func (StartRecoveryTaskCommand) commandName() string { return "start_recovery_task" }

type DrainRecoveryTaskCommand struct {
	VolumeID  string
	ReplicaID string
	Reason    string
}

func (DrainRecoveryTaskCommand) commandName() string { return "drain_recovery_task" }

type StartCatchUpCommand struct {
	VolumeID  string
	ReplicaID string
	TargetLSN uint64
}

func (StartCatchUpCommand) commandName() string { return "start_catchup" }

type StartRebuildCommand struct {
	VolumeID  string
	ReplicaID string
	TargetLSN uint64
}

func (StartRebuildCommand) commandName() string { return "start_rebuild" }

type InvalidateSessionCommand struct {
	VolumeID  string
	ReplicaID string
	Reason    string
}

func (InvalidateSessionCommand) commandName() string { return "invalidate_session" }

type PublishProjectionCommand struct {
	VolumeID   string
	Projection PublicationProjection
}

// PublishProjectionCommand emits the derived outward summary for the volume.
func (PublishProjectionCommand) commandName() string { return "publish_projection" }
