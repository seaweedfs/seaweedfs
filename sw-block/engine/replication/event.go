package replication

// Event is one bounded input into the Phase 14 core skeleton.
// Events describe observation or intent; they do not perform side effects.
type Event interface {
	VolumeID() string
}

// AssignmentDelivered carries the desired local role and replica set.
type AssignmentDelivered struct {
	ID       string
	Epoch    uint64
	Role     VolumeRole
	Replicas []ReplicaAssignment
}

func (e AssignmentDelivered) VolumeID() string { return e.ID }

// RoleApplied confirms the local runtime applied the desired role.
type RoleApplied struct {
	ID string
}

func (e RoleApplied) VolumeID() string { return e.ID }

// ReceiverReadyObserved confirms replica receiver readiness.
type ReceiverReadyObserved struct {
	ID string
}

func (e ReceiverReadyObserved) VolumeID() string { return e.ID }

// ShipperConfiguredObserved confirms shipper wiring exists.
type ShipperConfiguredObserved struct {
	ID string
}

func (e ShipperConfiguredObserved) VolumeID() string { return e.ID }

// ShipperConnectedObserved confirms the shipper is connected.
type ShipperConnectedObserved struct {
	ID string
}

func (e ShipperConnectedObserved) VolumeID() string { return e.ID }

// DiagnosticShippedAdvanced updates sender-side diagnostic progress only.
type DiagnosticShippedAdvanced struct {
	ID         string
	ShippedLSN uint64
}

func (e DiagnosticShippedAdvanced) VolumeID() string { return e.ID }

// BarrierAccepted advances authoritative durable progress.
type BarrierAccepted struct {
	ID         string
	FlushedLSN uint64
}

func (e BarrierAccepted) VolumeID() string { return e.ID }

// BarrierRejected marks the bounded path degraded until a later success clears it.
type BarrierRejected struct {
	ID     string
	Reason string
}

func (e BarrierRejected) VolumeID() string { return e.ID }

// CheckpointAdvanced updates the durable base-image boundary.
type CheckpointAdvanced struct {
	ID            string
	CheckpointLSN uint64
}

func (e CheckpointAdvanced) VolumeID() string { return e.ID }

// NeedsRebuildObserved is a fail-closed rebuild escalation.
type NeedsRebuildObserved struct {
	ID     string
	Reason string
}

func (e NeedsRebuildObserved) VolumeID() string { return e.ID }

// RebuildCommitted clears the rebuild condition with bounded durable truth.
type RebuildCommitted struct {
	ID            string
	FlushedLSN    uint64
	CheckpointLSN uint64
}

func (e RebuildCommitted) VolumeID() string { return e.ID }
