package blockcmd

import (
	"fmt"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// Ops executes concrete backend-integrated command operations.
// It does not own command dispatch or host-side event semantics.
type Ops interface {
	ApplyRole(assignment blockvol.BlockVolumeAssignment) (bool, error)
	StartReceiver(assignment blockvol.BlockVolumeAssignment) (bool, error)
	ConfigureShipper(volumeID string, replicas []engine.ReplicaAssignment) (executed bool, shipperConnected bool, err error)
	StartRecoveryTask(replicaID string, assignment blockvol.BlockVolumeAssignment) (bool, error)
	InvalidateSession(volumeID, reason string) (bool, error)
	StartCatchUp(replicaID string, targetLSN uint64) (bool, error)
	StartRebuild(replicaID string, targetLSN uint64) (bool, error)
}

// HostEffects applies server-adapter side effects after concrete command
// execution succeeds. These effects stay out of the backend-binding layer.
type HostEffects interface {
	RecordCommand(volumeID, name string)
	EmitCoreEvent(ev engine.Event)
	PublishProjection(volumeID string, projection engine.PublicationProjection) error
}

// Dispatcher runs engine commands against concrete ops and host effects.
type Dispatcher struct {
	ops     Ops
	effects HostEffects
}

func NewDispatcher(ops Ops, effects HostEffects) *Dispatcher {
	return &Dispatcher{ops: ops, effects: effects}
}

func (d *Dispatcher) Run(cmds []engine.Command, assignment *blockvol.BlockVolumeAssignment) error {
	for _, cmd := range cmds {
		switch v := cmd.(type) {
		case engine.ApplyRoleCommand:
			a, ok, err := requireAssignment("apply_role", v.VolumeID, assignment)
			if err != nil || !ok {
				return err
			}
			executed, err := d.ops.ApplyRole(a)
			if err != nil {
				return err
			}
			if executed {
				d.effects.RecordCommand(v.VolumeID, "apply_role")
			}
		case engine.StartReceiverCommand:
			a, ok, err := requireAssignment("start_receiver", v.VolumeID, assignment)
			if err != nil || !ok {
				return err
			}
			if a.ReplicaDataAddr == "" || a.ReplicaCtrlAddr == "" {
				continue
			}
			executed, err := d.ops.StartReceiver(a)
			if err != nil {
				return err
			}
			if executed {
				d.effects.RecordCommand(v.VolumeID, "start_receiver")
				d.effects.EmitCoreEvent(engine.ReceiverReadyObserved{ID: v.VolumeID})
			}
		case engine.ConfigureShipperCommand:
			executed, connected, err := d.ops.ConfigureShipper(v.VolumeID, v.Replicas)
			if err != nil {
				return err
			}
			if executed {
				d.effects.RecordCommand(v.VolumeID, "configure_shipper")
				d.effects.EmitCoreEvent(engine.ShipperConfiguredObserved{ID: v.VolumeID})
				if connected {
					d.effects.EmitCoreEvent(engine.ShipperConnectedObserved{ID: v.VolumeID})
				}
			}
		case engine.StartRecoveryTaskCommand:
			a, ok, err := requireAssignment("start_recovery_task", v.VolumeID, assignment)
			if err != nil || !ok {
				return err
			}
			if v.ReplicaID == "" {
				continue
			}
			executed, err := d.ops.StartRecoveryTask(v.ReplicaID, a)
			if err != nil {
				return err
			}
			if executed {
				d.effects.RecordCommand(v.VolumeID, "start_recovery_task")
			}
		case engine.InvalidateSessionCommand:
			executed, err := d.ops.InvalidateSession(v.VolumeID, v.Reason)
			if err != nil {
				return err
			}
			if executed {
				d.effects.RecordCommand(v.VolumeID, "invalidate_session")
			}
		case engine.StartCatchUpCommand:
			if v.ReplicaID == "" {
				continue
			}
			executed, err := d.ops.StartCatchUp(v.ReplicaID, v.TargetLSN)
			if err != nil {
				return err
			}
			if executed {
				d.effects.RecordCommand(v.VolumeID, "start_catchup")
			}
		case engine.StartRebuildCommand:
			if v.ReplicaID == "" {
				continue
			}
			executed, err := d.ops.StartRebuild(v.ReplicaID, v.TargetLSN)
			if err != nil {
				return err
			}
			if executed {
				d.effects.RecordCommand(v.VolumeID, "start_rebuild")
			}
		case engine.PublishProjectionCommand:
			if err := d.effects.PublishProjection(v.VolumeID, v.Projection); err != nil {
				return fmt.Errorf("publish projection %s: %w", v.VolumeID, err)
			}
		}
	}
	return nil
}

func requireAssignment(action, volumeID string, assignment *blockvol.BlockVolumeAssignment) (blockvol.BlockVolumeAssignment, bool, error) {
	if assignment == nil {
		return blockvol.BlockVolumeAssignment{}, false, nil
	}
	if assignment.Path != volumeID {
		return blockvol.BlockVolumeAssignment{}, false, fmt.Errorf("block service: core %s path mismatch %q != %q", action, assignment.Path, volumeID)
	}
	return *assignment, true, nil
}
