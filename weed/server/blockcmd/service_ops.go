package blockcmd

import (
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// BackendOps owns the concrete backend-facing operations that remain separate
// from server-side command orchestration.
type BackendOps interface {
	ApplyRole(assignment blockvol.BlockVolumeAssignment) (bool, error)
	StartReceiver(assignment blockvol.BlockVolumeAssignment) (bool, error)
	ConfigureShipper(volumeID string, replicas []engine.ReplicaAssignment) (executed bool, shipperConnected bool, err error)
}

// RecoveryCoordinator is the runtime recovery surface used by command ops.
type RecoveryCoordinator interface {
	StartRecoveryTask(replicaID string, assignments []blockvol.BlockVolumeAssignment)
	ExecutePendingCatchUp(volumeID string, targetLSN uint64) error
	ExecutePendingRebuild(volumeID string, targetLSN uint64) error
}

// ProjectionReader provides access to current core publication state.
type ProjectionReader interface {
	Projection(volumeID string) (engine.PublicationProjection, bool)
}

// SessionInvalidator invalidates an active sender session.
type SessionInvalidator interface {
	InvalidateSession(reason string, targetState engine.ReplicaState)
}

// SenderResolver resolves one sender/session invalidator by replica ID.
type SenderResolver func(replicaID string) SessionInvalidator

// ServiceOps composes backend ops with runtime/server dependencies to satisfy
// the dispatcher Ops interface without requiring a full BlockService pointer.
type ServiceOps struct {
	backend    BackendOps
	recovery   RecoveryCoordinator
	projection ProjectionReader
	senderByID SenderResolver
}

func NewServiceOps(
	backend BackendOps,
	recovery RecoveryCoordinator,
	projection ProjectionReader,
	senderByID SenderResolver,
) *ServiceOps {
	return &ServiceOps{
		backend:    backend,
		recovery:   recovery,
		projection: projection,
		senderByID: senderByID,
	}
}

func (ops *ServiceOps) ApplyRole(assignment blockvol.BlockVolumeAssignment) (bool, error) {
	if ops == nil || ops.backend == nil {
		return false, nil
	}
	return ops.backend.ApplyRole(assignment)
}

func (ops *ServiceOps) StartReceiver(assignment blockvol.BlockVolumeAssignment) (bool, error) {
	if ops == nil || ops.backend == nil {
		return false, nil
	}
	return ops.backend.StartReceiver(assignment)
}

func (ops *ServiceOps) ConfigureShipper(volumeID string, replicas []engine.ReplicaAssignment) (bool, bool, error) {
	if ops == nil || ops.backend == nil {
		return false, false, nil
	}
	return ops.backend.ConfigureShipper(volumeID, replicas)
}

func (ops *ServiceOps) StartRecoveryTask(replicaID string, assignment blockvol.BlockVolumeAssignment) (bool, error) {
	if ops == nil || ops.recovery == nil {
		return false, nil
	}
	ops.recovery.StartRecoveryTask(replicaID, []blockvol.BlockVolumeAssignment{assignment})
	return true, nil
}

func (ops *ServiceOps) InvalidateSession(volumeID, reason string) (bool, error) {
	if ops == nil || ops.projection == nil || ops.senderByID == nil {
		return false, nil
	}
	proj, ok := ops.projection.Projection(volumeID)
	if !ok {
		return false, nil
	}
	for _, replicaID := range proj.ReplicaIDs {
		sender := ops.senderByID(replicaID)
		if sender == nil {
			continue
		}
		sender.InvalidateSession(reason, engine.StateDisconnected)
	}
	return true, nil
}

func (ops *ServiceOps) StartCatchUp(volumeID string, targetLSN uint64) (bool, error) {
	if ops == nil || ops.recovery == nil {
		return false, nil
	}
	if err := ops.recovery.ExecutePendingCatchUp(volumeID, targetLSN); err != nil {
		return false, err
	}
	return true, nil
}

func (ops *ServiceOps) StartRebuild(volumeID string, targetLSN uint64) (bool, error) {
	if ops == nil || ops.recovery == nil {
		return false, nil
	}
	if err := ops.recovery.ExecutePendingRebuild(volumeID, targetLSN); err != nil {
		return false, err
	}
	return true, nil
}
