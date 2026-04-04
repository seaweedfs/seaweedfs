// control.go implements the real control-plane delivery bridge.
// Converts BlockVolumeAssignment (from master heartbeat) into V2 engine
// AssignmentIntent using stable server identity from the master registry.
//
// Identity rule: ReplicaID = <volume-path>/<server-id>
// ServerID comes from BlockVolumeAssignment.ReplicaServerID or
// ReplicaAddr.ServerID — NOT derived from transport addresses.
package v2bridge

import (
	"log"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ControlBridge converts real BlockVolumeAssignment into V2 engine intents.
type ControlBridge struct {
	adapter *bridge.ControlAdapter
}

func NewControlBridge() *ControlBridge {
	return &ControlBridge{adapter: bridge.NewControlAdapter()}
}

// ConvertAssignment converts a real BlockVolumeAssignment into an engine intent.
// localServerID is the identity of the local volume server (for replica/rebuild roles).
func (cb *ControlBridge) ConvertAssignment(a blockvol.BlockVolumeAssignment, localServerID string) engine.AssignmentIntent {
	role := blockvol.RoleFromWire(a.Role)
	volumeName := a.Path

	switch role {
	case blockvol.RolePrimary:
		return cb.convertPrimaryAssignment(a, volumeName)
	case blockvol.RoleReplica:
		return cb.convertReplicaAssignment(a, volumeName, localServerID)
	case blockvol.RoleRebuilding:
		return cb.convertRebuildAssignment(a, volumeName, localServerID)
	default:
		return engine.AssignmentIntent{Epoch: a.Epoch}
	}
}

func (cb *ControlBridge) convertPrimaryAssignment(a blockvol.BlockVolumeAssignment, volumeName string) engine.AssignmentIntent {
	primary := bridge.MasterAssignment{
		VolumeName: volumeName,
		Epoch:      a.Epoch,
		Role:       "primary",
	}

	var replicas []bridge.MasterAssignment
	if len(a.ReplicaAddrs) > 0 {
		for _, ra := range a.ReplicaAddrs {
			if ra.ServerID == "" {
				log.Printf("v2bridge: skipping replica with empty ServerID (data=%s)", ra.DataAddr)
				continue // fail closed: skip replicas without stable identity
			}
			replicas = append(replicas, bridge.MasterAssignment{
				VolumeName:      volumeName,
				Epoch:           a.Epoch,
				Role:            "replica",
				ReplicaServerID: ra.ServerID,
				DataAddr:        ra.DataAddr,
				CtrlAddr:        ra.CtrlAddr,
			})
		}
	} else if a.ReplicaServerID != "" && a.ReplicaDataAddr != "" {
		// Scalar RF=2 path with explicit ServerID.
		replicas = append(replicas, bridge.MasterAssignment{
			VolumeName:      volumeName,
			Epoch:           a.Epoch,
			Role:            "replica",
			ReplicaServerID: a.ReplicaServerID,
			DataAddr:        a.ReplicaDataAddr,
			CtrlAddr:        a.ReplicaCtrlAddr,
		})
	} else if a.ReplicaDataAddr != "" {
		log.Printf("v2bridge: scalar replica assignment without ServerID (data=%s) — skipping", a.ReplicaDataAddr)
		// Fail closed: do not create address-derived identity.
	}

	return cb.adapter.ToAssignmentIntent(primary, replicas)
}

func (cb *ControlBridge) convertReplicaAssignment(a blockvol.BlockVolumeAssignment, volumeName, localServerID string) engine.AssignmentIntent {
	replica := bridge.ReplicaAssignmentForServer(volumeName, localServerID, engine.Endpoint{
		DataAddr: a.ReplicaDataAddr,
		CtrlAddr: a.ReplicaCtrlAddr,
	})
	return engine.AssignmentIntent{
		Epoch:    a.Epoch,
		Replicas: []engine.ReplicaAssignment{replica},
	}
}

func (cb *ControlBridge) convertRebuildAssignment(a blockvol.BlockVolumeAssignment, volumeName, localServerID string) engine.AssignmentIntent {
	replica := bridge.ReplicaAssignmentForServer(volumeName, localServerID, engine.Endpoint{
		DataAddr: a.ReplicaDataAddr,
		CtrlAddr: a.ReplicaCtrlAddr,
	})
	return engine.AssignmentIntent{
		Epoch:    a.Epoch,
		Replicas: []engine.ReplicaAssignment{replica},
		RecoveryTargets: map[string]engine.SessionKind{
			replica.ReplicaID: bridge.RecoveryTargetForRole("rebuilding"),
		},
	}
}
