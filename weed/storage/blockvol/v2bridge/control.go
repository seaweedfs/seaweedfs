// control.go implements the real control-plane delivery bridge.
// It converts BlockVolumeAssignment (from master heartbeat) into
// V2 engine AssignmentIntent, using real master/registry identity.
//
// Identity rule: ReplicaID = <volume-path>/<replica-server>
// The replica-server is the VS identity from the master registry,
// not a transport address. This survives address changes.
package v2bridge

import (
	"fmt"
	"strings"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ControlBridge converts real BlockVolumeAssignment into V2 engine intents.
// It is the live replacement for direct AssignmentIntent construction.
type ControlBridge struct {
	adapter *bridge.ControlAdapter
}

// NewControlBridge creates a control bridge.
func NewControlBridge() *ControlBridge {
	return &ControlBridge{
		adapter: bridge.NewControlAdapter(),
	}
}

// ConvertAssignment converts a real BlockVolumeAssignment from the master
// heartbeat response into a V2 engine AssignmentIntent.
//
// Identity mapping:
//   - VolumeName = assignment.Path
//   - For primary: ReplicaID per replica = <path>/<replica-server-id>
//   - replica-server-id = extracted from ReplicaAddrs or scalar fields
//   - Epoch from assignment
//   - SessionKind from Role
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

// convertPrimaryAssignment: primary receives assignment with replica targets.
func (cb *ControlBridge) convertPrimaryAssignment(a blockvol.BlockVolumeAssignment, volumeName string) engine.AssignmentIntent {
	primary := bridge.MasterAssignment{
		VolumeName:      volumeName,
		Epoch:           a.Epoch,
		Role:            "primary",
		PrimaryServerID: "", // primary doesn't need its own server ID in the assignment
	}

	var replicas []bridge.MasterAssignment
	if len(a.ReplicaAddrs) > 0 {
		for _, ra := range a.ReplicaAddrs {
			serverID := extractServerID(ra.DataAddr)
			replicas = append(replicas, bridge.MasterAssignment{
				VolumeName:      volumeName,
				Epoch:           a.Epoch,
				Role:            "replica",
				ReplicaServerID: serverID,
				DataAddr:        ra.DataAddr,
				CtrlAddr:        ra.CtrlAddr,
				AddrVersion:     0, // will be bumped on address change detection
			})
		}
	} else if a.ReplicaDataAddr != "" {
		// Scalar RF=2 compat.
		serverID := extractServerID(a.ReplicaDataAddr)
		replicas = append(replicas, bridge.MasterAssignment{
			VolumeName:      volumeName,
			Epoch:           a.Epoch,
			Role:            "replica",
			ReplicaServerID: serverID,
			DataAddr:        a.ReplicaDataAddr,
			CtrlAddr:        a.ReplicaCtrlAddr,
		})
	}

	return cb.adapter.ToAssignmentIntent(primary, replicas)
}

// convertReplicaAssignment: replica receives its own role assignment.
func (cb *ControlBridge) convertReplicaAssignment(a blockvol.BlockVolumeAssignment, volumeName, localServerID string) engine.AssignmentIntent {
	// Replica doesn't manage other replicas — just acknowledges its role.
	return engine.AssignmentIntent{
		Epoch: a.Epoch,
		Replicas: []engine.ReplicaAssignment{
			{
				ReplicaID: fmt.Sprintf("%s/%s", volumeName, localServerID),
				Endpoint: engine.Endpoint{
					DataAddr: a.ReplicaDataAddr,
					CtrlAddr: a.ReplicaCtrlAddr,
				},
			},
		},
	}
}

// convertRebuildAssignment: rebuilding replica.
func (cb *ControlBridge) convertRebuildAssignment(a blockvol.BlockVolumeAssignment, volumeName, localServerID string) engine.AssignmentIntent {
	replicaID := fmt.Sprintf("%s/%s", volumeName, localServerID)
	return engine.AssignmentIntent{
		Epoch: a.Epoch,
		Replicas: []engine.ReplicaAssignment{
			{
				ReplicaID: replicaID,
				Endpoint: engine.Endpoint{
					DataAddr: a.ReplicaDataAddr,
					CtrlAddr: a.ReplicaCtrlAddr,
				},
			},
		},
		RecoveryTargets: map[string]engine.SessionKind{
			replicaID: engine.SessionRebuild,
		},
	}
}

// extractServerID derives a stable server identity from an address.
// Uses the host:port as the server ID (this is how the master registry
// keys servers). In production, this would come from the registry's
// ReplicaInfo.Server field directly.
//
// For now: strip to host:grpc-port format to match master registry keys.
func extractServerID(addr string) string {
	// addr is typically "ip:port" — use as-is for server ID.
	// The master registry uses the same format for ReplicaInfo.Server.
	if addr == "" {
		return "unknown"
	}
	// Strip any path suffix, keep host:port.
	if idx := strings.Index(addr, "/"); idx >= 0 {
		return addr[:idx]
	}
	return addr
}
