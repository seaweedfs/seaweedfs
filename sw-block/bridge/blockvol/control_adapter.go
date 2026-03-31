package blockvol

import (
	"fmt"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// MasterAssignment represents a block-volume assignment from the master,
// as delivered via heartbeat response. This is the raw input from the
// existing master_grpc_server / block_heartbeat_loop path.
type MasterAssignment struct {
	VolumeName      string // e.g., "pvc-data-1"
	Epoch           uint64
	Role            string // "primary", "replica", "rebuilding"
	PrimaryServerID string // which server is the primary
	ReplicaServerID string // which server is this replica
	DataAddr        string // replica's current data address
	CtrlAddr        string // replica's current control address
	AddrVersion     uint64 // bumped on address change
}

// ControlAdapter converts master assignments into engine AssignmentIntent.
// Identity mapping: ReplicaID = <volume-name>/<replica-server-id>.
// This adapter does NOT decide recovery policy — it only translates
// master role/state into engine SessionKind.
type ControlAdapter struct{}

// NewControlAdapter creates a control adapter.
func NewControlAdapter() *ControlAdapter {
	return &ControlAdapter{}
}

// MakeReplicaID derives a stable engine ReplicaID from volume + server identity.
// NOT derived from any address field.
func MakeReplicaID(volumeName, serverID string) string {
	return fmt.Sprintf("%s/%s", volumeName, serverID)
}

// ToAssignmentIntent converts a master assignment into an engine intent.
// The adapter maps role transitions to SessionKind but does NOT decide
// the actual recovery outcome (that's the engine's job).
func (ca *ControlAdapter) ToAssignmentIntent(primary MasterAssignment, replicas []MasterAssignment) engine.AssignmentIntent {
	intent := engine.AssignmentIntent{
		Epoch: primary.Epoch,
	}

	for _, r := range replicas {
		replicaID := MakeReplicaID(r.VolumeName, r.ReplicaServerID)
		intent.Replicas = append(intent.Replicas, engine.ReplicaAssignment{
			ReplicaID: replicaID,
			Endpoint: engine.Endpoint{
				DataAddr: r.DataAddr,
				CtrlAddr: r.CtrlAddr,
				Version:  r.AddrVersion,
			},
		})

		// Map role to recovery intent (if needed).
		kind := mapRoleToSessionKind(r.Role)
		if kind != "" {
			if intent.RecoveryTargets == nil {
				intent.RecoveryTargets = map[string]engine.SessionKind{}
			}
			intent.RecoveryTargets[replicaID] = kind
		}
	}

	return intent
}

// mapRoleToSessionKind maps a master-assigned role to an engine SessionKind.
// This is a pure translation — NO policy decision.
func mapRoleToSessionKind(role string) engine.SessionKind {
	switch role {
	case "replica":
		return engine.SessionCatchUp // default recovery for reconnecting replicas
	case "rebuilding":
		return engine.SessionRebuild
	default:
		return "" // no recovery needed (primary, or unknown)
	}
}
