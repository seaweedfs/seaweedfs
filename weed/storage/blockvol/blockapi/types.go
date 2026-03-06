package blockapi

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// CreateVolumeRequest is the request body for POST /block/volume.
type CreateVolumeRequest struct {
	Name             string `json:"name"`
	SizeBytes        uint64 `json:"size_bytes"`
	ReplicaPlacement string `json:"replica_placement"` // SeaweedFS placement string: "000", "001", "010", "100"
	DiskType         string `json:"disk_type"`         // e.g. "ssd", "hdd"
}

// VolumeInfo describes a block volume.
type VolumeInfo struct {
	Name             string `json:"name"`
	VolumeServer     string `json:"volume_server"`
	SizeBytes        uint64 `json:"size_bytes"`
	ReplicaPlacement string `json:"replica_placement,omitempty"`
	Epoch            uint64 `json:"epoch"`
	Role             string `json:"role"`
	Status           string `json:"status"`
	ISCSIAddr        string `json:"iscsi_addr"`
	IQN              string `json:"iqn"`
	ReplicaServer    string `json:"replica_server,omitempty"`
	ReplicaISCSIAddr string `json:"replica_iscsi_addr,omitempty"`
	ReplicaIQN       string `json:"replica_iqn,omitempty"`
	ReplicaDataAddr  string `json:"replica_data_addr,omitempty"`
	ReplicaCtrlAddr  string `json:"replica_ctrl_addr,omitempty"`
	// CP8-2: Multi-replica fields.
	ReplicaFactor   int             `json:"replica_factor"`
	Replicas        []ReplicaDetail `json:"replicas,omitempty"`
	HealthScore     float64         `json:"health_score"`
	ReplicaDegraded bool            `json:"replica_degraded,omitempty"`
}

// ReplicaDetail describes one replica in the API response.
type ReplicaDetail struct {
	Server      string  `json:"server"`
	ISCSIAddr   string  `json:"iscsi_addr,omitempty"`
	IQN         string  `json:"iqn,omitempty"`
	HealthScore float64 `json:"health_score"`
	WALLag      uint64  `json:"wal_lag,omitempty"`
}

// AssignRequest is the request body for POST /block/assign.
type AssignRequest struct {
	Name       string `json:"name"`
	Epoch      uint64 `json:"epoch"`
	Role       string `json:"role"`         // "primary" | "replica"
	LeaseTTLMs uint64 `json:"lease_ttl_ms"` // lease TTL in milliseconds
}

// ServerInfo describes a block-capable volume server.
type ServerInfo struct {
	Address      string `json:"address"`
	VolumeCount  int    `json:"volume_count"`
	BlockCapable bool   `json:"block_capable"`
}

// RoleFromString converts a role string to its uint32 wire value.
// Returns 0 (RoleNone) for unrecognized strings.
func RoleFromString(s string) uint32 {
	switch strings.ToLower(s) {
	case "primary":
		return uint32(blockvol.RolePrimary)
	case "replica":
		return uint32(blockvol.RoleReplica)
	case "stale":
		return uint32(blockvol.RoleStale)
	case "rebuilding":
		return uint32(blockvol.RoleRebuilding)
	case "draining":
		return uint32(blockvol.RoleDraining)
	default:
		return uint32(blockvol.RoleNone)
	}
}
