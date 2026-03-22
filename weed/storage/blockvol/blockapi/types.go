package blockapi

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// CreateVolumeRequest is the request body for POST /block/volume.
type CreateVolumeRequest struct {
	Name             string `json:"name"`
	SizeBytes        uint64 `json:"size_bytes"`
	ReplicaPlacement string `json:"replica_placement"`          // SeaweedFS placement string: "000", "001", "010", "100"
	DiskType         string `json:"disk_type"`                  // e.g. "ssd", "hdd"
	DurabilityMode   string `json:"durability_mode,omitempty"`  // "best_effort", "sync_all", "sync_quorum"
	ReplicaFactor    int    `json:"replica_factor,omitempty"`   // 1, 2, or 3 (default: 2)
	Preset           string `json:"preset,omitempty"`           // "database", "general", "throughput", or ""
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
	DurabilityMode  string          `json:"durability_mode"` // CP8-3-1
	Preset          string          `json:"preset,omitempty"` // CP11B-1: preset used at creation
	NvmeAddr        string          `json:"nvme_addr,omitempty"`
	NQN             string          `json:"nqn,omitempty"`
}

// ResolvedPolicyResponse is the response for POST /block/volume/resolve.
type ResolvedPolicyResponse struct {
	Policy    ResolvedPolicyView `json:"policy"`
	Overrides []string           `json:"overrides,omitempty"`
	Warnings  []string           `json:"warnings,omitempty"`
	Errors    []string           `json:"errors,omitempty"`
}

// ResolvedPolicyView is the fully resolved policy shown to the user.
type ResolvedPolicyView struct {
	Preset              string `json:"preset,omitempty"`
	DurabilityMode      string `json:"durability_mode"`
	ReplicaFactor       int    `json:"replica_factor"`
	DiskType            string `json:"disk_type,omitempty"`
	TransportPreference string `json:"transport_preference"`
	WorkloadHint        string `json:"workload_hint"`
	WALSizeRecommended  uint64 `json:"wal_size_recommended"`
	StorageProfile      string `json:"storage_profile"`
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

// ExpandVolumeRequest is the request body for POST /block/volume/{name}/expand.
type ExpandVolumeRequest struct {
	NewSizeBytes uint64 `json:"new_size_bytes"`
}

// ExpandVolumeResponse is the response for POST /block/volume/{name}/expand.
type ExpandVolumeResponse struct {
	CapacityBytes uint64 `json:"capacity_bytes"`
}

// PromoteVolumeRequest is the request body for POST /block/volume/{name}/promote.
type PromoteVolumeRequest struct {
	TargetServer string `json:"target_server,omitempty"` // specific replica, or empty for auto
	Force        bool   `json:"force,omitempty"`         // bypass soft safety checks
	Reason       string `json:"reason,omitempty"`        // audit note
}

// PromoteVolumeResponse is the response for POST /block/volume/{name}/promote.
type PromoteVolumeResponse struct {
	NewPrimary string               `json:"new_primary"`
	Epoch      uint64               `json:"epoch"`
	Reason     string               `json:"reason,omitempty"`      // rejection reason if failed
	Rejections []PreflightRejection `json:"rejections,omitempty"`  // per-replica rejection details
}

// BlockStatusResponse is the response for GET /block/status.
type BlockStatusResponse struct {
	VolumeCount          int    `json:"volume_count"`
	ServerCount          int    `json:"server_count"`
	PromotionLSNTolerance uint64 `json:"promotion_lsn_tolerance"`
	BarrierLagLSN        uint64 `json:"barrier_lag_lsn"`
	PromotionsTotal      int64  `json:"promotions_total"`
	FailoversTotal       int64  `json:"failovers_total"`
	RebuildsTotal        int64  `json:"rebuilds_total"`
	AssignmentQueueDepth int    `json:"assignment_queue_depth"`
}

// PreflightRejection describes why a specific replica was rejected for promotion.
type PreflightRejection struct {
	Server string `json:"server"`
	Reason string `json:"reason"` // "stale_heartbeat", "wal_lag", "wrong_role", "server_dead", "no_heartbeat"
}

// PreflightResponse is the response for GET /block/volume/{name}/preflight.
type PreflightResponse struct {
	VolumeName      string                `json:"volume_name"`
	Promotable      bool                  `json:"promotable"`
	Reason          string                `json:"reason,omitempty"`
	CandidateServer string                `json:"candidate_server,omitempty"`
	CandidateHealth float64               `json:"candidate_health,omitempty"`
	CandidateWALLSN uint64                `json:"candidate_wal_lsn,omitempty"`
	Rejections      []PreflightRejection  `json:"rejections,omitempty"`
	PrimaryServer   string                `json:"primary_server"`
	PrimaryAlive    bool                  `json:"primary_alive"`
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
