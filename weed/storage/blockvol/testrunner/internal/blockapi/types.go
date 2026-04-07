// Package blockapi provides HTTP client types for the master's block volume REST API.
// This is a standalone copy of weed/storage/blockvol/blockapi for use by the test runner,
// decoupled from the engine package. The canonical source remains blockvol/blockapi.
package blockapi

// CreateVolumeRequest is the request body for POST /block/volume.
type CreateVolumeRequest struct {
	Name             string `json:"name"`
	SizeBytes        uint64 `json:"size_bytes"`
	WALSizeBytes     uint64 `json:"wal_size_bytes,omitempty"`
	ReplicaPlacement string `json:"replica_placement"`
	DiskType         string `json:"disk_type"`
	DurabilityMode   string `json:"durability_mode,omitempty"`
	ReplicaFactor    int    `json:"replica_factor,omitempty"`
	Preset           string `json:"preset,omitempty"`
}

// VolumeInfo describes a block volume.
type VolumeInfo struct {
	Name             string          `json:"name"`
	VolumeServer     string          `json:"volume_server"`
	SizeBytes        uint64          `json:"size_bytes"`
	ReplicaPlacement string          `json:"replica_placement,omitempty"`
	Epoch            uint64          `json:"epoch"`
	Role             string          `json:"role"`
	Status           string          `json:"status"`
	ISCSIAddr        string          `json:"iscsi_addr"`
	IQN              string          `json:"iqn"`
	ReplicaServer    string          `json:"replica_server,omitempty"`
	ReplicaISCSIAddr string          `json:"replica_iscsi_addr,omitempty"`
	ReplicaIQN       string          `json:"replica_iqn,omitempty"`
	ReplicaDataAddr  string          `json:"replica_data_addr,omitempty"`
	ReplicaCtrlAddr  string          `json:"replica_ctrl_addr,omitempty"`
	ReplicaFactor    int             `json:"replica_factor"`
	Replicas         []ReplicaDetail `json:"replicas,omitempty"`
	ReplicaReady     bool            `json:"replica_ready,omitempty"`
	HealthScore      float64         `json:"health_score"`
	ReplicaDegraded  bool            `json:"replica_degraded,omitempty"`
	DurabilityMode   string          `json:"durability_mode"`
	Preset           string          `json:"preset,omitempty"`
	NvmeAddr         string          `json:"nvme_addr,omitempty"`
	NQN              string          `json:"nqn,omitempty"`
	VolumeMode       string          `json:"volume_mode,omitempty"` // CP13-9
	VolumeModeReason string          `json:"volume_mode_reason,omitempty"`
}

// ReplicaDetail describes one replica in the API response.
type ReplicaDetail struct {
	Server      string  `json:"server"`
	ISCSIAddr   string  `json:"iscsi_addr,omitempty"`
	IQN         string  `json:"iqn,omitempty"`
	Ready       bool    `json:"ready,omitempty"`
	HealthScore float64 `json:"health_score"`
	WALLag      uint64  `json:"wal_lag,omitempty"`
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
	TargetServer string `json:"target_server,omitempty"`
	Force        bool   `json:"force,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

// PromoteVolumeResponse is the response for POST /block/volume/{name}/promote.
type PromoteVolumeResponse struct {
	NewPrimary string               `json:"new_primary"`
	Epoch      uint64               `json:"epoch"`
	Reason     string               `json:"reason,omitempty"`
	Rejections []PreflightRejection `json:"rejections,omitempty"`
}

// BlockStatusResponse is the response for GET /block/status.
type BlockStatusResponse struct {
	VolumeCount           int    `json:"volume_count"`
	ServerCount           int    `json:"server_count"`
	PromotionLSNTolerance uint64 `json:"promotion_lsn_tolerance"`
	BarrierLagLSN         uint64 `json:"barrier_lag_lsn"`
	PromotionsTotal       int64  `json:"promotions_total"`
	FailoversTotal        int64  `json:"failovers_total"`
	RebuildsTotal         int64  `json:"rebuilds_total"`
	AssignmentQueueDepth  int    `json:"assignment_queue_depth"`
}

// PreflightRejection describes why a specific replica was rejected for promotion.
type PreflightRejection struct {
	Server string `json:"server"`
	Reason string `json:"reason"`
}

// PreflightResponse is the response for GET /block/volume/{name}/preflight.
type PreflightResponse struct {
	VolumeName      string               `json:"volume_name"`
	Promotable      bool                 `json:"promotable"`
	Reason          string               `json:"reason,omitempty"`
	CandidateServer string               `json:"candidate_server,omitempty"`
	CandidateHealth float64              `json:"candidate_health,omitempty"`
	CandidateWALLSN uint64               `json:"candidate_wal_lsn,omitempty"`
	Rejections      []PreflightRejection `json:"rejections,omitempty"`
	PrimaryServer   string               `json:"primary_server"`
	PrimaryAlive    bool                 `json:"primary_alive"`
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

// VolumePlanResponse is the response for POST /block/volume/plan.
type VolumePlanResponse struct {
	ResolvedPolicy ResolvedPolicyView `json:"resolved_policy"`
	Plan           VolumePlanView     `json:"plan"`
	Warnings       []string           `json:"warnings,omitempty"`
	Errors         []string           `json:"errors,omitempty"`
}

// VolumePlanView describes the placement plan.
type VolumePlanView struct {
	Primary    string                `json:"primary"`
	Replicas   []string              `json:"replicas,omitempty"`
	Candidates []string              `json:"candidates"`
	Rejections []VolumePlanRejection `json:"rejections,omitempty"`
}

// VolumePlanRejection explains why a candidate server was not selected.
type VolumePlanRejection struct {
	Server string `json:"server"`
	Reason string `json:"reason"`
}
