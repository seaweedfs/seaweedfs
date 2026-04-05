package protocolv2

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ReplicaMember identifies one replica in the low-frequency identity loop.
// It carries identity and transport only, never replication progress.
type ReplicaMember struct {
	NodeID   string
	DataAddr string
	CtrlAddr string
}

// Assignment is the Loop 1 assignment surface from masterv2 to volumev2.
type Assignment struct {
	Name          string
	Path          string
	NodeID        string
	Epoch         uint64
	LeaseTTL      time.Duration
	CreateOptions blockvol.CreateOptions
	Role          string
	ReplicaSet    []ReplicaMember
}

// VolumeHeartbeat is the periodic identity-loop heartbeat surface.
// It stays small and carries only applied identity, compressed outward mode,
// and a passive committed-boundary cache.
type VolumeHeartbeat struct {
	Name         string
	Path         string
	Epoch        uint64
	Role         string
	Mode         string
	ModeReason   string
	CommittedLSN uint64
	RoleApplied  bool
	ReplicaReady bool
}

// NodeHeartbeat is the low-frequency periodic heartbeat from a volume node.
type NodeHeartbeat struct {
	NodeID     string
	ReportedAt time.Time
	Volumes    []VolumeHeartbeat
}

// PromotionQueryRequest asks one candidate for fresh failover evidence.
type PromotionQueryRequest struct {
	VolumeName    string
	ExpectedEpoch uint64
}

// PromotionQueryResponse returns fresh candidate evidence at query time.
type PromotionQueryResponse struct {
	VolumeName    string
	NodeID        string
	Epoch         uint64
	Role          string
	CommittedLSN  uint64
	WALHeadLSN    uint64
	ReceiverReady bool
	Eligible      bool
	Reason        string
}
