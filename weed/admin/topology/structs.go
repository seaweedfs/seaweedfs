package topology

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TaskSource represents a single source in a multi-source task (for replicated volume cleanup)
type TaskSource struct {
	SourceServer  string            `json:"source_server"`
	SourceDisk    uint32            `json:"source_disk"`
	StorageChange StorageSlotChange `json:"storage_change"` // Storage impact on this source
	EstimatedSize int64             `json:"estimated_size"` // Estimated size for this source
}

// TaskDestination represents a single destination in a multi-destination task
type TaskDestination struct {
	TargetServer  string            `json:"target_server"`
	TargetDisk    uint32            `json:"target_disk"`
	StorageChange StorageSlotChange `json:"storage_change"` // Storage impact on this destination
	EstimatedSize int64             `json:"estimated_size"` // Estimated size for this destination
}

// taskState represents the current state of tasks affecting the topology (internal)
// Uses unified multi-source/multi-destination design:
// - Single-source tasks (balance, vacuum, replication): 1 source, 1 destination
// - Multi-source EC tasks (replicated volumes): N sources, M destinations
type taskState struct {
	VolumeID      uint32     `json:"volume_id"`
	TaskType      TaskType   `json:"task_type"`
	Status        TaskStatus `json:"status"`
	StartedAt     time.Time  `json:"started_at"`
	CompletedAt   time.Time  `json:"completed_at,omitempty"`
	EstimatedSize int64      `json:"estimated_size"` // Total estimated size of task

	// Unified source and destination arrays (always used)
	Sources      []TaskSource      `json:"sources"`      // Source locations (1+ for all task types)
	Destinations []TaskDestination `json:"destinations"` // Destination locations (1+ for all task types)
}

// DiskInfo represents a disk with its current state and ongoing tasks (public for external access)
type DiskInfo struct {
	NodeID     string              `json:"node_id"`
	DiskID     uint32              `json:"disk_id"`
	DiskType   string              `json:"disk_type"`
	DataCenter string              `json:"data_center"`
	Rack       string              `json:"rack"`
	DiskInfo   *master_pb.DiskInfo `json:"disk_info"`
	LoadCount  int                 `json:"load_count"` // Number of active tasks
}

// activeDisk represents internal disk state (private)
type activeDisk struct {
	*DiskInfo
	pendingTasks  []*taskState
	assignedTasks []*taskState
	recentTasks   []*taskState // Completed in last N seconds
}

// activeNode represents a node with its disks (private)
type activeNode struct {
	nodeID     string
	dataCenter string
	rack       string
	nodeInfo   *master_pb.DataNodeInfo
	disks      map[uint32]*activeDisk // DiskID -> activeDisk
}

// ActiveTopology provides a real-time view of cluster state with task awareness
type ActiveTopology struct {
	// Core topology from master
	topologyInfo *master_pb.TopologyInfo
	lastUpdated  time.Time

	// Structured topology for easy access (private)
	nodes map[string]*activeNode // NodeID -> activeNode
	disks map[string]*activeDisk // "NodeID:DiskID" -> activeDisk

	// Performance indexes for O(1) lookups (private)
	volumeIndex  map[uint32][]string // VolumeID -> list of "NodeID:DiskID" where volume replicas exist
	ecShardIndex map[uint32][]string // VolumeID -> list of "NodeID:DiskID" where EC shards exist

	// Task states affecting the topology (private)
	pendingTasks  map[string]*taskState
	assignedTasks map[string]*taskState
	recentTasks   map[string]*taskState

	// Configuration
	recentTaskWindowSeconds int

	// Synchronization
	mutex sync.RWMutex
}

// DestinationPlan represents a planned destination for a volume/shard operation
type DestinationPlan struct {
	TargetNode     string  `json:"target_node"`
	TargetAddress  string  `json:"target_address"`
	TargetDisk     uint32  `json:"target_disk"`
	TargetRack     string  `json:"target_rack"`
	TargetDC       string  `json:"target_dc"`
	ExpectedSize   uint64  `json:"expected_size"`
	PlacementScore float64 `json:"placement_score"`
}

// MultiDestinationPlan represents multiple planned destinations for operations like EC
type MultiDestinationPlan struct {
	Plans          []*DestinationPlan `json:"plans"`
	TotalShards    int                `json:"total_shards"`
	SuccessfulRack int                `json:"successful_racks"`
	SuccessfulDCs  int                `json:"successful_dcs"`
}

// VolumeReplica represents a replica location with server and disk information
type VolumeReplica struct {
	ServerID   string `json:"server_id"`
	DiskID     uint32 `json:"disk_id"`
	DataCenter string `json:"data_center"`
	Rack       string `json:"rack"`
}
