package topology

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// taskState represents the current state of tasks affecting the topology (internal)
type taskState struct {
	VolumeID     uint32     `json:"volume_id"`
	TaskType     TaskType   `json:"task_type"`
	SourceServer string     `json:"source_server"`
	SourceDisk   uint32     `json:"source_disk"`
	TargetServer string     `json:"target_server,omitempty"`
	TargetDisk   uint32     `json:"target_disk,omitempty"`
	Status       TaskStatus `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  time.Time  `json:"completed_at,omitempty"`

	// Storage impact information
	SourceStorageChange StorageSlotChange `json:"source_storage_change"` // Change in storage on source disk
	TargetStorageChange StorageSlotChange `json:"target_storage_change"` // Change in storage on target disk
	EstimatedSize       int64             `json:"estimated_size"`        // Estimated size of data being processed
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
	TargetNode     string   `json:"target_node"`
	TargetDisk     uint32   `json:"target_disk"`
	TargetRack     string   `json:"target_rack"`
	TargetDC       string   `json:"target_dc"`
	ExpectedSize   uint64   `json:"expected_size"`
	PlacementScore float64  `json:"placement_score"`
	Conflicts      []string `json:"conflicts"`
}

// MultiDestinationPlan represents multiple planned destinations for operations like EC
type MultiDestinationPlan struct {
	Plans          []*DestinationPlan `json:"plans"`
	TotalShards    int                `json:"total_shards"`
	SuccessfulRack int                `json:"successful_racks"`
	SuccessfulDCs  int                `json:"successful_dcs"`
}
