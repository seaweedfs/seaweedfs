package topology

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TaskType represents different types of maintenance operations
type TaskType string

// TaskStatus represents the current status of a task
type TaskStatus string

// Common task type constants
const (
	TaskTypeVacuum        TaskType = "vacuum"
	TaskTypeBalance       TaskType = "balance"
	TaskTypeErasureCoding TaskType = "erasure_coding"
	TaskTypeReplication   TaskType = "replication"
)

// Common task status constants
const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusCompleted  TaskStatus = "completed"
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

// NewActiveTopology creates a new ActiveTopology instance
func NewActiveTopology(recentTaskWindowSeconds int) *ActiveTopology {
	if recentTaskWindowSeconds <= 0 {
		recentTaskWindowSeconds = 10 // Default 10 seconds
	}

	return &ActiveTopology{
		nodes:                   make(map[string]*activeNode),
		disks:                   make(map[string]*activeDisk),
		pendingTasks:            make(map[string]*taskState),
		assignedTasks:           make(map[string]*taskState),
		recentTasks:             make(map[string]*taskState),
		recentTaskWindowSeconds: recentTaskWindowSeconds,
	}
}

// UpdateTopology updates the topology information from master
func (at *ActiveTopology) UpdateTopology(topologyInfo *master_pb.TopologyInfo) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	at.topologyInfo = topologyInfo
	at.lastUpdated = time.Now()

	// Rebuild structured topology
	at.nodes = make(map[string]*activeNode)
	at.disks = make(map[string]*activeDisk)

	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, nodeInfo := range rack.DataNodeInfos {
				node := &activeNode{
					nodeID:     nodeInfo.Id,
					dataCenter: dc.Id,
					rack:       rack.Id,
					nodeInfo:   nodeInfo,
					disks:      make(map[uint32]*activeDisk),
				}

				// Add disks for this node
				for diskType, diskInfo := range nodeInfo.DiskInfos {
					disk := &activeDisk{
						DiskInfo: &DiskInfo{
							NodeID:     nodeInfo.Id,
							DiskID:     diskInfo.DiskId,
							DiskType:   diskType,
							DataCenter: dc.Id,
							Rack:       rack.Id,
							DiskInfo:   diskInfo,
						},
					}

					diskKey := fmt.Sprintf("%s:%d", nodeInfo.Id, diskInfo.DiskId)
					node.disks[diskInfo.DiskId] = disk
					at.disks[diskKey] = disk
				}

				at.nodes[nodeInfo.Id] = node
			}
		}
	}

	// Reassign task states to updated topology
	at.reassignTaskStates()

	glog.V(1).Infof("ActiveTopology updated: %d nodes, %d disks", len(at.nodes), len(at.disks))
	return nil
}

// AddPendingTask adds a pending task to the topology
func (at *ActiveTopology) AddPendingTask(taskID string, taskType TaskType, volumeID uint32,
	sourceServer string, sourceDisk uint32, targetServer string, targetDisk uint32) {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	task := &taskState{
		VolumeID:     volumeID,
		TaskType:     taskType,
		SourceServer: sourceServer,
		SourceDisk:   sourceDisk,
		TargetServer: targetServer,
		TargetDisk:   targetDisk,
		Status:       TaskStatusPending,
		StartedAt:    time.Now(),
	}

	at.pendingTasks[taskID] = task
	at.assignTaskToDisk(task)
}

// AssignTask moves a task from pending to assigned
func (at *ActiveTopology) AssignTask(taskID string) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	task, exists := at.pendingTasks[taskID]
	if !exists {
		return fmt.Errorf("pending task %s not found", taskID)
	}

	delete(at.pendingTasks, taskID)
	task.Status = TaskStatusInProgress
	at.assignedTasks[taskID] = task
	at.reassignTaskStates()

	return nil
}

// CompleteTask moves a task from assigned to recent
func (at *ActiveTopology) CompleteTask(taskID string) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	task, exists := at.assignedTasks[taskID]
	if !exists {
		return fmt.Errorf("assigned task %s not found", taskID)
	}

	delete(at.assignedTasks, taskID)
	task.Status = TaskStatusCompleted
	task.CompletedAt = time.Now()
	at.recentTasks[taskID] = task
	at.reassignTaskStates()

	// Clean up old recent tasks
	at.cleanupRecentTasks()

	return nil
}

// GetAvailableDisks returns disks that can accept new tasks of the given type
func (at *ActiveTopology) GetAvailableDisks(taskType TaskType, excludeNodeID string) []*DiskInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	var available []*DiskInfo

	for _, disk := range at.disks {
		if disk.NodeID == excludeNodeID {
			continue // Skip excluded node
		}

		if at.isDiskAvailable(disk, taskType) {
			// Create a copy with current load count
			diskCopy := *disk.DiskInfo
			diskCopy.LoadCount = len(disk.pendingTasks) + len(disk.assignedTasks)
			available = append(available, &diskCopy)
		}
	}

	return available
}

// GetDiskLoad returns the current load on a disk (number of active tasks)
func (at *ActiveTopology) GetDiskLoad(nodeID string, diskID uint32) int {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	diskKey := fmt.Sprintf("%s:%d", nodeID, diskID)
	disk, exists := at.disks[diskKey]
	if !exists {
		return 0
	}

	return len(disk.pendingTasks) + len(disk.assignedTasks)
}

// HasRecentTaskForVolume checks if a volume had a recent task (to avoid immediate re-detection)
func (at *ActiveTopology) HasRecentTaskForVolume(volumeID uint32, taskType TaskType) bool {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	for _, task := range at.recentTasks {
		if task.VolumeID == volumeID && task.TaskType == taskType {
			return true
		}
	}

	return false
}

// GetAllNodes returns information about all nodes (public interface)
func (at *ActiveTopology) GetAllNodes() map[string]*master_pb.DataNodeInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	result := make(map[string]*master_pb.DataNodeInfo)
	for nodeID, node := range at.nodes {
		result[nodeID] = node.nodeInfo
	}
	return result
}

// GetNodeDisks returns all disks for a specific node
func (at *ActiveTopology) GetNodeDisks(nodeID string) []*DiskInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	node, exists := at.nodes[nodeID]
	if !exists {
		return nil
	}

	var disks []*DiskInfo
	for _, disk := range node.disks {
		diskCopy := *disk.DiskInfo
		diskCopy.LoadCount = len(disk.pendingTasks) + len(disk.assignedTasks)
		disks = append(disks, &diskCopy)
	}

	return disks
}

// Private methods

// reassignTaskStates assigns tasks to the appropriate disks
func (at *ActiveTopology) reassignTaskStates() {
	// Clear existing task assignments
	for _, disk := range at.disks {
		disk.pendingTasks = nil
		disk.assignedTasks = nil
		disk.recentTasks = nil
	}

	// Reassign pending tasks
	for _, task := range at.pendingTasks {
		at.assignTaskToDisk(task)
	}

	// Reassign assigned tasks
	for _, task := range at.assignedTasks {
		at.assignTaskToDisk(task)
	}

	// Reassign recent tasks
	for _, task := range at.recentTasks {
		at.assignTaskToDisk(task)
	}
}

// assignTaskToDisk assigns a task to the appropriate disk(s)
func (at *ActiveTopology) assignTaskToDisk(task *taskState) {
	// Assign to source disk
	sourceKey := fmt.Sprintf("%s:%d", task.SourceServer, task.SourceDisk)
	if sourceDisk, exists := at.disks[sourceKey]; exists {
		switch task.Status {
		case TaskStatusPending:
			sourceDisk.pendingTasks = append(sourceDisk.pendingTasks, task)
		case TaskStatusInProgress:
			sourceDisk.assignedTasks = append(sourceDisk.assignedTasks, task)
		case TaskStatusCompleted:
			sourceDisk.recentTasks = append(sourceDisk.recentTasks, task)
		}
	}

	// Assign to target disk if it exists and is different from source
	if task.TargetServer != "" && (task.TargetServer != task.SourceServer || task.TargetDisk != task.SourceDisk) {
		targetKey := fmt.Sprintf("%s:%d", task.TargetServer, task.TargetDisk)
		if targetDisk, exists := at.disks[targetKey]; exists {
			switch task.Status {
			case TaskStatusPending:
				targetDisk.pendingTasks = append(targetDisk.pendingTasks, task)
			case TaskStatusInProgress:
				targetDisk.assignedTasks = append(targetDisk.assignedTasks, task)
			case TaskStatusCompleted:
				targetDisk.recentTasks = append(targetDisk.recentTasks, task)
			}
		}
	}
}

// isDiskAvailable checks if a disk can accept new tasks
func (at *ActiveTopology) isDiskAvailable(disk *activeDisk, taskType TaskType) bool {
	// Check if disk has too many active tasks
	activeLoad := len(disk.pendingTasks) + len(disk.assignedTasks)
	if activeLoad >= 2 { // Max 2 concurrent tasks per disk
		return false
	}

	// Check for conflicting task types
	for _, task := range disk.assignedTasks {
		if at.areTaskTypesConflicting(task.TaskType, taskType) {
			return false
		}
	}

	return true
}

// areTaskTypesConflicting checks if two task types conflict
func (at *ActiveTopology) areTaskTypesConflicting(existing, new TaskType) bool {
	// Examples of conflicting task types
	conflictMap := map[TaskType][]TaskType{
		TaskTypeVacuum:        {TaskTypeBalance, TaskTypeErasureCoding},
		TaskTypeBalance:       {TaskTypeVacuum, TaskTypeErasureCoding},
		TaskTypeErasureCoding: {TaskTypeVacuum, TaskTypeBalance},
	}

	if conflicts, exists := conflictMap[existing]; exists {
		for _, conflictType := range conflicts {
			if conflictType == new {
				return true
			}
		}
	}

	return false
}

// cleanupRecentTasks removes old recent tasks
func (at *ActiveTopology) cleanupRecentTasks() {
	cutoff := time.Now().Add(-time.Duration(at.recentTaskWindowSeconds) * time.Second)

	for taskID, task := range at.recentTasks {
		if task.CompletedAt.Before(cutoff) {
			delete(at.recentTasks, taskID)
		}
	}
}
