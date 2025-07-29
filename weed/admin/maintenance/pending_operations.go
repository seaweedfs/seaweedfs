package maintenance

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// PendingOperationType represents the type of pending operation
type PendingOperationType string

const (
	OpTypeVolumeMove    PendingOperationType = "volume_move"
	OpTypeVolumeBalance PendingOperationType = "volume_balance"
	OpTypeErasureCoding PendingOperationType = "erasure_coding"
	OpTypeVacuum        PendingOperationType = "vacuum"
	OpTypeReplication   PendingOperationType = "replication"
)

// PendingOperation represents a pending volume/shard operation
type PendingOperation struct {
	VolumeID      uint32               `json:"volume_id"`
	OperationType PendingOperationType `json:"operation_type"`
	SourceNode    string               `json:"source_node"`
	DestNode      string               `json:"dest_node,omitempty"` // Empty for non-movement operations
	TaskID        string               `json:"task_id"`
	StartTime     time.Time            `json:"start_time"`
	EstimatedSize uint64               `json:"estimated_size"` // Bytes
	Collection    string               `json:"collection"`
	Status        string               `json:"status"` // "assigned", "in_progress", "completing"
}

// PendingOperations tracks all pending volume/shard operations
type PendingOperations struct {
	// Operations by volume ID for conflict detection
	byVolumeID map[uint32]*PendingOperation

	// Operations by task ID for updates
	byTaskID map[string]*PendingOperation

	// Operations by node for capacity calculations
	bySourceNode map[string][]*PendingOperation
	byDestNode   map[string][]*PendingOperation

	mutex sync.RWMutex
}

// NewPendingOperations creates a new pending operations tracker
func NewPendingOperations() *PendingOperations {
	return &PendingOperations{
		byVolumeID:   make(map[uint32]*PendingOperation),
		byTaskID:     make(map[string]*PendingOperation),
		bySourceNode: make(map[string][]*PendingOperation),
		byDestNode:   make(map[string][]*PendingOperation),
	}
}

// AddOperation adds a pending operation
func (po *PendingOperations) AddOperation(op *PendingOperation) {
	po.mutex.Lock()
	defer po.mutex.Unlock()

	// Check for existing operation on this volume
	if existing, exists := po.byVolumeID[op.VolumeID]; exists {
		glog.V(1).Infof("Replacing existing pending operation on volume %d: %s -> %s",
			op.VolumeID, existing.TaskID, op.TaskID)
		po.removeOperationUnlocked(existing)
	}

	// Add new operation
	po.byVolumeID[op.VolumeID] = op
	po.byTaskID[op.TaskID] = op

	// Add to node indexes
	po.bySourceNode[op.SourceNode] = append(po.bySourceNode[op.SourceNode], op)
	if op.DestNode != "" {
		po.byDestNode[op.DestNode] = append(po.byDestNode[op.DestNode], op)
	}

	glog.V(2).Infof("Added pending operation: volume %d, type %s, task %s, %s -> %s",
		op.VolumeID, op.OperationType, op.TaskID, op.SourceNode, op.DestNode)
}

// RemoveOperation removes a completed operation
func (po *PendingOperations) RemoveOperation(taskID string) {
	po.mutex.Lock()
	defer po.mutex.Unlock()

	if op, exists := po.byTaskID[taskID]; exists {
		po.removeOperationUnlocked(op)
		glog.V(2).Infof("Removed completed operation: volume %d, task %s", op.VolumeID, taskID)
	}
}

// removeOperationUnlocked removes an operation (must hold lock)
func (po *PendingOperations) removeOperationUnlocked(op *PendingOperation) {
	delete(po.byVolumeID, op.VolumeID)
	delete(po.byTaskID, op.TaskID)

	// Remove from source node list
	if ops, exists := po.bySourceNode[op.SourceNode]; exists {
		for i, other := range ops {
			if other.TaskID == op.TaskID {
				po.bySourceNode[op.SourceNode] = append(ops[:i], ops[i+1:]...)
				break
			}
		}
	}

	// Remove from dest node list
	if op.DestNode != "" {
		if ops, exists := po.byDestNode[op.DestNode]; exists {
			for i, other := range ops {
				if other.TaskID == op.TaskID {
					po.byDestNode[op.DestNode] = append(ops[:i], ops[i+1:]...)
					break
				}
			}
		}
	}
}

// HasPendingOperationOnVolume checks if a volume has a pending operation
func (po *PendingOperations) HasPendingOperationOnVolume(volumeID uint32) bool {
	po.mutex.RLock()
	defer po.mutex.RUnlock()

	_, exists := po.byVolumeID[volumeID]
	return exists
}

// GetPendingOperationOnVolume returns the pending operation on a volume
func (po *PendingOperations) GetPendingOperationOnVolume(volumeID uint32) *PendingOperation {
	po.mutex.RLock()
	defer po.mutex.RUnlock()

	return po.byVolumeID[volumeID]
}

// WouldConflictWithPending checks if a new operation would conflict with pending ones
func (po *PendingOperations) WouldConflictWithPending(volumeID uint32, opType PendingOperationType) bool {
	po.mutex.RLock()
	defer po.mutex.RUnlock()

	if existing, exists := po.byVolumeID[volumeID]; exists {
		// Volume already has a pending operation
		glog.V(3).Infof("Volume %d conflict: already has %s operation (task %s)",
			volumeID, existing.OperationType, existing.TaskID)
		return true
	}

	return false
}

// GetPendingCapacityImpactForNode calculates pending capacity changes for a node
func (po *PendingOperations) GetPendingCapacityImpactForNode(nodeID string) (incoming uint64, outgoing uint64) {
	po.mutex.RLock()
	defer po.mutex.RUnlock()

	// Calculate outgoing capacity (volumes leaving this node)
	if ops, exists := po.bySourceNode[nodeID]; exists {
		for _, op := range ops {
			// Only count movement operations
			if op.DestNode != "" {
				outgoing += op.EstimatedSize
			}
		}
	}

	// Calculate incoming capacity (volumes coming to this node)
	if ops, exists := po.byDestNode[nodeID]; exists {
		for _, op := range ops {
			incoming += op.EstimatedSize
		}
	}

	return incoming, outgoing
}

// FilterVolumeMetricsExcludingPending filters out volumes with pending operations
func (po *PendingOperations) FilterVolumeMetricsExcludingPending(metrics []*types.VolumeHealthMetrics) []*types.VolumeHealthMetrics {
	po.mutex.RLock()
	defer po.mutex.RUnlock()

	var filtered []*types.VolumeHealthMetrics
	excludedCount := 0

	for _, metric := range metrics {
		if _, hasPending := po.byVolumeID[metric.VolumeID]; !hasPending {
			filtered = append(filtered, metric)
		} else {
			excludedCount++
			glog.V(3).Infof("Excluding volume %d from scan due to pending operation", metric.VolumeID)
		}
	}

	if excludedCount > 0 {
		glog.V(1).Infof("Filtered out %d volumes with pending operations from %d total volumes",
			excludedCount, len(metrics))
	}

	return filtered
}

// GetNodeCapacityProjection calculates projected capacity for a node
func (po *PendingOperations) GetNodeCapacityProjection(nodeID string, currentUsed uint64, totalCapacity uint64) NodeCapacityProjection {
	incoming, outgoing := po.GetPendingCapacityImpactForNode(nodeID)

	projectedUsed := currentUsed + incoming - outgoing
	projectedFree := totalCapacity - projectedUsed

	return NodeCapacityProjection{
		NodeID:          nodeID,
		CurrentUsed:     currentUsed,
		TotalCapacity:   totalCapacity,
		PendingIncoming: incoming,
		PendingOutgoing: outgoing,
		ProjectedUsed:   projectedUsed,
		ProjectedFree:   projectedFree,
	}
}

// GetAllPendingOperations returns all pending operations
func (po *PendingOperations) GetAllPendingOperations() []*PendingOperation {
	po.mutex.RLock()
	defer po.mutex.RUnlock()

	var operations []*PendingOperation
	for _, op := range po.byVolumeID {
		operations = append(operations, op)
	}

	return operations
}

// UpdateOperationStatus updates the status of a pending operation
func (po *PendingOperations) UpdateOperationStatus(taskID string, status string) {
	po.mutex.Lock()
	defer po.mutex.Unlock()

	if op, exists := po.byTaskID[taskID]; exists {
		op.Status = status
		glog.V(3).Infof("Updated operation status: task %s, volume %d -> %s", taskID, op.VolumeID, status)
	}
}

// CleanupStaleOperations removes operations that have been running too long
func (po *PendingOperations) CleanupStaleOperations(maxAge time.Duration) int {
	po.mutex.Lock()
	defer po.mutex.Unlock()

	cutoff := time.Now().Add(-maxAge)
	var staleOps []*PendingOperation

	for _, op := range po.byVolumeID {
		if op.StartTime.Before(cutoff) {
			staleOps = append(staleOps, op)
		}
	}

	for _, op := range staleOps {
		po.removeOperationUnlocked(op)
		glog.Warningf("Removed stale pending operation: volume %d, task %s, age %v",
			op.VolumeID, op.TaskID, time.Since(op.StartTime))
	}

	return len(staleOps)
}

// NodeCapacityProjection represents projected capacity for a node
type NodeCapacityProjection struct {
	NodeID          string `json:"node_id"`
	CurrentUsed     uint64 `json:"current_used"`
	TotalCapacity   uint64 `json:"total_capacity"`
	PendingIncoming uint64 `json:"pending_incoming"`
	PendingOutgoing uint64 `json:"pending_outgoing"`
	ProjectedUsed   uint64 `json:"projected_used"`
	ProjectedFree   uint64 `json:"projected_free"`
}

// GetStats returns statistics about pending operations
func (po *PendingOperations) GetStats() PendingOperationsStats {
	po.mutex.RLock()
	defer po.mutex.RUnlock()

	stats := PendingOperationsStats{
		TotalOperations: len(po.byVolumeID),
		ByType:          make(map[PendingOperationType]int),
		ByStatus:        make(map[string]int),
	}

	var totalSize uint64
	for _, op := range po.byVolumeID {
		stats.ByType[op.OperationType]++
		stats.ByStatus[op.Status]++
		totalSize += op.EstimatedSize
	}

	stats.TotalEstimatedSize = totalSize
	return stats
}

// PendingOperationsStats provides statistics about pending operations
type PendingOperationsStats struct {
	TotalOperations    int                          `json:"total_operations"`
	ByType             map[PendingOperationType]int `json:"by_type"`
	ByStatus           map[string]int               `json:"by_status"`
	TotalEstimatedSize uint64                       `json:"total_estimated_size"`
}
