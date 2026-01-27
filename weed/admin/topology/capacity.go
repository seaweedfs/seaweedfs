package topology

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// GetEffectiveAvailableCapacity returns the effective available capacity for a disk
// This considers BOTH pending and assigned tasks for capacity reservation.
//
// Formula: BaseAvailable - (VolumeSlots + ShardSlots/ShardsPerVolumeSlot) from all tasks
//
// The calculation includes:
// - Pending tasks: Reserve capacity immediately when added
// - Assigned tasks: Continue to reserve capacity during execution
// - Recently completed tasks are NOT counted against capacity
func (at *ActiveTopology) GetEffectiveAvailableCapacity(nodeID string, diskID uint32) int64 {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	diskKey := fmt.Sprintf("%s:%d", nodeID, diskID)
	disk, exists := at.disks[diskKey]
	if !exists {
		return 0
	}

	if disk.DiskInfo == nil || disk.DiskInfo.DiskInfo == nil {
		return 0
	}

	// Use the same logic as getEffectiveAvailableCapacityUnsafe but with locking
	capacity := at.getEffectiveAvailableCapacityUnsafe(disk)
	return int64(capacity.VolumeSlots)
}

// GetEffectiveAvailableCapacityDetailed returns detailed available capacity as StorageSlotChange
// This provides granular information about available volume slots and shard slots
func (at *ActiveTopology) GetEffectiveAvailableCapacityDetailed(nodeID string, diskID uint32) StorageSlotChange {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	diskKey := fmt.Sprintf("%s:%d", nodeID, diskID)
	disk, exists := at.disks[diskKey]
	if !exists {
		return StorageSlotChange{}
	}

	if disk.DiskInfo == nil || disk.DiskInfo.DiskInfo == nil {
		return StorageSlotChange{}
	}

	return at.getEffectiveAvailableCapacityUnsafe(disk)
}

// GetEffectiveCapacityImpact returns the StorageSlotChange impact for a disk
// This shows the net impact from all pending and assigned tasks
func (at *ActiveTopology) GetEffectiveCapacityImpact(nodeID string, diskID uint32) StorageSlotChange {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	diskKey := fmt.Sprintf("%s:%d", nodeID, diskID)
	disk, exists := at.disks[diskKey]
	if !exists {
		return StorageSlotChange{}
	}

	return at.getEffectiveCapacityUnsafe(disk)
}

// GetDisksWithEffectiveCapacity returns disks with sufficient effective capacity
// This method considers BOTH pending and assigned tasks for capacity reservation using StorageSlotChange.
//
// Parameters:
//   - taskType: type of task to check compatibility for
//   - excludeNodeID: node to exclude from results
//   - minCapacity: minimum effective capacity required (in volume slots)
//
// Returns: DiskInfo objects where VolumeCount reflects capacity reserved by all tasks
func (at *ActiveTopology) GetDisksWithEffectiveCapacity(taskType TaskType, excludeNodeID string, minCapacity int64) []*DiskInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	var available []*DiskInfo

	glog.V(2).Infof("GetDisksWithEffectiveCapacity checking %d disks for type %s, minCapacity %d", len(at.disks), taskType, minCapacity)
	for _, disk := range at.disks {
		if disk.NodeID == excludeNodeID {
			continue // Skip excluded node
		}

		if at.isDiskAvailable(disk, taskType) {
			effectiveCapacity := at.getEffectiveAvailableCapacityUnsafe(disk)

			// Only include disks that meet minimum capacity requirement
			if int64(effectiveCapacity.VolumeSlots) >= minCapacity {
				// Create a new DiskInfo with current capacity information
				diskCopy := DiskInfo{
					NodeID:     disk.DiskInfo.NodeID,
					DiskID:     disk.DiskInfo.DiskID,
					DiskType:   disk.DiskInfo.DiskType,
					DataCenter: disk.DiskInfo.DataCenter,
					Rack:       disk.DiskInfo.Rack,
					LoadCount:  len(disk.pendingTasks) + len(disk.assignedTasks), // Count all tasks
				}

				// Create a new protobuf DiskInfo to avoid modifying the original
				diskInfoCopy := &master_pb.DiskInfo{
					DiskId:            disk.DiskInfo.DiskInfo.DiskId,
					MaxVolumeCount:    disk.DiskInfo.DiskInfo.MaxVolumeCount,
					VolumeCount:       disk.DiskInfo.DiskInfo.MaxVolumeCount - int64(effectiveCapacity.VolumeSlots),
					VolumeInfos:       disk.DiskInfo.DiskInfo.VolumeInfos,
					EcShardInfos:      disk.DiskInfo.DiskInfo.EcShardInfos,
					RemoteVolumeCount: disk.DiskInfo.DiskInfo.RemoteVolumeCount,
					ActiveVolumeCount: disk.DiskInfo.DiskInfo.ActiveVolumeCount,
					FreeVolumeCount:   disk.DiskInfo.DiskInfo.FreeVolumeCount,
				}
				diskCopy.DiskInfo = diskInfoCopy
				diskCopy.DiskInfo.MaxVolumeCount = disk.DiskInfo.DiskInfo.MaxVolumeCount // Ensure Max is set

				available = append(available, &diskCopy)
			} else {
				glog.V(2).Infof("Disk %s:%d capacity %d < %d (Max:%d, Vol:%d)", disk.NodeID, disk.DiskInfo.DiskID, effectiveCapacity.VolumeSlots, minCapacity, disk.DiskInfo.DiskInfo.MaxVolumeCount, disk.DiskInfo.DiskInfo.VolumeCount)
			}
		} else {
			tasksInfo := ""
			for _, t := range disk.pendingTasks {
				tasksInfo += fmt.Sprintf("[P:%s,Vol:%d] ", t.TaskType, t.VolumeID)
			}
			for _, t := range disk.assignedTasks {
				tasksInfo += fmt.Sprintf("[A:%s,Vol:%d] ", t.TaskType, t.VolumeID)
			}
			glog.V(2).Infof("Disk %s:%d unavailable. Load: %d, MaxLoad: %d. Tasks: %s", disk.NodeID, disk.DiskInfo.DiskID, len(disk.pendingTasks)+len(disk.assignedTasks), MaxConcurrentTasksPerDisk, tasksInfo)
		}
	}
	glog.V(2).Infof("GetDisksWithEffectiveCapacity found %d available disks", len(available))

	return available
}

// GetDisksForPlanning returns disks considering both active and pending tasks for planning decisions
// This helps avoid over-scheduling tasks to the same disk
func (at *ActiveTopology) GetDisksForPlanning(taskType TaskType, excludeNodeID string, minCapacity int64) []*DiskInfo {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	var available []*DiskInfo

	for _, disk := range at.disks {
		if disk.NodeID == excludeNodeID {
			continue // Skip excluded node
		}

		// Consider both pending and active tasks for scheduling decisions
		if at.isDiskAvailableForPlanning(disk, taskType) {
			// Check if disk can accommodate new task considering pending tasks
			planningCapacity := at.getPlanningCapacityUnsafe(disk)

			if int64(planningCapacity.VolumeSlots) >= minCapacity {
				// Create a new DiskInfo with planning information
				diskCopy := DiskInfo{
					NodeID:     disk.DiskInfo.NodeID,
					DiskID:     disk.DiskInfo.DiskID,
					DiskType:   disk.DiskInfo.DiskType,
					DataCenter: disk.DiskInfo.DataCenter,
					Rack:       disk.DiskInfo.Rack,
					LoadCount:  len(disk.pendingTasks) + len(disk.assignedTasks),
				}

				// Create a new protobuf DiskInfo to avoid modifying the original
				diskInfoCopy := &master_pb.DiskInfo{
					DiskId:            disk.DiskInfo.DiskInfo.DiskId,
					MaxVolumeCount:    disk.DiskInfo.DiskInfo.MaxVolumeCount,
					VolumeCount:       disk.DiskInfo.DiskInfo.MaxVolumeCount - int64(planningCapacity.VolumeSlots),
					VolumeInfos:       disk.DiskInfo.DiskInfo.VolumeInfos,
					EcShardInfos:      disk.DiskInfo.DiskInfo.EcShardInfos,
					RemoteVolumeCount: disk.DiskInfo.DiskInfo.RemoteVolumeCount,
					ActiveVolumeCount: disk.DiskInfo.DiskInfo.ActiveVolumeCount,
					FreeVolumeCount:   disk.DiskInfo.DiskInfo.FreeVolumeCount,
				}
				diskCopy.DiskInfo = diskInfoCopy

				available = append(available, &diskCopy)
			}
		}
	}

	return available
}

// CanAccommodateTask checks if a disk can accommodate a new task considering all constraints
func (at *ActiveTopology) CanAccommodateTask(nodeID string, diskID uint32, taskType TaskType, volumesNeeded int64) bool {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	diskKey := fmt.Sprintf("%s:%d", nodeID, diskID)
	disk, exists := at.disks[diskKey]
	if !exists {
		return false
	}

	// Check basic availability
	if !at.isDiskAvailable(disk, taskType) {
		return false
	}

	// Check effective capacity
	effectiveCapacity := at.getEffectiveAvailableCapacityUnsafe(disk)
	return int64(effectiveCapacity.VolumeSlots) >= volumesNeeded
}

// getPlanningCapacityUnsafe considers both pending and active tasks for planning
func (at *ActiveTopology) getPlanningCapacityUnsafe(disk *activeDisk) StorageSlotChange {
	if disk.DiskInfo == nil || disk.DiskInfo.DiskInfo == nil {
		return StorageSlotChange{}
	}

	baseAvailableVolumes := disk.DiskInfo.DiskInfo.MaxVolumeCount - disk.DiskInfo.DiskInfo.VolumeCount

	// Use the centralized helper function to calculate task storage impact
	totalImpact := at.calculateTaskStorageImpact(disk)

	// Calculate available capacity considering impact (negative impact reduces availability)
	availableVolumeSlots := baseAvailableVolumes - totalImpact.ToVolumeSlots()
	if availableVolumeSlots < 0 {
		availableVolumeSlots = 0
	}

	// Return detailed capacity information
	return StorageSlotChange{
		VolumeSlots: int32(availableVolumeSlots),
		ShardSlots:  -totalImpact.ShardSlots, // Available shard capacity (negative impact becomes positive availability)
	}
}

// isDiskAvailableForPlanning checks if disk can accept new tasks considering pending load
func (at *ActiveTopology) isDiskAvailableForPlanning(disk *activeDisk, taskType TaskType) bool {
	// Check total load including pending tasks
	totalLoad := len(disk.pendingTasks) + len(disk.assignedTasks)
	if totalLoad >= MaxTotalTaskLoadPerDisk {
		return false
	}

	// Check for conflicting task types in active tasks only
	for _, task := range disk.assignedTasks {
		if at.areTaskTypesConflicting(task.TaskType, taskType) {
			return false
		}
	}

	return true
}

// calculateTaskStorageImpact is a helper function that calculates the total storage impact
// from all tasks (pending and assigned) on a given disk. This eliminates code duplication
// between multiple capacity calculation functions.
func (at *ActiveTopology) calculateTaskStorageImpact(disk *activeDisk) StorageSlotChange {
	if disk.DiskInfo == nil || disk.DiskInfo.DiskInfo == nil {
		return StorageSlotChange{}
	}

	totalImpact := StorageSlotChange{}

	// Process both pending and assigned tasks with identical logic
	taskLists := [][]*taskState{disk.pendingTasks, disk.assignedTasks}

	for _, taskList := range taskLists {
		for _, task := range taskList {
			// Calculate impact for all source locations
			for _, source := range task.Sources {
				if source.SourceServer == disk.NodeID && source.SourceDisk == disk.DiskID {
					totalImpact.AddInPlace(source.StorageChange)
				}
			}

			// Calculate impact for all destination locations
			for _, dest := range task.Destinations {
				if dest.TargetServer == disk.NodeID && dest.TargetDisk == disk.DiskID {
					totalImpact.AddInPlace(dest.StorageChange)
				}
			}
		}
	}

	return totalImpact
}

// getEffectiveCapacityUnsafe returns effective capacity impact without locking (for internal use)
// Returns StorageSlotChange representing the net impact from all tasks
func (at *ActiveTopology) getEffectiveCapacityUnsafe(disk *activeDisk) StorageSlotChange {
	return at.calculateTaskStorageImpact(disk)
}

// getEffectiveAvailableCapacityUnsafe returns detailed available capacity as StorageSlotChange
func (at *ActiveTopology) getEffectiveAvailableCapacityUnsafe(disk *activeDisk) StorageSlotChange {
	if disk.DiskInfo == nil || disk.DiskInfo.DiskInfo == nil {
		return StorageSlotChange{}
	}

	baseAvailable := disk.DiskInfo.DiskInfo.MaxVolumeCount - disk.DiskInfo.DiskInfo.VolumeCount
	netImpact := at.getEffectiveCapacityUnsafe(disk)

	// Calculate available volume slots (negative impact reduces availability)
	availableVolumeSlots := baseAvailable - netImpact.ToVolumeSlots()
	if availableVolumeSlots < 0 {
		availableVolumeSlots = 0
	}

	// Return detailed capacity information
	return StorageSlotChange{
		VolumeSlots: int32(availableVolumeSlots),
		ShardSlots:  -netImpact.ShardSlots, // Available shard capacity (negative impact becomes positive availability)
	}
}
