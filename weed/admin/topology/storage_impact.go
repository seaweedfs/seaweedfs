package topology

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// CalculateTaskStorageImpact calculates storage impact for different task types
func CalculateTaskStorageImpact(taskType TaskType, volumeSize int64) (sourceChange, targetChange StorageSlotChange) {
	switch taskType {
	case TaskTypeErasureCoding:
		// EC task: distributes shards to MULTIPLE targets, source reserves with zero impact
		// Source reserves capacity but with zero StorageSlotChange (no actual capacity consumption during planning)
		// WARNING: EC has multiple targets! Use AddPendingECShardTask for proper multi-target handling
		// This function only returns source impact; target impact is meaningless for EC
		return StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, StorageSlotChange{VolumeSlots: 0, ShardSlots: 1}

	case TaskTypeBalance:
		// Balance task: moves volume from source to target
		// Source loses 1 volume, target gains 1 volume
		return StorageSlotChange{VolumeSlots: -1, ShardSlots: 0}, StorageSlotChange{VolumeSlots: 1, ShardSlots: 0}

	case TaskTypeVacuum:
		// Vacuum task: frees space by removing deleted entries, no slot change
		return StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}

	case TaskTypeReplication:
		// Replication task: creates new replica on target
		return StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, StorageSlotChange{VolumeSlots: 1, ShardSlots: 0}

	default:
		// Unknown task type, assume minimal impact
		glog.Warningf("unhandled task type %s in CalculateTaskStorageImpact, assuming default impact", taskType)
		return StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, StorageSlotChange{VolumeSlots: 1, ShardSlots: 0}
	}
}

// CalculateECShardStorageImpact calculates storage impact for EC shards specifically
func CalculateECShardStorageImpact(shardCount int32, expectedShardSize int64) StorageSlotChange {
	// EC shards are typically much smaller than full volumes
	// Use shard-level tracking for granular capacity planning
	return StorageSlotChange{VolumeSlots: 0, ShardSlots: shardCount}
}

// CalculateECShardCleanupImpact calculates storage impact for cleaning up existing EC shards
func CalculateECShardCleanupImpact(originalVolumeSize int64) StorageSlotChange {
	// Cleaning up existing EC shards frees shard slots
	// Use the actual EC configuration constants for accurate shard count
	return StorageSlotChange{VolumeSlots: 0, ShardSlots: -int32(erasure_coding.TotalShardsCount)} // Negative = freed capacity
}

// GetDiskStorageImpact returns comprehensive storage impact information for a specific disk
// Returns separate counts for planned (pending) vs reserved (active) capacity
func (at *ActiveTopology) GetDiskStorageImpact(nodeID string, diskID uint32) (plannedVolumeSlots, reservedVolumeSlots int64, plannedShardSlots, reservedShardSlots int32, estimatedSize int64) {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	diskKey := fmt.Sprintf("%s:%d", nodeID, diskID)
	disk, exists := at.disks[diskKey]
	if !exists {
		return 0, 0, 0, 0, 0
	}

	plannedVolumeSlots = 0
	reservedVolumeSlots = 0
	plannedShardSlots = 0
	reservedShardSlots = 0
	estimatedSize = 0

	// Calculate PLANNED impact from pending tasks (not yet consuming capacity)
	for _, task := range disk.pendingTasks {
		// Handle all source locations
		for _, source := range task.Sources {
			if source.SourceServer == nodeID && source.SourceDisk == diskID {
				plannedVolumeSlots += int64(source.StorageChange.VolumeSlots)
				plannedShardSlots += source.StorageChange.ShardSlots
				estimatedSize += source.EstimatedSize
			}
		}

		// Handle all destination locations
		for _, dest := range task.Destinations {
			if dest.TargetServer == nodeID && dest.TargetDisk == diskID {
				plannedVolumeSlots += int64(dest.StorageChange.VolumeSlots)
				plannedShardSlots += dest.StorageChange.ShardSlots
			}
		}
	}

	// Calculate RESERVED impact from active tasks (currently consuming capacity)
	for _, task := range disk.assignedTasks {
		// Handle all source locations
		for _, source := range task.Sources {
			if source.SourceServer == nodeID && source.SourceDisk == diskID {
				reservedVolumeSlots += int64(source.StorageChange.VolumeSlots)
				reservedShardSlots += source.StorageChange.ShardSlots
				estimatedSize += source.EstimatedSize
			}
		}

		// Handle all destination locations
		for _, dest := range task.Destinations {
			if dest.TargetServer == nodeID && dest.TargetDisk == diskID {
				reservedVolumeSlots += int64(dest.StorageChange.VolumeSlots)
				reservedShardSlots += dest.StorageChange.ShardSlots
			}
		}
	}

	return plannedVolumeSlots, reservedVolumeSlots, plannedShardSlots, reservedShardSlots, estimatedSize
}
