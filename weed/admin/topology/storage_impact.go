package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// CalculateTaskStorageImpact calculates storage impact for different task types
func CalculateTaskStorageImpact(taskType TaskType, volumeSize int64) (sourceChange, targetChange StorageSlotChange) {
	switch taskType {
	case TaskTypeErasureCoding:
		// EC task: distributes shards to MULTIPLE targets, source reserves with zero impact
		// Source reserves capacity but with zero StorageSlotChange (no actual capacity consumption during planning)
		// WARNING: EC has multiple targets! Use AddPendingTask with multiple destinations for proper multi-target handling
		// This simplified function returns zero impact; real EC requires specialized multi-destination calculation
		return StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}

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
