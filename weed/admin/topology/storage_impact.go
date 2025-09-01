package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// CalculateTaskStorageImpact calculates storage impact for different task types
func CalculateTaskStorageImpact(taskType TaskType, volumeSize int64) (sourceChange, targetChange StorageSlotChange) {
	switch string(taskType) {
	case "erasure_coding":
		// EC task: distributes shards to MULTIPLE targets, source reserves with zero impact
		// Source reserves capacity but with zero StorageSlotChange (no actual capacity consumption during planning)
		// WARNING: EC has multiple targets! Use AddPendingTask with multiple destinations for proper multi-destination calculation
		// This simplified function returns zero impact; real EC requires specialized multi-destination calculation
		return StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}

	case "replication":
		// Replication task: creates new replica on target
		return StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, StorageSlotChange{VolumeSlots: 1, ShardSlots: 0}

	default:
		// Unknown task type, assume minimal impact
		glog.V(2).Infof("Task type %s not specifically handled in CalculateTaskStorageImpact, using default impact", taskType)
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
