package topology

import "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"

// TaskType represents different types of maintenance operations
type TaskType string

// TaskStatus represents the current status of a task
type TaskStatus string

// Common task type constants
const (
	TaskTypeNone          TaskType = ""
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

// Task and capacity management configuration constants
const (
	// MaxConcurrentTasksPerDisk defines the maximum number of concurrent tasks per disk
	// This prevents overloading a single disk with too many simultaneous operations
	MaxConcurrentTasksPerDisk = 10

	// MaxTotalTaskLoadPerDisk defines the maximum total task load (pending + active) per disk
	// This allows more tasks to be queued but limits the total pipeline depth
	MaxTotalTaskLoadPerDisk = 20

	// MaxTaskLoadForECPlacement defines the maximum task load to consider a disk for EC placement
	// This threshold ensures disks aren't overloaded when planning EC operations
	MaxTaskLoadForECPlacement = 10
)

// StorageSlotChange represents storage impact at both volume and shard levels
type StorageSlotChange struct {
	VolumeSlots int32 `json:"volume_slots"` // Volume-level slot changes (full volumes)
	ShardSlots  int32 `json:"shard_slots"`  // Shard-level slot changes (EC shards, fractional capacity)
}

// Add returns a new StorageSlotChange with the sum of this and other
func (s StorageSlotChange) Add(other StorageSlotChange) StorageSlotChange {
	return StorageSlotChange{
		VolumeSlots: s.VolumeSlots + other.VolumeSlots,
		ShardSlots:  s.ShardSlots + other.ShardSlots,
	}
}

// Subtract returns a new StorageSlotChange with other subtracted from this
func (s StorageSlotChange) Subtract(other StorageSlotChange) StorageSlotChange {
	return StorageSlotChange{
		VolumeSlots: s.VolumeSlots - other.VolumeSlots,
		ShardSlots:  s.ShardSlots - other.ShardSlots,
	}
}

// AddInPlace adds other to this StorageSlotChange in-place
func (s *StorageSlotChange) AddInPlace(other StorageSlotChange) {
	s.VolumeSlots += other.VolumeSlots
	s.ShardSlots += other.ShardSlots
}

// SubtractInPlace subtracts other from this StorageSlotChange in-place
func (s *StorageSlotChange) SubtractInPlace(other StorageSlotChange) {
	s.VolumeSlots -= other.VolumeSlots
	s.ShardSlots -= other.ShardSlots
}

// IsZero returns true if both VolumeSlots and ShardSlots are zero
func (s StorageSlotChange) IsZero() bool {
	return s.VolumeSlots == 0 && s.ShardSlots == 0
}

// ShardsPerVolumeSlot defines how many EC shards are equivalent to one volume slot
const ShardsPerVolumeSlot = erasure_coding.DataShardsCount

// ToVolumeSlots converts the entire StorageSlotChange to equivalent volume slots
func (s StorageSlotChange) ToVolumeSlots() int64 {
	return int64(s.VolumeSlots) + int64(s.ShardSlots)/ShardsPerVolumeSlot
}

// ToShardSlots converts the entire StorageSlotChange to equivalent shard slots
func (s StorageSlotChange) ToShardSlots() int32 {
	return s.ShardSlots + s.VolumeSlots*ShardsPerVolumeSlot
}

// CanAccommodate checks if this StorageSlotChange can accommodate the required StorageSlotChange
// Both are converted to shard slots for a more precise comparison
func (s StorageSlotChange) CanAccommodate(required StorageSlotChange) bool {
	return s.ToShardSlots() >= required.ToShardSlots()
}
