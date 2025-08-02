package topology

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

// TotalImpact returns the total capacity impact as int64 (VolumeSlots + ShardSlots/10)
func (s StorageSlotChange) TotalImpact() int64 {
	return int64(s.VolumeSlots) + int64(s.ShardSlots)/10
}
