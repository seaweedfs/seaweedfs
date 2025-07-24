package task

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// InProgressTask represents a task currently being executed
type InProgressTask struct {
	Task           *types.Task
	WorkerID       string
	StartedAt      time.Time
	LastUpdate     time.Time
	Progress       float64
	EstimatedEnd   time.Time
	VolumeReserved bool // Reserved for capacity planning
}

// VolumeCandidate represents a volume that needs maintenance
type VolumeCandidate struct {
	VolumeID   uint32
	Server     string
	Collection string
	TaskType   types.TaskType
	Priority   types.TaskPriority
	Reason     string
	DetectedAt time.Time
	ScheduleAt time.Time
	Parameters map[string]interface{}
}

// VolumeChange represents a volume state change
type VolumeChange struct {
	VolumeID         uint32
	ChangeType       ChangeType
	OldCapacity      int64
	NewCapacity      int64
	TaskID           string
	CompletedAt      time.Time
	ReportedToMaster bool
}

// ChangeType represents the type of volume change
type ChangeType string

const (
	ChangeTypeECEncoding     ChangeType = "ec_encoding"
	ChangeTypeVacuumComplete ChangeType = "vacuum_completed"
)

// WorkerMetrics represents performance metrics for a worker
type WorkerMetrics struct {
	TasksCompleted  int
	TasksFailed     int
	AverageTaskTime time.Duration
	LastTaskTime    time.Time
	SuccessRate     float64
}

// VolumeReservation represents a reserved volume capacity
type VolumeReservation struct {
	VolumeID      uint32
	TaskID        string
	ReservedAt    time.Time
	ExpectedEnd   time.Time
	CapacityDelta int64 // Expected change in capacity
}
