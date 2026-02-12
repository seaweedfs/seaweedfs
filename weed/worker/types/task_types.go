package types

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/grpc"
)

// TaskType represents the type of maintenance task
type TaskType string

const (
	TaskTypeVacuum           TaskType = "vacuum"
	TaskTypeErasureCoding    TaskType = "erasure_coding"
	TaskTypeBalance          TaskType = "balance"
	TaskTypeReplication      TaskType = "replication"
	TaskTypeTableMaintenance TaskType = "table_maintenance"
)

// TaskStatus represents the status of a maintenance task
type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusAssigned   TaskStatus = "assigned"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusCompleted  TaskStatus = "completed"
	TaskStatusFailed     TaskStatus = "failed"
	TaskStatusCancelled  TaskStatus = "cancelled"
)

// TaskPriority represents the priority of a maintenance task
type TaskPriority string

const (
	TaskPriorityLow      TaskPriority = "low"
	TaskPriorityMedium   TaskPriority = "medium"
	TaskPriorityNormal   TaskPriority = "normal"
	TaskPriorityHigh     TaskPriority = "high"
	TaskPriorityCritical TaskPriority = "critical"
)

// TaskInput represents a maintenance task data
type TaskInput struct {
	ID          string                `json:"id"`
	Type        TaskType              `json:"type"`
	Status      TaskStatus            `json:"status"`
	Priority    TaskPriority          `json:"priority"`
	VolumeID    uint32                `json:"volume_id,omitempty"`
	Server      string                `json:"server,omitempty"`
	Collection  string                `json:"collection,omitempty"`
	WorkerID    string                `json:"worker_id,omitempty"`
	Progress    float64               `json:"progress"`
	Error       string                `json:"error,omitempty"`
	TypedParams *worker_pb.TaskParams `json:"typed_params,omitempty"`
	CreatedAt   time.Time             `json:"created_at"`
	ScheduledAt time.Time             `json:"scheduled_at"`
	StartedAt   *time.Time            `json:"started_at,omitempty"`
	CompletedAt *time.Time            `json:"completed_at,omitempty"`
	RetryCount  int                   `json:"retry_count"`
	MaxRetries  int                   `json:"max_retries"`
}

// TaskParams represents parameters for task execution
type TaskParams struct {
	VolumeID       uint32                `json:"volume_id,omitempty"`
	Collection     string                `json:"collection,omitempty"`
	WorkingDir     string                `json:"working_dir,omitempty"`
	TypedParams    *worker_pb.TaskParams `json:"typed_params,omitempty"`
	GrpcDialOption grpc.DialOption       `json:"-"` // Not serializable, for runtime use only
}

// TaskDetectionResult represents the result of scanning for maintenance needs
type TaskDetectionResult struct {
	TaskID      string                `json:"task_id"` // ActiveTopology task ID for lifecycle management
	TaskType    TaskType              `json:"task_type"`
	VolumeID    uint32                `json:"volume_id,omitempty"`
	Server      string                `json:"server,omitempty"`
	Collection  string                `json:"collection,omitempty"`
	Priority    TaskPriority          `json:"priority"`
	Reason      string                `json:"reason"`
	TypedParams *worker_pb.TaskParams `json:"typed_params,omitempty"`
	ScheduleAt  time.Time             `json:"schedule_at"`
}

// ClusterReplicationTask represents a cluster replication task parameters
type ClusterReplicationTask struct {
	SourcePath      string            `json:"source_path"`
	TargetCluster   string            `json:"target_cluster"`
	TargetPath      string            `json:"target_path"`
	ReplicationMode string            `json:"replication_mode"` // "sync", "async", "backup"
	Priority        int               `json:"priority"`
	Checksum        string            `json:"checksum,omitempty"`
	FileSize        int64             `json:"file_size"`
	CreatedAt       time.Time         `json:"created_at"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}
