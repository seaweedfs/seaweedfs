package types

import (
	"time"
)

// TaskDetector defines how a task detects when it should run
type TaskDetector interface {
	// GetTaskType returns the task type this detector handles
	GetTaskType() TaskType

	// ScanForTasks scans for tasks that need to be created
	ScanForTasks(volumeMetrics []*VolumeHealthMetrics, clusterInfo *ClusterInfo) ([]*TaskDetectionResult, error)

	// ScanInterval returns how often this task type should be scanned
	ScanInterval() time.Duration

	// IsEnabled returns whether this task type is enabled
	IsEnabled() bool
}
