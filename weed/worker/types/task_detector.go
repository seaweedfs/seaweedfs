package types

import (
	"time"
)

// TaskDetector defines the interface for task detection
type TaskDetector interface {
	// GetTaskType returns the task type this detector handles
	GetTaskType() TaskType

	// ScanForTasks scans for tasks that need to be executed
	ScanForTasks(volumeMetrics []*VolumeHealthMetrics, clusterInfo *ClusterInfo) ([]*TaskDetectionResult, error)

	// ScanInterval returns how often this detector should scan
	ScanInterval() time.Duration

	// IsEnabled returns whether this detector is enabled
	IsEnabled() bool
}

// PolicyConfigurableDetector defines the interface for detectors that can be configured from policy
type PolicyConfigurableDetector interface {
	TaskDetector

	// ConfigureFromPolicy configures the detector based on the maintenance policy
	ConfigureFromPolicy(policy interface{})
}
