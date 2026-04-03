package tasks

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BaseTask provides common functionality for all tasks
type BaseTask struct {
	taskType          types.TaskType
	taskID            string
	progress          float64
	cancelled         bool
	mutex             sync.RWMutex
	startTime         time.Time
	estimatedDuration time.Duration
	logger            TaskLogger
	loggerConfig      TaskLoggerConfig
	progressCallback  func(float64, string) // Callback function for progress updates
	currentStage      string                // Current stage description
}

// UnsupportedTaskTypeError represents an error for unsupported task types
type UnsupportedTaskTypeError struct {
	TaskType types.TaskType
}

// BaseTaskFactory provides common functionality for task factories
type BaseTaskFactory struct {
	taskType     types.TaskType
	capabilities []string
	description  string
}

// NewBaseTaskFactory creates a new base task factory
func NewBaseTaskFactory(taskType types.TaskType, capabilities []string, description string) *BaseTaskFactory {
	return &BaseTaskFactory{
		taskType:     taskType,
		capabilities: capabilities,
		description:  description,
	}
}

// Capabilities returns the capabilities required for this task type
func (f *BaseTaskFactory) Capabilities() []string {
	return f.capabilities
}

// Description returns the description of this task type
func (f *BaseTaskFactory) Description() string {
	return f.description
}

// ValidationError represents a parameter validation error
type ValidationError struct {
	Field   string
	Message string
}

// getServerFromSources extracts the server address from unified sources
func getServerFromSources(sources []*worker_pb.TaskSource) string {
	if len(sources) > 0 {
		return sources[0].Node
	}
	return ""
}
