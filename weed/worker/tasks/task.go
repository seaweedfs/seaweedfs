package tasks

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

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
