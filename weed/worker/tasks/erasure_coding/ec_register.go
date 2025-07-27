package erasure_coding

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Auto-register this task when the package is imported
func init() {
	RegisterErasureCodingTask()
}

// RegisterErasureCodingTask registers the erasure coding task with the new architecture
func RegisterErasureCodingTask() {
	// Create configuration instance
	config := NewDefaultConfig()

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeErasureCoding,
		Name:         "erasure_coding",
		DisplayName:  "Erasure Coding",
		Description:  "Applies erasure coding to volumes for data protection",
		Icon:         "fas fa-shield-alt text-success",
		Capabilities: []string{"erasure_coding", "data_protection"},

		Config:         config,
		ConfigSpec:     GetConfigSpec(),
		CreateTask:     CreateTask,
		DetectionFunc:  Detection,
		ScanInterval:   1 * time.Hour,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  1,
		RepeatInterval: 24 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
