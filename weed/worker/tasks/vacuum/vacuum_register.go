package vacuum

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Auto-register this task when the package is imported
func init() {
	RegisterVacuumTask()
}

// RegisterVacuumTask registers the vacuum task with the new architecture
func RegisterVacuumTask() {
	// Create configuration instance
	config := NewDefaultConfig()

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeVacuum,
		Name:         "vacuum",
		DisplayName:  "Volume Vacuum",
		Description:  "Reclaims disk space by removing deleted files from volumes",
		Icon:         "fas fa-broom text-primary",
		Capabilities: []string{"vacuum", "storage"},

		Config:         config,
		ConfigSpec:     GetConfigSpec(),
		CreateTask:     CreateTask,
		DetectionFunc:  Detection,
		ScanInterval:   2 * time.Hour,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  2,
		RepeatInterval: 7 * 24 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
