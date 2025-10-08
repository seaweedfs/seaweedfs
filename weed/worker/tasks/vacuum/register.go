package vacuum

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Global variable to hold the task definition for configuration updates
var globalTaskDef *base.TaskDefinition

// Auto-register this task when the package is imported
func init() {
	RegisterVacuumTask()

	// Register config updater
	tasks.AutoRegisterConfigUpdater(types.TaskTypeVacuum, UpdateConfigFromPersistence)
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

		Config:     config,
		ConfigSpec: GetConfigSpec(),
		CreateTask: func(params *worker_pb.TaskParams) (types.Task, error) {
			if params == nil {
				return nil, fmt.Errorf("task parameters are required")
			}
			if len(params.Sources) == 0 {
				return nil, fmt.Errorf("at least one source is required for vacuum task")
			}
			return NewVacuumTask(
				fmt.Sprintf("vacuum-%d", params.VolumeId),
				params.Sources[0].Node, // Use first source node
				params.VolumeId,
				params.Collection,
			), nil
		},
		DetectionFunc:  Detection,
		ScanInterval:   2 * time.Hour,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  2,
		RepeatInterval: 7 * 24 * time.Hour,
	}

	// Store task definition globally for configuration updates
	globalTaskDef = taskDef

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}

// UpdateConfigFromPersistence updates the vacuum configuration from persistence
func UpdateConfigFromPersistence(configPersistence interface{}) error {
	if globalTaskDef == nil {
		return fmt.Errorf("vacuum task not registered")
	}

	// Load configuration from persistence
	newConfig := LoadConfigFromPersistence(configPersistence)
	if newConfig == nil {
		return fmt.Errorf("failed to load configuration from persistence")
	}

	// Update the task definition's config
	globalTaskDef.Config = newConfig

	glog.V(1).Infof("Updated vacuum task configuration from persistence")
	return nil
}
