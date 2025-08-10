package ec_vacuum

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Global variable to hold the task definition for configuration updates
var globalTaskDef *base.TaskDefinition

// Auto-register this task when the package is imported
func init() {
	RegisterEcVacuumTask()

	// Register config updater
	tasks.AutoRegisterConfigUpdater(types.TaskType("ec_vacuum"), UpdateConfigFromPersistence)
}

// RegisterEcVacuumTask registers the EC vacuum task with the new architecture
func RegisterEcVacuumTask() {
	// Create configuration instance
	config := NewDefaultConfig()

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskType("ec_vacuum"),
		Name:         "ec_vacuum",
		DisplayName:  "EC Vacuum",
		Description:  "Cleans up deleted data from erasure coded volumes",
		Icon:         "fas fa-broom text-warning",
		Capabilities: []string{"ec_vacuum", "data_cleanup"},

		Config:     config,
		ConfigSpec: GetConfigSpec(),
		CreateTask: func(params *worker_pb.TaskParams) (types.Task, error) {
			if params == nil {
				return nil, fmt.Errorf("task parameters are required")
			}
			if params.VolumeId == 0 {
				return nil, fmt.Errorf("volume ID is required for EC vacuum task")
			}

			// Parse source nodes from task parameters
			sourceNodes := make(map[pb.ServerAddress]erasure_coding.ShardBits)

			// For now, we'll collect source nodes during execution since the exact
			// EC shard distribution is determined dynamically during detection
			// The task will discover and collect all shards during execution

			return NewEcVacuumTask(
				fmt.Sprintf("ec_vacuum-%d", params.VolumeId),
				params.VolumeId,
				params.Collection,
				sourceNodes,
				0, // default to generation 0 (current generation to vacuum)
			), nil
		},
		DetectionFunc:  Detection,
		ScanInterval:   24 * time.Hour, // Default scan every 24 hours
		SchedulingFunc: Scheduling,
		MaxConcurrent:  1,                  // Default max 1 concurrent
		RepeatInterval: 7 * 24 * time.Hour, // Repeat weekly for same volumes
	}

	// Store task definition globally for configuration updates
	globalTaskDef = taskDef

	// Register everything with a single function call!
	base.RegisterTask(taskDef)

	glog.V(1).Infof("✅ Registered EC vacuum task definition")
}

// UpdateConfigFromPersistence updates the EC vacuum configuration from persistence
func UpdateConfigFromPersistence(configPersistence interface{}) error {
	if globalTaskDef == nil {
		return fmt.Errorf("EC vacuum task not registered")
	}

	// Load configuration from persistence
	newConfig := LoadConfigFromPersistence(configPersistence)
	if newConfig == nil {
		return fmt.Errorf("failed to load configuration from persistence")
	}

	// Update the task definition's config
	globalTaskDef.Config = newConfig

	// Update scan interval from config
	globalTaskDef.ScanInterval = time.Duration(newConfig.ScanIntervalSeconds) * time.Second
	globalTaskDef.MaxConcurrent = newConfig.MaxConcurrent

	glog.V(1).Infof("Updated EC vacuum task configuration from persistence")
	return nil
}
