package balance

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
	RegisterBalanceTask()

	// Register config updater
	tasks.AutoRegisterConfigUpdater(types.TaskTypeBalance, UpdateConfigFromPersistence)
}

// RegisterBalanceTask registers the balance task with the new architecture
func RegisterBalanceTask() {
	// Create configuration instance
	config := NewDefaultConfig()

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeBalance,
		Name:         "balance",
		DisplayName:  "Volume Balance",
		Description:  "Balances volume distribution across servers",
		Icon:         "fas fa-balance-scale text-warning",
		Capabilities: []string{"balance", "distribution"},

		Config:     config,
		ConfigSpec: GetConfigSpec(),
		CreateTask: func(params *worker_pb.TaskParams) (types.Task, error) {
			if params == nil {
				return nil, fmt.Errorf("task parameters are required")
			}
			if len(params.Sources) == 0 {
				return nil, fmt.Errorf("at least one source is required for balance task")
			}
			return NewBalanceTask(
				fmt.Sprintf("balance-%d", params.VolumeId),
				params.Sources[0].Node, // Use first source node
				params.VolumeId,
				params.Collection,
			), nil
		},
		DetectionFunc:  Detection,
		ScanInterval:   30 * time.Minute,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  1,
		RepeatInterval: 2 * time.Hour,
	}

	// Store task definition globally for configuration updates
	globalTaskDef = taskDef

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}

// UpdateConfigFromPersistence updates the balance configuration from persistence
func UpdateConfigFromPersistence(configPersistence interface{}) error {
	if globalTaskDef == nil {
		return fmt.Errorf("balance task not registered")
	}

	// Load configuration from persistence
	newConfig := LoadConfigFromPersistence(configPersistence)
	if newConfig == nil {
		return fmt.Errorf("failed to load configuration from persistence")
	}

	// Update the task definition's config
	globalTaskDef.Config = newConfig

	glog.V(1).Infof("Updated balance task configuration from persistence")
	return nil
}
