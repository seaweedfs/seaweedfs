package erasure_coding

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Global variable to hold the task definition for configuration updates
var globalTaskDef *base.TaskDefinition

// Auto-register this task when the package is imported
func init() {
	RegisterErasureCodingTask()

	// Register config updater
	tasks.AutoRegisterConfigUpdater(types.TaskTypeErasureCoding, UpdateConfigFromPersistence)
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
		CreateTask:     nil, // Uses typed task system - see init() in ec.go
		DetectionFunc:  Detection,
		ScanInterval:   1 * time.Hour,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  1,
		RepeatInterval: 24 * time.Hour,
	}

	// Store task definition globally for configuration updates
	globalTaskDef = taskDef

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}

// UpdateConfigFromPersistence updates the erasure coding configuration from persistence
func UpdateConfigFromPersistence(configPersistence interface{}) error {
	if globalTaskDef == nil {
		return fmt.Errorf("erasure coding task not registered")
	}

	// Load configuration from persistence
	newConfig := LoadConfigFromPersistence(configPersistence)
	if newConfig == nil {
		return fmt.Errorf("failed to load configuration from persistence")
	}

	// Update the task definition's config
	globalTaskDef.Config = newConfig

	glog.V(1).Infof("Updated erasure coding task configuration from persistence")
	return nil
}
