package tasks

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

var (
	globalRegistry *TaskRegistry
	registryOnce   sync.Once
)

// GetGlobalRegistry returns the global task registry (singleton)
func GetGlobalRegistry() *TaskRegistry {
	registryOnce.Do(func() {
		globalRegistry = NewTaskRegistry()
		glog.V(1).Infof("Created global task registry")
	})
	return globalRegistry
}

// AutoRegister registers a task directly with the global registry
func AutoRegister(taskType types.TaskType, factory types.TaskFactory) {
	registry := GetGlobalRegistry()
	registry.Register(taskType, factory)
	glog.V(1).Infof("Auto-registered task type: %s", taskType)
}
