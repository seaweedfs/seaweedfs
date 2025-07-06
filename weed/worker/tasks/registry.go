package tasks

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

var (
	globalRegistry      *TaskRegistry
	globalTypesRegistry *types.TaskRegistry
	registryOnce        sync.Once
	typesRegistryOnce   sync.Once
)

// GetGlobalRegistry returns the global task registry (singleton)
func GetGlobalRegistry() *TaskRegistry {
	registryOnce.Do(func() {
		globalRegistry = NewTaskRegistry()
		glog.V(1).Infof("Created global task registry")
	})
	return globalRegistry
}

// GetGlobalTypesRegistry returns the global types registry (singleton)
func GetGlobalTypesRegistry() *types.TaskRegistry {
	typesRegistryOnce.Do(func() {
		globalTypesRegistry = types.NewTaskRegistry()
		glog.V(1).Infof("Created global types registry")
	})
	return globalTypesRegistry
}

// AutoRegister registers a task directly with the global registry
func AutoRegister(taskType types.TaskType, factory types.TaskFactory) {
	registry := GetGlobalRegistry()
	registry.Register(taskType, factory)
	glog.V(1).Infof("Auto-registered task type: %s", taskType)
}

// AutoRegisterTypes registers a task with the global types registry
func AutoRegisterTypes(registerFunc func(*types.TaskRegistry)) {
	registry := GetGlobalTypesRegistry()
	registerFunc(registry)
	glog.V(1).Infof("Auto-registered task with types registry")
}
