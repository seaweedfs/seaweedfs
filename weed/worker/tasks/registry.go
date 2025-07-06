package tasks

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

var (
	globalRegistry      *TaskRegistry
	globalTypesRegistry *types.TaskRegistry
	globalUIRegistry    *types.UIRegistry
	registryOnce        sync.Once
	typesRegistryOnce   sync.Once
	uiRegistryOnce      sync.Once
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

// GetGlobalUIRegistry returns the global UI registry (singleton)
func GetGlobalUIRegistry() *types.UIRegistry {
	uiRegistryOnce.Do(func() {
		globalUIRegistry = types.NewUIRegistry()
		glog.V(1).Infof("Created global UI registry")
	})
	return globalUIRegistry
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

// AutoRegisterUI registers a UI provider with the global UI registry
func AutoRegisterUI(registerFunc func(*types.UIRegistry)) {
	registry := GetGlobalUIRegistry()
	registerFunc(registry)
	glog.V(1).Infof("Auto-registered task UI provider")
}

// SetDefaultCapabilitiesFromRegistry sets the default worker capabilities
// based on all registered task types
func SetDefaultCapabilitiesFromRegistry() {
	typesRegistry := GetGlobalTypesRegistry()

	var capabilities []types.TaskType
	for taskType := range typesRegistry.GetAllDetectors() {
		capabilities = append(capabilities, taskType)
	}

	// Set the default capabilities in the types package
	types.SetDefaultCapabilities(capabilities)

	glog.V(1).Infof("Set default worker capabilities from registry: %v", capabilities)
}
