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

// BuildMaintenancePolicyFromTasks creates a maintenance policy with default configurations
// from all registered tasks using their UI providers
func BuildMaintenancePolicyFromTasks() *types.MaintenancePolicy {
	policy := types.NewMaintenancePolicy()

	// Get all registered task types from the UI registry
	uiRegistry := GetGlobalUIRegistry()

	for taskType, provider := range uiRegistry.GetAllProviders() {
		// Get the default configuration from the UI provider
		defaultConfig := provider.GetCurrentConfig()

		// Set the configuration in the policy
		policy.SetTaskConfig(taskType, defaultConfig)

		glog.V(3).Infof("Added default config for task type %s to policy", taskType)
	}

	glog.V(2).Infof("Built maintenance policy with %d task configurations", len(policy.TaskConfigs))
	return policy
}

// SetMaintenancePolicyFromTasks sets the default maintenance policy from registered tasks
func SetMaintenancePolicyFromTasks() {
	// This function can be called to initialize the policy from registered tasks
	// For now, we'll just log that this should be called by the integration layer
	glog.V(1).Infof("SetMaintenancePolicyFromTasks called - policy should be built by the integration layer")
}
