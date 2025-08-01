package tasks

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

var (
	globalTypesRegistry *types.TaskRegistry
	globalUIRegistry    *types.UIRegistry
	globalTaskRegistry  *TaskRegistry
	typesRegistryOnce   sync.Once
	uiRegistryOnce      sync.Once
	taskRegistryOnce    sync.Once
)

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

// GetGlobalTaskRegistry returns the global task registry (singleton)
func GetGlobalTaskRegistry() *TaskRegistry {
	taskRegistryOnce.Do(func() {
		globalTaskRegistry = NewTaskRegistry()
		glog.V(1).Infof("Created global task registry")
	})
	return globalTaskRegistry
}

// AutoRegister registers a task with the global task registry
func AutoRegister(taskType types.TaskType, factory types.TaskFactory) {
	registry := GetGlobalTaskRegistry()
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

// TaskRegistry manages task factories
type TaskRegistry struct {
	factories map[types.TaskType]types.TaskFactory
	mutex     sync.RWMutex
}

// NewTaskRegistry creates a new task registry
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		factories: make(map[types.TaskType]types.TaskFactory),
	}
}

// Register adds a factory to the registry
func (r *TaskRegistry) Register(taskType types.TaskType, factory types.TaskFactory) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.factories[taskType] = factory
}

// Get returns a factory from the registry
func (r *TaskRegistry) Get(taskType types.TaskType) types.TaskFactory {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.factories[taskType]
}

// GetAll returns all registered factories
func (r *TaskRegistry) GetAll() map[types.TaskType]types.TaskFactory {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	result := make(map[types.TaskType]types.TaskFactory)
	for k, v := range r.factories {
		result[k] = v
	}
	return result
}
