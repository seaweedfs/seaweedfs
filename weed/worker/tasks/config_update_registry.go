package tasks

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// ConfigUpdateFunc is a function type for updating task configurations
type ConfigUpdateFunc func(configPersistence interface{}) error

// ConfigUpdateRegistry manages config update functions for all task types
type ConfigUpdateRegistry struct {
	updaters map[types.TaskType]ConfigUpdateFunc
	mutex    sync.RWMutex
}

var (
	globalConfigUpdateRegistry *ConfigUpdateRegistry
	configUpdateRegistryOnce   sync.Once
)

// GetGlobalConfigUpdateRegistry returns the global config update registry (singleton)
func GetGlobalConfigUpdateRegistry() *ConfigUpdateRegistry {
	configUpdateRegistryOnce.Do(func() {
		globalConfigUpdateRegistry = &ConfigUpdateRegistry{
			updaters: make(map[types.TaskType]ConfigUpdateFunc),
		}
		glog.V(1).Infof("Created global config update registry")
	})
	return globalConfigUpdateRegistry
}

// RegisterConfigUpdater registers a config update function for a task type
func (r *ConfigUpdateRegistry) RegisterConfigUpdater(taskType types.TaskType, updateFunc ConfigUpdateFunc) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.updaters[taskType] = updateFunc
	glog.V(1).Infof("Registered config updater for task type: %s", taskType)
}

// UpdateAllConfigs updates configurations for all registered task types
func (r *ConfigUpdateRegistry) UpdateAllConfigs(configPersistence interface{}) {
	r.mutex.RLock()
	updaters := make(map[types.TaskType]ConfigUpdateFunc)
	for k, v := range r.updaters {
		updaters[k] = v
	}
	r.mutex.RUnlock()

	for taskType, updateFunc := range updaters {
		if err := updateFunc(configPersistence); err != nil {
			glog.Warningf("Failed to load %s configuration from persistence: %v", taskType, err)
		} else {
			glog.V(1).Infof("Loaded %s configuration from persistence", taskType)
		}
	}

	glog.V(1).Infof("All task configurations loaded from persistence")
}

// AutoRegisterConfigUpdater is a convenience function for registering config updaters
func AutoRegisterConfigUpdater(taskType types.TaskType, updateFunc ConfigUpdateFunc) {
	registry := GetGlobalConfigUpdateRegistry()
	registry.RegisterConfigUpdater(taskType, updateFunc)
}
