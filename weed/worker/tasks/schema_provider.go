package tasks

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
)

// TaskConfigSchema defines the schema for task configuration
type TaskConfigSchema struct {
	config.Schema        // Embed common schema functionality
	TaskName      string `json:"task_name"`
	DisplayName   string `json:"display_name"`
	Description   string `json:"description"`
	Icon          string `json:"icon"`
}

// TaskConfigSchemaProvider is an interface for providing task configuration schemas
type TaskConfigSchemaProvider interface {
	GetConfigSchema() *TaskConfigSchema
}

// schemaRegistry maintains a registry of schema providers by task type
type schemaRegistry struct {
	providers map[string]TaskConfigSchemaProvider
	mutex     sync.RWMutex
}

var globalSchemaRegistry = &schemaRegistry{
	providers: make(map[string]TaskConfigSchemaProvider),
}

// RegisterTaskConfigSchema registers a schema provider for a task type
func RegisterTaskConfigSchema(taskType string, provider TaskConfigSchemaProvider) {
	globalSchemaRegistry.mutex.Lock()
	defer globalSchemaRegistry.mutex.Unlock()
	globalSchemaRegistry.providers[taskType] = provider
}

// GetTaskConfigSchema returns the schema for the specified task type
func GetTaskConfigSchema(taskType string) *TaskConfigSchema {
	globalSchemaRegistry.mutex.RLock()
	provider, exists := globalSchemaRegistry.providers[taskType]
	globalSchemaRegistry.mutex.RUnlock()

	if !exists {
		return nil
	}

	return provider.GetConfigSchema()
}
