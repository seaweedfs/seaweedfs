package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
)

// SchemaProvider implements the TaskConfigSchemaProvider interface for balance tasks
type SchemaProvider struct{}

// GetConfigSchema returns the schema for balance task configuration
func (p *SchemaProvider) GetConfigSchema() *tasks.TaskConfigSchema {
	return GetConfigSchema()
}

// init registers the balance schema provider
func init() {
	tasks.RegisterTaskConfigSchema("balance", &SchemaProvider{})
}
