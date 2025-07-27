package vacuum

import (
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
)

// SchemaProvider implements the TaskConfigSchemaProvider interface for vacuum tasks
type SchemaProvider struct{}

// GetConfigSchema returns the schema for vacuum task configuration
func (p *SchemaProvider) GetConfigSchema() *tasks.TaskConfigSchema {
	return GetConfigSchema()
}

// init registers the vacuum schema provider
func init() {
	tasks.RegisterTaskConfigSchema("vacuum", &SchemaProvider{})
}
