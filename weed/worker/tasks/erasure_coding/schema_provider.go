package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
)

// SchemaProvider implements the TaskConfigSchemaProvider interface for erasure coding tasks
type SchemaProvider struct{}

// GetConfigSchema returns the schema for erasure coding task configuration
func (p *SchemaProvider) GetConfigSchema() *tasks.TaskConfigSchema {
	return GetConfigSchema()
}

// init registers the erasure coding schema provider
func init() {
	tasks.RegisterTaskConfigSchema("erasure_coding", &SchemaProvider{})
}
