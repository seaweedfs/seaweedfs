package types

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/view/components"
)

// TaskUITemplProvider defines how tasks provide their configuration UI using templ components
type TaskUITemplProvider interface {
	// GetTaskType returns the task type
	GetTaskType() TaskType

	// GetDisplayName returns the human-readable name
	GetDisplayName() string

	// GetDescription returns a description of what this task does
	GetDescription() string

	// GetIcon returns the icon CSS class or HTML for this task type
	GetIcon() string

	// RenderConfigSections renders the configuration as templ section data
	RenderConfigSections(currentConfig interface{}) ([]components.ConfigSectionData, error)

	// ParseConfigForm parses form data into configuration
	ParseConfigForm(formData map[string][]string) (interface{}, error)

	// GetCurrentConfig returns the current configuration
	GetCurrentConfig() interface{}

	// ApplyConfig applies the new configuration
	ApplyConfig(config interface{}) error
}

// UITemplRegistry manages task UI providers that use templ components
type UITemplRegistry struct {
	providers map[TaskType]TaskUITemplProvider
}

// NewUITemplRegistry creates a new templ-based UI registry
func NewUITemplRegistry() *UITemplRegistry {
	return &UITemplRegistry{
		providers: make(map[TaskType]TaskUITemplProvider),
	}
}

// RegisterUI registers a task UI provider
func (r *UITemplRegistry) RegisterUI(provider TaskUITemplProvider) {
	r.providers[provider.GetTaskType()] = provider
}

// GetProvider returns the UI provider for a task type
func (r *UITemplRegistry) GetProvider(taskType TaskType) TaskUITemplProvider {
	return r.providers[taskType]
}

// GetAllProviders returns all registered UI providers
func (r *UITemplRegistry) GetAllProviders() map[TaskType]TaskUITemplProvider {
	result := make(map[TaskType]TaskUITemplProvider)
	for k, v := range r.providers {
		result[k] = v
	}
	return result
}
