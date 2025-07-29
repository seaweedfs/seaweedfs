package handlers

// TaskConfig defines the interface that all task configuration types must implement
type TaskConfig interface {
	// Common methods from BaseConfig
	IsEnabled() bool
	SetEnabled(enabled bool)
	Validate() error

	// Serialization methods
	ToMap() map[string]interface{}
	FromMap(data map[string]interface{}) error
}

// TaskConfigProvider defines the interface for creating specific task config types
type TaskConfigProvider interface {
	NewConfig() TaskConfig
	GetTaskType() string
}
