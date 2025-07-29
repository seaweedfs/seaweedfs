package handlers

import "github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"

// TaskConfig defines the interface that all task configuration types must implement
type TaskConfig interface {
	// Common methods from BaseConfig
	IsEnabled() bool
	SetEnabled(enabled bool)
	Validate() error

	// Protobuf serialization methods - no more map[string]interface{}!
	ToTaskPolicy() *worker_pb.TaskPolicy
	FromTaskPolicy(policy *worker_pb.TaskPolicy) error
}

// TaskConfigProvider defines the interface for creating specific task config types
type TaskConfigProvider interface {
	NewConfig() TaskConfig
	GetTaskType() string
}
