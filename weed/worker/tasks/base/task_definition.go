package base

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TaskDefinition encapsulates everything needed to define a complete task type
type TaskDefinition struct {
	// Basic task information
	Type         types.TaskType
	Name         string
	DisplayName  string
	Description  string
	Icon         string
	Capabilities []string

	// Task configuration
	Config     TaskConfig
	ConfigSpec ConfigSpec

	// Task creation
	CreateTask func(params *worker_pb.TaskParams) (types.Task, error)

	// Detection logic
	DetectionFunc func(metrics []*types.VolumeHealthMetrics, info *types.ClusterInfo, config TaskConfig) ([]*types.TaskDetectionResult, error)
	ScanInterval  time.Duration

	// Scheduling logic
	SchedulingFunc func(task *types.TaskInput, running []*types.TaskInput, workers []*types.WorkerData, config TaskConfig) bool
	MaxConcurrent  int
	RepeatInterval time.Duration
}

// TaskConfig provides a configuration interface that supports type-safe defaults
type TaskConfig interface {
	config.ConfigWithDefaults // Extends ConfigWithDefaults for type-safe schema operations
	IsEnabled() bool
	SetEnabled(bool)
	ToTaskPolicy() *worker_pb.TaskPolicy
	FromTaskPolicy(policy *worker_pb.TaskPolicy) error
}

// ConfigSpec defines the configuration schema
type ConfigSpec struct {
	Fields []*config.Field
}

// BaseConfig provides common configuration fields with reflection-based serialization
type BaseConfig struct {
	Enabled             bool `json:"enabled"`
	ScanIntervalSeconds int  `json:"scan_interval_seconds"`
	MaxConcurrent       int  `json:"max_concurrent"`
}

// IsEnabled returns whether the task is enabled
func (c *BaseConfig) IsEnabled() bool {
	return c.Enabled
}

// SetEnabled sets whether the task is enabled
func (c *BaseConfig) SetEnabled(enabled bool) {
	c.Enabled = enabled
}

// Validate validates the base configuration
func (c *BaseConfig) Validate() error {
	// Common validation logic
	return nil
}

// ToMap converts config to map using reflection
// ToTaskPolicy converts BaseConfig to protobuf (partial implementation)
// Note: Concrete implementations should override this to include task-specific config
func (c *BaseConfig) ToTaskPolicy() *worker_pb.TaskPolicy {
	return &worker_pb.TaskPolicy{
		Enabled:               c.Enabled,
		MaxConcurrent:         int32(c.MaxConcurrent),
		RepeatIntervalSeconds: int32(c.ScanIntervalSeconds),
		CheckIntervalSeconds:  int32(c.ScanIntervalSeconds),
		// TaskConfig field should be set by concrete implementations
	}
}

// FromTaskPolicy loads BaseConfig from protobuf (partial implementation)
// Note: Concrete implementations should override this to handle task-specific config
func (c *BaseConfig) FromTaskPolicy(policy *worker_pb.TaskPolicy) error {
	if policy == nil {
		return fmt.Errorf("policy is nil")
	}
	c.Enabled = policy.Enabled
	c.MaxConcurrent = int(policy.MaxConcurrent)
	c.ScanIntervalSeconds = int(policy.RepeatIntervalSeconds)
	return nil
}

// ApplySchemaDefaults applies default values from schema using reflection
func (c *BaseConfig) ApplySchemaDefaults(schema *config.Schema) error {
	// Use reflection-based approach for BaseConfig since it needs to handle embedded structs
	return schema.ApplyDefaultsToProtobuf(c)
}

