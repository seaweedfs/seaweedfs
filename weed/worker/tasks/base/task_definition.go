package base

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
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
	CreateTask func(params types.TaskParams) (types.TaskInterface, error)

	// Detection logic
	DetectionFunc func(metrics []*types.VolumeHealthMetrics, info *types.ClusterInfo, config TaskConfig) ([]*types.TaskDetectionResult, error)
	ScanInterval  time.Duration

	// Scheduling logic
	SchedulingFunc func(task *types.Task, running []*types.Task, workers []*types.Worker, config TaskConfig) bool
	MaxConcurrent  int
	RepeatInterval time.Duration
}

// TaskConfig provides a simple configuration interface
type TaskConfig interface {
	IsEnabled() bool
	SetEnabled(bool)
	Validate() error
	ToMap() map[string]interface{}
	FromMap(map[string]interface{}) error
}

// ConfigSpec defines the configuration schema
type ConfigSpec struct {
	Fields []*config.Field
}

// BaseConfig provides common configuration fields
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

// ToMap converts config to map
func (c *BaseConfig) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"enabled":               c.Enabled,
		"scan_interval_seconds": c.ScanIntervalSeconds,
		"max_concurrent":        c.MaxConcurrent,
	}
}

// FromMap loads config from map
func (c *BaseConfig) FromMap(data map[string]interface{}) error {
	if enabled, ok := data["enabled"].(bool); ok {
		c.Enabled = enabled
	}
	if interval, ok := data["scan_interval_seconds"].(int); ok {
		c.ScanIntervalSeconds = interval
	}
	if concurrent, ok := data["max_concurrent"].(int); ok {
		c.MaxConcurrent = concurrent
	}
	return nil
}
