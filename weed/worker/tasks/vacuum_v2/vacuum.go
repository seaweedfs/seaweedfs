package vacuum_v2

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VacuumConfig extends BaseConfig with vacuum-specific settings
type VacuumConfig struct {
	base.BaseConfig
	GarbageThreshold    float64 `json:"garbage_threshold"`
	MinVolumeAgeSeconds int     `json:"min_volume_age_seconds"`
	MinIntervalSeconds  int     `json:"min_interval_seconds"`
}

// ToMap converts config to map (extend base functionality)
func (c *VacuumConfig) ToMap() map[string]interface{} {
	result := c.BaseConfig.ToMap()
	result["garbage_threshold"] = c.GarbageThreshold
	result["min_volume_age_seconds"] = c.MinVolumeAgeSeconds
	result["min_interval_seconds"] = c.MinIntervalSeconds
	return result
}

// FromMap loads config from map (extend base functionality)
func (c *VacuumConfig) FromMap(data map[string]interface{}) error {
	// Load base config first
	if err := c.BaseConfig.FromMap(data); err != nil {
		return err
	}

	// Load vacuum-specific config
	if threshold, ok := data["garbage_threshold"].(float64); ok {
		c.GarbageThreshold = threshold
	}
	if ageSeconds, ok := data["min_volume_age_seconds"].(int); ok {
		c.MinVolumeAgeSeconds = ageSeconds
	}
	if intervalSeconds, ok := data["min_interval_seconds"].(int); ok {
		c.MinIntervalSeconds = intervalSeconds
	}
	return nil
}

// vacuumDetection implements the detection logic for vacuum tasks
func vacuumDetection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	vacuumConfig := config.(*VacuumConfig)
	var results []*types.TaskDetectionResult
	minVolumeAge := time.Duration(vacuumConfig.MinVolumeAgeSeconds) * time.Second

	for _, metric := range metrics {
		// Check if volume needs vacuum
		if metric.GarbageRatio >= vacuumConfig.GarbageThreshold && metric.Age >= minVolumeAge {
			priority := types.TaskPriorityNormal
			if metric.GarbageRatio > 0.6 {
				priority = types.TaskPriorityHigh
			}

			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeVacuum,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   priority,
				Reason:     "Volume has excessive garbage requiring vacuum",
				Parameters: map[string]interface{}{
					"garbage_ratio": metric.GarbageRatio,
					"volume_age":    metric.Age.String(),
				},
				ScheduleAt: time.Now(),
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// vacuumScheduling implements the scheduling logic for vacuum tasks
func vacuumScheduling(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker, config base.TaskConfig) bool {
	vacuumConfig := config.(*VacuumConfig)

	// Count running vacuum tasks
	runningVacuumCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeVacuum {
			runningVacuumCount++
		}
	}

	// Check concurrency limit
	if runningVacuumCount >= vacuumConfig.MaxConcurrent {
		return false
	}

	// Check for available workers with vacuum capability
	for _, worker := range availableWorkers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			for _, capability := range worker.Capabilities {
				if capability == types.TaskTypeVacuum {
					return true
				}
			}
		}
	}

	return false
}

// createVacuumTask creates a vacuum task instance
func createVacuumTask(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	// Reuse existing vacuum task implementation
	task := vacuum.NewTask(params.Server, params.VolumeID)
	task.SetEstimatedDuration(task.EstimateTime(params))
	return task, nil
}

// getConfigSpec returns the configuration schema for vacuum tasks
func getConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Vacuum Tasks",
				Description:  "Whether vacuum tasks should be automatically created",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "garbage_threshold",
				JSONName:     "garbage_threshold",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.3,
				MinValue:     0.0,
				MaxValue:     1.0,
				Required:     true,
				DisplayName:  "Garbage Percentage Threshold",
				Description:  "Trigger vacuum when garbage ratio exceeds this percentage",
				Placeholder:  "0.30 (30%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 2 * 60 * 60,
				MinValue:     10 * 60,
				MaxValue:     24 * 60 * 60,
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for volumes needing vacuum",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "max_concurrent",
				JSONName:     "max_concurrent",
				Type:         config.FieldTypeInt,
				DefaultValue: 2,
				MinValue:     1,
				MaxValue:     10,
				Required:     true,
				DisplayName:  "Max Concurrent Tasks",
				Description:  "Maximum number of vacuum tasks that can run simultaneously",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_volume_age_seconds",
				JSONName:     "min_volume_age_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 24 * 60 * 60,
				MinValue:     1 * 60 * 60,
				MaxValue:     7 * 24 * 60 * 60,
				Required:     true,
				DisplayName:  "Minimum Volume Age",
				Description:  "Only vacuum volumes older than this duration",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_interval_seconds",
				JSONName:     "min_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 7 * 24 * 60 * 60,
				MinValue:     1 * 24 * 60 * 60,
				MaxValue:     30 * 24 * 60 * 60,
				Required:     true,
				DisplayName:  "Minimum Interval",
				Description:  "Minimum time between vacuum operations on the same volume",
				Unit:         config.UnitDays,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
		},
	}
}

// RegisterVacuumV2 registers the refactored vacuum task
func init() {
	// Create configuration instance
	config := &VacuumConfig{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 2 * 60 * 60, // 2 hours
			MaxConcurrent:       2,
		},
		GarbageThreshold:    0.3,              // 30%
		MinVolumeAgeSeconds: 24 * 60 * 60,     // 24 hours
		MinIntervalSeconds:  7 * 24 * 60 * 60, // 7 days
	}

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeVacuum,
		Name:         "vacuum_v2",
		DisplayName:  "Volume Vacuum (V2)",
		Description:  "Reclaims disk space by removing deleted files from volumes",
		Icon:         "fas fa-broom text-primary",
		Capabilities: []string{"vacuum", "storage"},

		Config:         config,
		ConfigSpec:     getConfigSpec(),
		CreateTask:     createVacuumTask,
		DetectionFunc:  vacuumDetection,
		ScanInterval:   2 * time.Hour,
		SchedulingFunc: vacuumScheduling,
		MaxConcurrent:  2,
		RepeatInterval: 7 * 24 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
