package erasure_coding

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// ErasureCodingConfigV2 extends BaseConfig with erasure coding specific settings
type ErasureCodingConfigV2 struct {
	base.BaseConfig
	QuietForSeconds  int     `json:"quiet_for_seconds"`
	FullnessRatio    float64 `json:"fullness_ratio"`
	CollectionFilter string  `json:"collection_filter"`
}

// ToMap converts config to map (extend base functionality)
func (c *ErasureCodingConfigV2) ToMap() map[string]interface{} {
	result := c.BaseConfig.ToMap()
	result["quiet_for_seconds"] = c.QuietForSeconds
	result["fullness_ratio"] = c.FullnessRatio
	result["collection_filter"] = c.CollectionFilter
	return result
}

// FromMap loads config from map (extend base functionality)
func (c *ErasureCodingConfigV2) FromMap(data map[string]interface{}) error {
	// Load base config first
	if err := c.BaseConfig.FromMap(data); err != nil {
		return err
	}

	// Load erasure coding specific config
	if quietFor, ok := data["quiet_for_seconds"].(int); ok {
		c.QuietForSeconds = quietFor
	}
	if fullness, ok := data["fullness_ratio"].(float64); ok {
		c.FullnessRatio = fullness
	}
	if filter, ok := data["collection_filter"].(string); ok {
		c.CollectionFilter = filter
	}
	return nil
}

// ecDetection implements the detection logic for erasure coding tasks
func ecDetection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	ecConfig := config.(*ErasureCodingConfigV2)
	var results []*types.TaskDetectionResult
	now := time.Now()
	quietThreshold := time.Duration(ecConfig.QuietForSeconds) * time.Second
	minSizeBytes := uint64(100) * 1024 * 1024 // 100MB minimum

	for _, metric := range metrics {
		// Skip if already EC volume
		if metric.IsECVolume {
			continue
		}

		// Check minimum size requirement
		if metric.Size < minSizeBytes {
			continue
		}

		// Check collection filter if specified
		if ecConfig.CollectionFilter != "" {
			// Parse comma-separated collections
			allowedCollections := make(map[string]bool)
			for _, collection := range strings.Split(ecConfig.CollectionFilter, ",") {
				allowedCollections[strings.TrimSpace(collection)] = true
			}
			// Skip if volume's collection is not in the allowed list
			if !allowedCollections[metric.Collection] {
				continue
			}
		}

		// Check quiet duration and fullness criteria
		if metric.Age >= quietThreshold && metric.FullnessRatio >= ecConfig.FullnessRatio {
			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeErasureCoding,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   types.TaskPriorityLow, // EC is not urgent
				Reason: fmt.Sprintf("Volume meets EC criteria: quiet for %.1fs (>%ds), fullness=%.1f%% (>%.1f%%), size=%.1fMB (>100MB)",
					metric.Age.Seconds(), ecConfig.QuietForSeconds, metric.FullnessRatio*100, ecConfig.FullnessRatio*100,
					float64(metric.Size)/(1024*1024)),
				Parameters: map[string]interface{}{
					"age_seconds":    int(metric.Age.Seconds()),
					"fullness_ratio": metric.FullnessRatio,
					"size_mb":        int(metric.Size / (1024 * 1024)),
				},
				ScheduleAt: now,
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// ecScheduling implements the scheduling logic for erasure coding tasks
func ecScheduling(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker, config base.TaskConfig) bool {
	ecConfig := config.(*ErasureCodingConfigV2)

	// Check if we have available workers
	if len(availableWorkers) == 0 {
		return false
	}

	// Count running EC tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeErasureCoding {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= ecConfig.MaxConcurrent {
		return false
	}

	// Check if any worker can handle EC tasks
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeErasureCoding {
				return true
			}
		}
	}

	return false
}

// createErasureCodingTask creates an erasure coding task instance
func createErasureCodingTask(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	// Extract additional parameters for comprehensive EC
	masterClient := "localhost:9333"    // Default master client
	workDir := "/tmp/seaweedfs_ec_work" // Default work directory

	if mc, ok := params.Parameters["master_client"].(string); ok && mc != "" {
		masterClient = mc
	}
	if wd, ok := params.Parameters["work_dir"].(string); ok && wd != "" {
		workDir = wd
	}

	// Create EC task with comprehensive capabilities
	task := NewTaskWithParams(params.Server, params.VolumeID, masterClient, workDir)

	// Set gRPC dial option if provided
	if params.GrpcDialOption != nil {
		task.SetDialOption(params.GrpcDialOption)
	}

	task.SetEstimatedDuration(task.EstimateTime(params))
	return task, nil
}

// getErasureCodingConfigSpec returns the configuration schema for erasure coding tasks
func getErasureCodingConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Erasure Coding Tasks",
				Description:  "Whether erasure coding tasks should be automatically created",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "quiet_for_seconds",
				JSONName:     "quiet_for_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 7 * 24 * 60 * 60,  // 7 days
				MinValue:     1 * 24 * 60 * 60,  // 1 day
				MaxValue:     30 * 24 * 60 * 60, // 30 days
				Required:     true,
				DisplayName:  "Quiet For Duration",
				Description:  "Only apply erasure coding to volumes that have not been modified for this duration",
				Unit:         config.UnitDays,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 12 * 60 * 60, // 12 hours
				MinValue:     2 * 60 * 60,  // 2 hours
				MaxValue:     24 * 60 * 60, // 24 hours
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for volumes needing erasure coding",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "max_concurrent",
				JSONName:     "max_concurrent",
				Type:         config.FieldTypeInt,
				DefaultValue: 1,
				MinValue:     1,
				MaxValue:     3,
				Required:     true,
				DisplayName:  "Max Concurrent Tasks",
				Description:  "Maximum number of erasure coding tasks that can run simultaneously",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "fullness_ratio",
				JSONName:     "fullness_ratio",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.9, // 90%
				MinValue:     0.5,
				MaxValue:     1.0,
				Required:     true,
				DisplayName:  "Fullness Ratio",
				Description:  "Only apply erasure coding to volumes with fullness ratio above this threshold",
				Placeholder:  "0.90 (90%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "collection_filter",
				JSONName:     "collection_filter",
				Type:         config.FieldTypeString,
				DefaultValue: "",
				Required:     false,
				DisplayName:  "Collection Filter",
				Description:  "Only apply erasure coding to volumes in these collections (comma-separated, leave empty for all)",
				Placeholder:  "collection1,collection2",
				InputType:    "text",
				CSSClasses:   "form-control",
			},
		},
	}
}

// initErasureCodingV2 registers the refactored erasure coding task
func initErasureCodingV2() {
	// Create configuration instance
	config := &ErasureCodingConfigV2{
		BaseConfig: base.BaseConfig{
			Enabled:             false,        // Conservative default - enable via configuration
			ScanIntervalSeconds: 12 * 60 * 60, // 12 hours
			MaxConcurrent:       1,            // Conservative default
		},
		QuietForSeconds:  7 * 24 * 60 * 60, // 7 days quiet period
		FullnessRatio:    0.90,             // 90% full threshold
		CollectionFilter: "",               // No collection filter by default
	}

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeErasureCoding,
		Name:         "erasure_coding",
		DisplayName:  "Erasure Coding",
		Description:  "Converts volumes to erasure coded format for improved data durability",
		Icon:         "fas fa-shield-alt text-info",
		Capabilities: []string{"erasure_coding", "storage", "durability"},

		Config:         config,
		ConfigSpec:     getErasureCodingConfigSpec(),
		CreateTask:     createErasureCodingTask,
		DetectionFunc:  ecDetection,
		ScanInterval:   12 * time.Hour,
		SchedulingFunc: ecScheduling,
		MaxConcurrent:  1,
		RepeatInterval: 24 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
