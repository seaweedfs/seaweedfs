package balance

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements balance operation to redistribute volumes across volume servers
type Task struct {
	*tasks.BaseTask
	server     string
	volumeID   uint32
	collection string
}

// NewTask creates a new balance task instance
func NewTask(server string, volumeID uint32, collection string) *Task {
	task := &Task{
		BaseTask:   tasks.NewBaseTask(types.TaskTypeBalance),
		server:     server,
		volumeID:   volumeID,
		collection: collection,
	}
	return task
}

// Execute executes the balance task
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting balance task for volume %d on server %s (collection: %s)", t.volumeID, t.server, t.collection)

	// Simulate balance operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing cluster state", 2 * time.Second, 15},
		{"Identifying optimal placement", 3 * time.Second, 35},
		{"Moving volume data", 6 * time.Second, 75},
		{"Updating cluster metadata", 2 * time.Second, 95},
		{"Verifying balance", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("balance task cancelled")
		}

		glog.V(1).Infof("Balance task step: %s", step.name)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("Balance task completed for volume %d on server %s", t.volumeID, t.server)
	return nil
}

// Validate validates the task parameters
func (t *Task) Validate(params types.TaskParams) error {
	if params.VolumeID == 0 {
		return fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return fmt.Errorf("server is required")
	}
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
	// Base time for balance operation
	baseTime := 35 * time.Second

	// Could adjust based on volume size or cluster state
	return baseTime
}

// BalanceConfigV2 extends BaseConfig with balance-specific settings
type BalanceConfigV2 struct {
	base.BaseConfig
	ImbalanceThreshold float64 `json:"imbalance_threshold"`
	MinServerCount     int     `json:"min_server_count"`
}

// balanceDetection implements the detection logic for balance tasks
func balanceDetection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	balanceConfig := config.(*BalanceConfigV2)

	// Skip if cluster is too small
	minVolumeCount := 10
	if len(metrics) < minVolumeCount {
		return nil, nil
	}

	// Analyze volume distribution across servers
	serverVolumeCounts := make(map[string]int)
	for _, metric := range metrics {
		serverVolumeCounts[metric.Server]++
	}

	if len(serverVolumeCounts) < balanceConfig.MinServerCount {
		return nil, nil
	}

	// Calculate balance metrics
	totalVolumes := len(metrics)
	avgVolumesPerServer := float64(totalVolumes) / float64(len(serverVolumeCounts))

	maxVolumes := 0
	minVolumes := totalVolumes
	maxServer := ""
	minServer := ""

	for server, count := range serverVolumeCounts {
		if count > maxVolumes {
			maxVolumes = count
			maxServer = server
		}
		if count < minVolumes {
			minVolumes = count
			minServer = server
		}
	}

	// Check if imbalance exceeds threshold
	imbalanceRatio := float64(maxVolumes-minVolumes) / avgVolumesPerServer
	if imbalanceRatio <= balanceConfig.ImbalanceThreshold {
		return nil, nil
	}

	// Create balance task
	reason := fmt.Sprintf("Cluster imbalance detected: %.1f%% (max: %d on %s, min: %d on %s, avg: %.1f)",
		imbalanceRatio*100, maxVolumes, maxServer, minVolumes, minServer, avgVolumesPerServer)

	task := &types.TaskDetectionResult{
		TaskType:   types.TaskTypeBalance,
		Priority:   types.TaskPriorityNormal,
		Reason:     reason,
		ScheduleAt: time.Now(),
		Parameters: map[string]interface{}{
			"imbalance_ratio":        imbalanceRatio,
			"threshold":              balanceConfig.ImbalanceThreshold,
			"max_volumes":            maxVolumes,
			"min_volumes":            minVolumes,
			"avg_volumes_per_server": avgVolumesPerServer,
			"max_server":             maxServer,
			"min_server":             minServer,
			"total_servers":          len(serverVolumeCounts),
		},
	}

	return []*types.TaskDetectionResult{task}, nil
}

// balanceScheduling implements the scheduling logic for balance tasks
func balanceScheduling(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker, config base.TaskConfig) bool {
	balanceConfig := config.(*BalanceConfigV2)

	// Count running balance tasks
	runningBalanceCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeBalance {
			runningBalanceCount++
		}
	}

	// Check concurrency limit
	if runningBalanceCount >= balanceConfig.MaxConcurrent {
		return false
	}

	// Check if we have available workers
	availableWorkerCount := 0
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeBalance {
				availableWorkerCount++
				break
			}
		}
	}

	return availableWorkerCount > 0
}

// createBalanceTask creates a balance task instance
func createBalanceTask(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	task := NewTask(params.Server, params.VolumeID, params.Collection)
	task.SetEstimatedDuration(task.EstimateTime(params))
	return task, nil
}

// getBalanceConfigSpec returns the configuration schema for balance tasks
func getBalanceConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Balance Tasks",
				Description:  "Whether balance tasks should be automatically created",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "imbalance_threshold",
				JSONName:     "imbalance_threshold",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.1, // 10%
				MinValue:     0.01,
				MaxValue:     0.5,
				Required:     true,
				DisplayName:  "Imbalance Threshold",
				Description:  "Trigger balance when storage imbalance exceeds this ratio",
				Placeholder:  "0.10 (10%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 6 * 60 * 60,  // 6 hours
				MinValue:     1 * 60 * 60,  // 1 hour
				MaxValue:     24 * 60 * 60, // 24 hours
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for imbalanced volumes",
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
				MaxValue:     5,
				Required:     true,
				DisplayName:  "Max Concurrent Tasks",
				Description:  "Maximum number of balance tasks that can run simultaneously",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_server_count",
				JSONName:     "min_server_count",
				Type:         config.FieldTypeInt,
				DefaultValue: 3,
				MinValue:     2,
				MaxValue:     20,
				Required:     true,
				DisplayName:  "Minimum Server Count",
				Description:  "Only balance when at least this many servers are available",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
		},
	}
}

// initBalanceV2 registers the refactored balance task
func initBalanceV2() {
	// Create configuration instance
	config := &BalanceConfigV2{
		BaseConfig: base.BaseConfig{
			Enabled:             false,       // Conservative default
			ScanIntervalSeconds: 6 * 60 * 60, // 6 hours
			MaxConcurrent:       2,
		},
		ImbalanceThreshold: 0.1, // 10%
		MinServerCount:     3,
	}

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeBalance,
		Name:         "balance",
		DisplayName:  "Volume Balance",
		Description:  "Redistributes volumes across volume servers to optimize storage utilization",
		Icon:         "fas fa-balance-scale text-secondary",
		Capabilities: []string{"balance", "storage", "optimization"},

		Config:         config,
		ConfigSpec:     getBalanceConfigSpec(),
		CreateTask:     createBalanceTask,
		DetectionFunc:  balanceDetection,
		ScanInterval:   6 * time.Hour,
		SchedulingFunc: balanceScheduling,
		MaxConcurrent:  2,
		RepeatInterval: 12 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
