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

// BalanceConfig extends BaseConfig with balance-specific settings
type BalanceConfig struct {
	base.BaseConfig
	ImbalanceThreshold float64 `json:"imbalance_threshold"`
	MinServerCount     int     `json:"min_server_count"`
}

// balanceDetection implements the detection logic for balance tasks
func balanceDetection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	balanceConfig := config.(*BalanceConfig)

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
	balanceConfig := config.(*BalanceConfig)

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

// createBalanceTask creates a new balance task instance
func createBalanceTask(params types.TaskParams) (types.TaskInterface, error) {
	// Extract configuration from params
	var config *BalanceConfig
	if configData, ok := params.Parameters["config"]; ok {
		if configMap, ok := configData.(map[string]interface{}); ok {
			config = &BalanceConfig{}
			if err := config.FromMap(configMap); err != nil {
				return nil, fmt.Errorf("failed to parse balance config: %v", err)
			}
		}
	}

	if config == nil {
		config = &BalanceConfig{
			BaseConfig: base.BaseConfig{
				Enabled:             true,
				ScanIntervalSeconds: 30 * 60, // 30 minutes
				MaxConcurrent:       1,
			},
			ImbalanceThreshold: 0.2, // 20%
			MinServerCount:     2,
		}
	}

	// Create and return the balance task using existing Task type
	return NewTask(params.Server, params.VolumeID, params.Collection), nil
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
				HelpText:     "Toggle this to enable or disable automatic balance task generation",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 30 * 60,
				MinValue:     5 * 60,
				MaxValue:     2 * 60 * 60,
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for volume distribution imbalances",
				HelpText:     "The system will check for volume distribution imbalances at this interval",
				Placeholder:  "30",
				Unit:         config.UnitMinutes,
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
				Description:  "Maximum number of balance tasks that can run simultaneously",
				HelpText:     "Limits the number of balance operations running at the same time",
				Placeholder:  "1 (default)",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "imbalance_threshold",
				JSONName:     "imbalance_threshold",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.2,
				MinValue:     0.05,
				MaxValue:     0.5,
				Required:     true,
				DisplayName:  "Imbalance Threshold",
				Description:  "Minimum imbalance ratio to trigger balancing",
				HelpText:     "Volume distribution imbalances above this threshold will trigger balancing",
				Placeholder:  "0.20 (20%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_server_count",
				JSONName:     "min_server_count",
				Type:         config.FieldTypeInt,
				DefaultValue: 2,
				MinValue:     2,
				MaxValue:     10,
				Required:     true,
				DisplayName:  "Minimum Server Count",
				Description:  "Minimum number of servers required for balancing",
				HelpText:     "Balancing will only occur if there are at least this many servers",
				Placeholder:  "2 (default)",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
		},
	}
}

// initBalance registers the refactored balance task
func initBalance() {
	// Create configuration instance
	config := &BalanceConfig{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 30 * 60, // 30 minutes
			MaxConcurrent:       1,
		},
		ImbalanceThreshold: 0.2, // 20%
		MinServerCount:     2,
	}

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeBalance,
		Name:         "balance",
		DisplayName:  "Volume Balance",
		Description:  "Balances volume distribution across servers",
		Icon:         "fas fa-balance-scale text-warning",
		Capabilities: []string{"balance", "distribution"},

		Config:         config,
		ConfigSpec:     getBalanceConfigSpec(),
		CreateTask:     createBalanceTask,
		DetectionFunc:  balanceDetection,
		ScanInterval:   30 * time.Minute,
		SchedulingFunc: balanceScheduling,
		MaxConcurrent:  1,
		RepeatInterval: 2 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
