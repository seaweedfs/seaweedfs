package balance

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detection implements the detection logic for balance tasks
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	balanceConfig := config.(*Config)

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

// Scheduling implements the scheduling logic for balance tasks
func Scheduling(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker, config base.TaskConfig) bool {
	balanceConfig := config.(*Config)

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

// CreateTask creates a new balance task instance
func CreateTask(params types.TaskParams) (types.TaskInterface, error) {
	// Extract configuration from params
	var config *Config
	if configData, ok := params.Parameters["config"]; ok {
		if configMap, ok := configData.(map[string]interface{}); ok {
			config = &Config{}
			if err := config.FromMap(configMap); err != nil {
				return nil, fmt.Errorf("failed to parse balance config: %v", err)
			}
		}
	}

	if config == nil {
		config = NewDefaultConfig()
	}

	// Create and return the balance task using existing Task type
	return NewTask(params.Server, params.VolumeID, params.Collection), nil
}
