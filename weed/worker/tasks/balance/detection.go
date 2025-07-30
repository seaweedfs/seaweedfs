package balance

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
	minVolumeCount := 2 // More reasonable for small clusters
	if len(metrics) < minVolumeCount {
		glog.Infof("BALANCE: No tasks created - cluster too small (%d volumes, need ≥%d)", len(metrics), minVolumeCount)
		return nil, nil
	}

	// Analyze volume distribution across servers
	serverVolumeCounts := make(map[string]int)
	for _, metric := range metrics {
		serverVolumeCounts[metric.Server]++
	}

	if len(serverVolumeCounts) < balanceConfig.MinServerCount {
		glog.Infof("BALANCE: No tasks created - too few servers (%d servers, need ≥%d)", len(serverVolumeCounts), balanceConfig.MinServerCount)
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
		glog.Infof("BALANCE: No tasks created - cluster well balanced. Imbalance=%.1f%% (threshold=%.1f%%). Max=%d volumes on %s, Min=%d on %s, Avg=%.1f",
			imbalanceRatio*100, balanceConfig.ImbalanceThreshold*100, maxVolumes, maxServer, minVolumes, minServer, avgVolumesPerServer)
		return nil, nil
	}

	// Select a volume from the overloaded server for balance
	var selectedVolume *types.VolumeHealthMetrics
	for _, metric := range metrics {
		if metric.Server == maxServer {
			selectedVolume = metric
			break
		}
	}

	if selectedVolume == nil {
		glog.Warningf("BALANCE: Could not find volume on overloaded server %s", maxServer)
		return nil, nil
	}

	// Create balance task with volume and destination planning info
	reason := fmt.Sprintf("Cluster imbalance detected: %.1f%% (max: %d on %s, min: %d on %s, avg: %.1f)",
		imbalanceRatio*100, maxVolumes, maxServer, minVolumes, minServer, avgVolumesPerServer)

	task := &types.TaskDetectionResult{
		TaskType:   types.TaskTypeBalance,
		VolumeID:   selectedVolume.VolumeID,
		Server:     selectedVolume.Server,
		Collection: selectedVolume.Collection,
		Priority:   types.TaskPriorityNormal,
		Reason:     reason,
		ScheduleAt: time.Now(),
		// TypedParams will be populated by the maintenance integration
		// with destination planning information
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
	// Create and return the balance task using existing Task type
	return NewTask(params.Server, params.VolumeID, params.Collection), nil
}
