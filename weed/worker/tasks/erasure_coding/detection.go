package erasure_coding

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detection implements the detection logic for erasure coding tasks
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	ecConfig := config.(*Config)
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

// Scheduling implements the scheduling logic for erasure coding tasks
func Scheduling(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker, config base.TaskConfig) bool {
	ecConfig := config.(*Config)

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

// CreateTask creates a new erasure coding task instance
func CreateTask(params types.TaskParams) (types.TaskInterface, error) {
	// Extract configuration from params
	var config *Config
	if configData, ok := params.Parameters["config"]; ok {
		if configMap, ok := configData.(map[string]interface{}); ok {
			config = &Config{}
			if err := config.FromMap(configMap); err != nil {
				return nil, fmt.Errorf("failed to parse erasure coding config: %v", err)
			}
		}
	}

	if config == nil {
		config = NewDefaultConfig()
	}

	// Create and return the erasure coding task using existing Task type
	return NewTask(params.Server, params.VolumeID), nil
}
