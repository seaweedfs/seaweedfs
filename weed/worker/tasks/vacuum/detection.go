package vacuum

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detection implements the detection logic for vacuum tasks
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	vacuumConfig := config.(*Config)
	var results []*types.TaskDetectionResult
	minVolumeAge := time.Duration(vacuumConfig.MinVolumeAgeSeconds) * time.Second

	debugCount := 0
	skippedDueToGarbage := 0
	skippedDueToAge := 0

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
				ScheduleAt: time.Now(),
			}
			results = append(results, result)
		} else {
			// Debug why volume was not selected
			if debugCount < 5 { // Limit debug output to first 5 volumes
				if metric.GarbageRatio < vacuumConfig.GarbageThreshold {
					skippedDueToGarbage++
				}
				if metric.Age < minVolumeAge {
					skippedDueToAge++
				}
			}
			debugCount++
		}
	}

	// Log debug summary if no tasks were created
	if len(results) == 0 && len(metrics) > 0 {
		totalVolumes := len(metrics)
		glog.Infof("VACUUM: No tasks created for %d volumes. Threshold=%.2f%%, MinAge=%s. Skipped: %d (garbage<threshold), %d (age<minimum)",
			totalVolumes, vacuumConfig.GarbageThreshold*100, minVolumeAge, skippedDueToGarbage, skippedDueToAge)

		// Show details for first few volumes
		for i, metric := range metrics {
			if i >= 3 { // Limit to first 3 volumes
				break
			}
			glog.Infof("VACUUM: Volume %d: garbage=%.2f%% (need ≥%.2f%%), age=%s (need ≥%s)",
				metric.VolumeID, metric.GarbageRatio*100, vacuumConfig.GarbageThreshold*100,
				metric.Age.Truncate(time.Minute), minVolumeAge.Truncate(time.Minute))
		}
	}

	return results, nil
}

// Scheduling implements the scheduling logic for vacuum tasks
func Scheduling(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker, config base.TaskConfig) bool {
	vacuumConfig := config.(*Config)

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

// CreateTask creates a new vacuum task instance
func CreateTask(params types.TaskParams) (types.TaskInterface, error) {
	// Create and return the vacuum task using existing Task type
	return NewTask(params.Server, params.VolumeID), nil
}
