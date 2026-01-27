package vacuum

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/util"
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

			// Generate task ID for future ActiveTopology integration
			taskID := fmt.Sprintf("vacuum_vol_%d_%d", metric.VolumeID, time.Now().Unix())

			result := &types.TaskDetectionResult{
				TaskID:     taskID, // For future ActiveTopology integration
				TaskType:   types.TaskTypeVacuum,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   priority,
				Reason:     "Volume has excessive garbage requiring vacuum",
				ScheduleAt: time.Now(),
			}

			// Check if ANY task already exists in ActiveTopology for this volume
			if clusterInfo != nil && clusterInfo.ActiveTopology != nil {
				if clusterInfo.ActiveTopology.HasAnyTask(metric.VolumeID) {
					glog.V(2).Infof("VACUUM: Skipping volume %d, task already exists in ActiveTopology", metric.VolumeID)
					continue
				}
			}

			// Create typed parameters for vacuum task
			result.TypedParams = createVacuumTaskParams(result, metric, vacuumConfig, clusterInfo)
			if result.TypedParams != nil {
				results = append(results, result)
			}
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

// createVacuumTaskParams creates typed parameters for vacuum tasks
// This function is moved from MaintenanceIntegration.createVacuumTaskParams to the detection logic
func createVacuumTaskParams(task *types.TaskDetectionResult, metric *types.VolumeHealthMetrics, vacuumConfig *Config, clusterInfo *types.ClusterInfo) *worker_pb.TaskParams {
	// Use configured values or defaults
	garbageThreshold := 0.3                    // Default 30%
	verifyChecksum := true                     // Default to verify
	batchSize := int32(1000)                   // Default batch size
	workingDir := "/tmp/seaweedfs_vacuum_work" // Default working directory

	if vacuumConfig != nil {
		garbageThreshold = vacuumConfig.GarbageThreshold
		// Note: VacuumTaskConfig has GarbageThreshold, MinVolumeAgeHours, MinIntervalSeconds
		// Other fields like VerifyChecksum, BatchSize, WorkingDir would need to be added
		// to the protobuf definition if they should be configurable
	}

	// Use DC and rack information directly from VolumeHealthMetrics
	sourceDC, sourceRack := metric.DataCenter, metric.Rack

	// Get server address from topology (required for vacuum tasks)
	if clusterInfo == nil || clusterInfo.ActiveTopology == nil {
		glog.Errorf("Topology not available for vacuum task on volume %d, skipping", task.VolumeID)
		return nil
	}
	address, err := util.ResolveServerAddress(task.Server, clusterInfo.ActiveTopology)
	if err != nil {
		glog.Errorf("Failed to resolve address for server %s for vacuum task on volume %d, skipping task: %v", task.Server, task.VolumeID, err)
		return nil
	}

	// Create typed protobuf parameters with unified sources
	return &worker_pb.TaskParams{
		TaskId:     task.TaskID, // Link to ActiveTopology pending task (if integrated)
		VolumeId:   task.VolumeID,
		Collection: task.Collection,
		VolumeSize: metric.Size, // Store original volume size for tracking changes

		// Unified sources array
		Sources: []*worker_pb.TaskSource{
			{
				Node:          address,
				VolumeId:      task.VolumeID,
				EstimatedSize: metric.Size,
				DataCenter:    sourceDC,
				Rack:          sourceRack,
			},
		},

		TaskParams: &worker_pb.TaskParams_VacuumParams{
			VacuumParams: &worker_pb.VacuumTaskParams{
				GarbageThreshold: garbageThreshold,
				ForceVacuum:      false,
				BatchSize:        batchSize,
				WorkingDir:       workingDir,
				VerifyChecksum:   verifyChecksum,
			},
		},
	}
}
