package replication

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleDetector implements replication task detection
type SimpleDetector struct {
	enabled       bool
	checkInterval time.Duration
	scanInterval  time.Duration
}

// NewSimpleDetector creates a new replication detector
func NewSimpleDetector() *SimpleDetector {
	return &SimpleDetector{
		enabled:       true,
		checkInterval: 4 * time.Hour,
		scanInterval:  time.Duration(6) * time.Hour,
	}
}

// GetTaskType returns the task type
func (d *SimpleDetector) GetTaskType() types.TaskType {
	return types.TaskTypeFixReplication
}

// ScanForTasks scans for volumes that have replication issues
func (d *SimpleDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
	if !d.enabled {
		return nil, nil
	}

	var results []*types.TaskDetectionResult
	now := time.Now()

	for _, metric := range volumeMetrics {
		// Check if replication count matches expected
		if metric.ReplicaCount != metric.ExpectedReplicas {
			priority := types.TaskPriorityHigh
			reason := ""

			if metric.ReplicaCount < metric.ExpectedReplicas {
				reason = "Under-replicated volume needs additional replicas"
				if metric.ReplicaCount == 1 && metric.ExpectedReplicas > 1 {
					priority = types.TaskPriorityHigh // Single point of failure
				}
			} else {
				reason = "Over-replicated volume has excess replicas"
				priority = types.TaskPriorityNormal // Less urgent
			}

			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeFixReplication,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   priority,
				Reason:     reason,
				Parameters: map[string]interface{}{
					"actual_replicas":   metric.ReplicaCount,
					"expected_replicas": metric.ExpectedReplicas,
				},
				ScheduleAt: now,
			}
			results = append(results, result)
		}
	}

	glog.V(2).Infof("Replication detector found %d tasks to schedule", len(results))
	return results, nil
}

// ScanInterval returns how often this task type should be scanned
func (d *SimpleDetector) ScanInterval() time.Duration {
	return d.scanInterval
}

// IsEnabled returns whether this task type is enabled
func (d *SimpleDetector) IsEnabled() bool {
	return d.enabled
}

// Configuration setters

func (d *SimpleDetector) SetEnabled(enabled bool) {
	d.enabled = enabled
}

func (d *SimpleDetector) SetCheckInterval(interval time.Duration) {
	d.checkInterval = interval
}

func (d *SimpleDetector) SetScanInterval(interval time.Duration) {
	d.scanInterval = interval
}
