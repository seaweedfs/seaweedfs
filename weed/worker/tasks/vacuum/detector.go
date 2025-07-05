package vacuum

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleDetector implements vacuum task detection using code instead of schemas
type SimpleDetector struct {
	enabled          bool
	garbageThreshold float64
	scanInterval     time.Duration
	minVolumeAge     time.Duration
}

// NewSimpleDetector creates a new simple vacuum detector
func NewSimpleDetector() *SimpleDetector {
	return &SimpleDetector{
		enabled:          true,
		garbageThreshold: 0.3, // 30% garbage ratio
		scanInterval:     30 * time.Minute,
		minVolumeAge:     1 * time.Hour,
	}
}

// GetTaskType returns the task type
func (d *SimpleDetector) GetTaskType() types.TaskType {
	return types.TaskTypeVacuum
}

// ScanForTasks scans for volumes that need vacuum operations
func (d *SimpleDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
	if !d.enabled {
		return nil, nil
	}

	var results []*types.TaskDetectionResult
	now := time.Now()

	for _, metric := range volumeMetrics {
		// Check if volume needs vacuum
		if metric.GarbageRatio >= d.garbageThreshold && metric.Age >= d.minVolumeAge {

			// Determine priority based on garbage ratio
			priority := types.TaskPriorityNormal
			if metric.GarbageRatio > 0.7 {
				priority = types.TaskPriorityHigh
			}

			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeVacuum,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   priority,
				Reason:     "Garbage ratio exceeds threshold",
				Parameters: map[string]interface{}{
					"garbage_ratio": metric.GarbageRatio,
					"threshold":     d.garbageThreshold,
				},
				ScheduleAt: now,
			}
			results = append(results, result)
		}
	}

	glog.V(2).Infof("Vacuum detector found %d tasks to schedule", len(results))
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

// SetEnabled sets whether the detector is enabled
func (d *SimpleDetector) SetEnabled(enabled bool) {
	d.enabled = enabled
}

// SetGarbageThreshold sets the garbage ratio threshold for triggering vacuum
func (d *SimpleDetector) SetGarbageThreshold(threshold float64) {
	d.garbageThreshold = threshold
}

// SetScanInterval sets how often to scan for vacuum tasks
func (d *SimpleDetector) SetScanInterval(interval time.Duration) {
	d.scanInterval = interval
}

// SetMinVolumeAge sets the minimum age a volume must have before being vacuum eligible
func (d *SimpleDetector) SetMinVolumeAge(age time.Duration) {
	d.minVolumeAge = age
}
