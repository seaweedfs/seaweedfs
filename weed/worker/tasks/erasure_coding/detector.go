package erasure_coding

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleDetector implements erasure coding task detection
type SimpleDetector struct {
	enabled        bool
	volumeAgeHours int
	fullnessRatio  float64
	scanInterval   time.Duration
}

// NewSimpleDetector creates a new erasure coding detector
func NewSimpleDetector() *SimpleDetector {
	return &SimpleDetector{
		enabled:        false,  // Conservative default
		volumeAgeHours: 24 * 7, // 1 week
		fullnessRatio:  0.9,    // 90% full
		scanInterval:   2 * time.Hour,
	}
}

// GetTaskType returns the task type
func (d *SimpleDetector) GetTaskType() types.TaskType {
	return types.TaskTypeErasureCoding
}

// ScanForTasks scans for volumes that should be converted to erasure coding
func (d *SimpleDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
	if !d.enabled {
		return nil, nil
	}

	var results []*types.TaskDetectionResult
	now := time.Now()
	ageThreshold := time.Duration(d.volumeAgeHours) * time.Hour

	for _, metric := range volumeMetrics {
		// Skip if already EC volume
		if metric.IsECVolume {
			continue
		}

		// Check age and fullness criteria
		if metric.Age >= ageThreshold && metric.FullnessRatio >= d.fullnessRatio {
			// Check if volume is read-only (safe for EC conversion)
			if !metric.IsReadOnly {
				continue
			}

			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeErasureCoding,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   types.TaskPriorityLow, // EC is not urgent
				Reason:     "Volume is old and full enough for EC conversion",
				Parameters: map[string]interface{}{
					"age_hours":      int(metric.Age.Hours()),
					"fullness_ratio": metric.FullnessRatio,
				},
				ScheduleAt: now,
			}
			results = append(results, result)
		}
	}

	glog.V(2).Infof("EC detector found %d tasks to schedule", len(results))
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

func (d *SimpleDetector) SetVolumeAgeHours(hours int) {
	d.volumeAgeHours = hours
}

func (d *SimpleDetector) SetFullnessRatio(ratio float64) {
	d.fullnessRatio = ratio
}

func (d *SimpleDetector) SetScanInterval(interval time.Duration) {
	d.scanInterval = interval
}
