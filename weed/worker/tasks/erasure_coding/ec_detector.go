package erasure_coding

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detector implements erasure coding task detection
type Detector struct {
	enabled        bool
	volumeAgeHours int
	fullnessRatio  float64
	scanInterval   time.Duration
}

// NewDetector creates a new erasure coding detector
func NewDetector() *Detector {
	return &Detector{
		enabled:        false,  // Conservative default
		volumeAgeHours: 24 * 7, // 1 week
		fullnessRatio:  0.9,    // 90% full
		scanInterval:   2 * time.Hour,
	}
}

// GetTaskType returns the task type
func (d *Detector) GetTaskType() types.TaskType {
	return types.TaskTypeErasureCoding
}

// ScanForTasks scans for volumes that should be converted to erasure coding
func (d *Detector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
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
func (d *Detector) ScanInterval() time.Duration {
	return d.scanInterval
}

// IsEnabled returns whether this task type is enabled
func (d *Detector) IsEnabled() bool {
	return d.enabled
}

// Configuration setters

func (d *Detector) SetEnabled(enabled bool) {
	d.enabled = enabled
}

func (d *Detector) SetVolumeAgeHours(hours int) {
	d.volumeAgeHours = hours
}

func (d *Detector) SetFullnessRatio(ratio float64) {
	d.fullnessRatio = ratio
}

func (d *Detector) SetScanInterval(interval time.Duration) {
	d.scanInterval = interval
}

// ConfigureFromPolicy configures the detector based on the maintenance policy
func (d *Detector) ConfigureFromPolicy(policy interface{}) {
	// Type assert to the maintenance policy type we expect
	if maintenancePolicy, ok := policy.(interface {
		GetECEnabled() bool
		GetECVolumeAgeHours() int
		GetECFullnessRatio() float64
	}); ok {
		d.SetEnabled(maintenancePolicy.GetECEnabled())
		d.SetVolumeAgeHours(maintenancePolicy.GetECVolumeAgeHours())
		d.SetFullnessRatio(maintenancePolicy.GetECFullnessRatio())
	} else {
		glog.V(1).Infof("Could not configure EC detector from policy: unsupported policy type")
	}
}
