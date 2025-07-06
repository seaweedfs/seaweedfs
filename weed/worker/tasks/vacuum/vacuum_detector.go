package vacuum

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VacuumDetector implements vacuum task detection using code instead of schemas
type VacuumDetector struct {
	enabled          bool
	garbageThreshold float64
	minVolumeAge     time.Duration
	scanInterval     time.Duration
}

// Compile-time interface assertions
var (
	_ types.TaskDetector               = (*VacuumDetector)(nil)
	_ types.PolicyConfigurableDetector = (*VacuumDetector)(nil)
)

// NewVacuumDetector creates a new simple vacuum detector
func NewVacuumDetector() *VacuumDetector {
	return &VacuumDetector{
		enabled:          true,
		garbageThreshold: 0.3,
		minVolumeAge:     24 * time.Hour,
		scanInterval:     30 * time.Minute,
	}
}

// GetTaskType returns the task type
func (d *VacuumDetector) GetTaskType() types.TaskType {
	return types.TaskTypeVacuum
}

// ScanForTasks scans for volumes that need vacuum operations
func (d *VacuumDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
	if !d.enabled {
		return nil, nil
	}

	var results []*types.TaskDetectionResult

	for _, metric := range volumeMetrics {
		// Check if volume needs vacuum
		if metric.GarbageRatio >= d.garbageThreshold && metric.Age >= d.minVolumeAge {
			// Higher priority for volumes with more garbage
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
				Parameters: map[string]interface{}{
					"garbage_ratio": metric.GarbageRatio,
					"volume_age":    metric.Age.String(),
				},
				ScheduleAt: time.Now(),
			}
			results = append(results, result)
		}
	}

	glog.V(2).Infof("Vacuum detector found %d volumes needing vacuum", len(results))
	return results, nil
}

// ScanInterval returns how often this detector should scan
func (d *VacuumDetector) ScanInterval() time.Duration {
	return d.scanInterval
}

// IsEnabled returns whether this detector is enabled
func (d *VacuumDetector) IsEnabled() bool {
	return d.enabled
}

// Configuration setters

func (d *VacuumDetector) SetEnabled(enabled bool) {
	d.enabled = enabled
}

func (d *VacuumDetector) SetGarbageThreshold(threshold float64) {
	d.garbageThreshold = threshold
}

func (d *VacuumDetector) SetScanInterval(interval time.Duration) {
	d.scanInterval = interval
}

func (d *VacuumDetector) SetMinVolumeAge(age time.Duration) {
	d.minVolumeAge = age
}

// GetGarbageThreshold returns the current garbage threshold
func (d *VacuumDetector) GetGarbageThreshold() float64 {
	return d.garbageThreshold
}

// GetMinVolumeAge returns the minimum volume age
func (d *VacuumDetector) GetMinVolumeAge() time.Duration {
	return d.minVolumeAge
}

// GetScanInterval returns the scan interval
func (d *VacuumDetector) GetScanInterval() time.Duration {
	return d.scanInterval
}

// ConfigureFromPolicy configures the detector based on the maintenance policy
func (d *VacuumDetector) ConfigureFromPolicy(policy interface{}) {
	// Type assert to the maintenance policy type we expect
	if maintenancePolicy, ok := policy.(interface {
		GetVacuumEnabled() bool
		GetVacuumGarbageRatio() float64
	}); ok {
		d.SetEnabled(maintenancePolicy.GetVacuumEnabled())
		d.SetGarbageThreshold(maintenancePolicy.GetVacuumGarbageRatio())
	} else {
		glog.V(1).Infof("Could not configure vacuum detector from policy: unsupported policy type")
	}
}
