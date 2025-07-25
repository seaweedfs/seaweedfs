package erasure_coding

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// EcDetector implements erasure coding task detection
type EcDetector struct {
	enabled        bool
	volumeAgeHours int
	fullnessRatio  float64
	minSizeMB      int // Minimum volume size in MB before considering EC
	scanInterval   time.Duration
}

// Compile-time interface assertions
var (
	_ types.TaskDetector               = (*EcDetector)(nil)
	_ types.PolicyConfigurableDetector = (*EcDetector)(nil)
)

// NewEcDetector creates a new erasure coding detector with configurable defaults
func NewEcDetector() *EcDetector {
	return &EcDetector{
		enabled:        false, // Disabled by default for safety
		volumeAgeHours: 24,    // Require 24 hours age by default
		fullnessRatio:  0.8,   // 80% full by default
		minSizeMB:      100,   // Minimum 100MB before considering EC
		scanInterval:   2 * time.Hour,
	}
}

// GetTaskType returns the task type
func (d *EcDetector) GetTaskType() types.TaskType {
	return types.TaskTypeErasureCoding
}

// ScanForTasks scans for volumes that should be converted to erasure coding
func (d *EcDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
	if !d.enabled {
		glog.V(2).Infof("EC detector is disabled")
		return nil, nil
	}

	var results []*types.TaskDetectionResult
	now := time.Now()
	ageThreshold := time.Duration(d.volumeAgeHours) * time.Hour
	minSizeBytes := uint64(d.minSizeMB) * 1024 * 1024

	glog.V(2).Infof("EC detector scanning %d volumes with thresholds: age=%dh, fullness=%.2f, minSize=%dMB",
		len(volumeMetrics), d.volumeAgeHours, d.fullnessRatio, d.minSizeMB)

	for _, metric := range volumeMetrics {
		// Skip if already EC volume
		if metric.IsECVolume {
			continue
		}

		// Check minimum size requirement
		if metric.Size < minSizeBytes {
			continue
		}

		// Check age and fullness criteria
		if metric.Age >= ageThreshold && metric.FullnessRatio >= d.fullnessRatio {
			// Note: Removed read-only requirement for testing
			// In production, you might want to enable this:
			// if !metric.IsReadOnly {
			// 	continue
			// }

			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeErasureCoding,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   types.TaskPriorityLow, // EC is not urgent
				Reason: fmt.Sprintf("Volume meets EC criteria: age=%.1fh (>%dh), fullness=%.1f%% (>%.1f%%), size=%.1fMB (>%dMB)",
					metric.Age.Hours(), d.volumeAgeHours, metric.FullnessRatio*100, d.fullnessRatio*100,
					float64(metric.Size)/(1024*1024), d.minSizeMB),
				Parameters: map[string]interface{}{
					"age_hours":      int(metric.Age.Hours()),
					"fullness_ratio": metric.FullnessRatio,
					"size_mb":        int(metric.Size / (1024 * 1024)),
				},
				ScheduleAt: now,
			}
			results = append(results, result)

			glog.V(1).Infof("EC task detected for volume %d on %s: %s", metric.VolumeID, metric.Server, result.Reason)
		}
	}

	glog.V(1).Infof("EC detector found %d tasks to schedule", len(results))
	return results, nil
}

// ScanInterval returns how often this task type should be scanned
func (d *EcDetector) ScanInterval() time.Duration {
	return d.scanInterval
}

// IsEnabled returns whether this task type is enabled
func (d *EcDetector) IsEnabled() bool {
	return d.enabled
}

// Configuration methods for runtime configuration

// Configure sets detector configuration from policy
func (d *EcDetector) Configure(config map[string]interface{}) error {
	if enabled, ok := config["enabled"].(bool); ok {
		d.enabled = enabled
	}

	if ageHours, ok := config["volume_age_hours"].(float64); ok {
		d.volumeAgeHours = int(ageHours)
	}

	if fullnessRatio, ok := config["fullness_ratio"].(float64); ok {
		d.fullnessRatio = fullnessRatio
	}

	if minSizeMB, ok := config["min_size_mb"].(float64); ok {
		d.minSizeMB = int(minSizeMB)
	}

	glog.V(1).Infof("EC detector configured: enabled=%v, age=%dh, fullness=%.2f, minSize=%dMB",
		d.enabled, d.volumeAgeHours, d.fullnessRatio, d.minSizeMB)

	return nil
}

// Legacy compatibility methods for existing code

func (d *EcDetector) SetEnabled(enabled bool) {
	d.enabled = enabled
}

func (d *EcDetector) SetVolumeAgeHours(hours int) {
	d.volumeAgeHours = hours
}

func (d *EcDetector) SetFullnessRatio(ratio float64) {
	d.fullnessRatio = ratio
}

func (d *EcDetector) SetScanInterval(interval time.Duration) {
	d.scanInterval = interval
}

// PolicyConfigurableDetector interface implementation

// ConfigureFromPolicy configures the detector from maintenance policy
func (d *EcDetector) ConfigureFromPolicy(policy interface{}) {
	// Cast policy to maintenance policy type
	if maintenancePolicy, ok := policy.(*types.MaintenancePolicy); ok {
		// Get EC-specific configuration from policy
		ecConfig := maintenancePolicy.GetTaskConfig(types.TaskTypeErasureCoding)

		if ecConfig != nil {
			// Convert to map for easier access
			if configMap, ok := ecConfig.(map[string]interface{}); ok {
				d.Configure(configMap)
			} else {
				glog.Warningf("EC detector policy configuration is not a map: %T", ecConfig)
			}
		} else {
			// No specific configuration found, use defaults with policy-based enabled status
			enabled := maintenancePolicy.GlobalSettings != nil && maintenancePolicy.GlobalSettings.MaintenanceEnabled
			glog.V(2).Infof("No EC-specific config found, using default with enabled=%v", enabled)
			d.enabled = enabled
		}
	} else {
		glog.Warningf("ConfigureFromPolicy received unknown policy type: %T", policy)
	}
}

// GetVolumeAgeHours returns the current volume age threshold in hours
func (d *EcDetector) GetVolumeAgeHours() int {
	return d.volumeAgeHours
}

// GetFullnessRatio returns the current fullness ratio threshold
func (d *EcDetector) GetFullnessRatio() float64 {
	return d.fullnessRatio
}

// GetScanInterval returns the scan interval
func (d *EcDetector) GetScanInterval() time.Duration {
	return d.scanInterval
}
