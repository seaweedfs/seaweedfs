package remote_upload

import (
	"regexp"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleDetector implements remote upload task detection
type SimpleDetector struct {
	enabled      bool
	ageHours     int
	pattern      string
	scanInterval time.Duration
}

// NewSimpleDetector creates a new remote upload detector
func NewSimpleDetector() *SimpleDetector {
	return &SimpleDetector{
		enabled:      false,
		ageHours:     24 * 30, // 1 month
		pattern:      "",
		scanInterval: 4 * time.Hour,
	}
}

// GetTaskType returns the task type
func (d *SimpleDetector) GetTaskType() types.TaskType {
	return types.TaskTypeRemoteUpload
}

// ScanForTasks scans for volumes that should be uploaded to remote storage
func (d *SimpleDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
	if !d.enabled {
		return nil, nil
	}

	var results []*types.TaskDetectionResult
	now := time.Now()
	ageThreshold := time.Duration(d.ageHours) * time.Hour

	for _, metric := range volumeMetrics {
		// Skip if already has remote copy
		if metric.HasRemoteCopy {
			continue
		}

		// Check age criteria
		if metric.Age < ageThreshold {
			continue
		}

		// Check collection pattern if specified
		if d.pattern != "" && !d.matchesPattern(metric.Collection, d.pattern) {
			continue
		}

		// Prefer read-only volumes for remote upload
		priority := types.TaskPriorityLow
		if metric.IsReadOnly {
			priority = types.TaskPriorityNormal
		}

		result := &types.TaskDetectionResult{
			TaskType:   types.TaskTypeRemoteUpload,
			VolumeID:   metric.VolumeID,
			Server:     metric.Server,
			Collection: metric.Collection,
			Priority:   priority,
			Reason:     "Volume is old enough for remote upload",
			Parameters: map[string]interface{}{
				"age_hours": int(metric.Age.Hours()),
			},
			ScheduleAt: now,
		}
		results = append(results, result)
	}

	glog.V(2).Infof("Remote upload detector found %d tasks to schedule", len(results))
	return results, nil
}

// matchesPattern checks if a collection name matches a pattern
func (d *SimpleDetector) matchesPattern(collection, pattern string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}

	// Convert shell-style pattern to regex
	regexPattern := strings.ReplaceAll(pattern, "*", ".*")
	regexPattern = "^" + regexPattern + "$"

	matched, err := regexp.MatchString(regexPattern, collection)
	if err != nil {
		glog.Errorf("Invalid pattern '%s': %v", pattern, err)
		return false
	}

	return matched
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

func (d *SimpleDetector) SetAgeHours(hours int) {
	d.ageHours = hours
}

func (d *SimpleDetector) SetPattern(pattern string) {
	d.pattern = pattern
}

func (d *SimpleDetector) SetScanInterval(interval time.Duration) {
	d.scanInterval = interval
}
