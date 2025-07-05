package remote_upload

import (
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// RemoteUploadDetector implements remote upload task detection
type RemoteUploadDetector struct {
	enabled      bool
	ageHours     int
	pattern      string
	scanInterval time.Duration
}

// Compile-time interface assertions
var (
	_ types.TaskDetector = (*RemoteUploadDetector)(nil)
)

// NewRemoteUploadDetector creates a new remote upload detector
func NewRemoteUploadDetector() *RemoteUploadDetector {
	return &RemoteUploadDetector{
		enabled:      false,  // Conservative default
		ageHours:     24 * 7, // 1 week
		pattern:      "",     // Empty pattern means all collections
		scanInterval: 6 * time.Hour,
	}
}

// GetTaskType returns the task type
func (d *RemoteUploadDetector) GetTaskType() types.TaskType {
	return types.TaskTypeRemoteUpload
}

// ScanForTasks scans for volumes that should be uploaded to remote storage
func (d *RemoteUploadDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
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
		if metric.Age >= ageThreshold {
			// Check if collection matches pattern
			if d.pattern != "" && !d.matchesPattern(metric.Collection, d.pattern) {
				continue
			}

			// Check if volume is read-only (safer for remote upload)
			if !metric.IsReadOnly {
				continue
			}

			result := &types.TaskDetectionResult{
				TaskType:   types.TaskTypeRemoteUpload,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   types.TaskPriorityLow, // Remote upload is not urgent
				Reason:     "Volume is old enough for remote upload",
				Parameters: map[string]interface{}{
					"age_hours": int(metric.Age.Hours()),
					"pattern":   d.pattern,
				},
				ScheduleAt: now,
			}
			results = append(results, result)
		}
	}

	glog.V(2).Infof("Remote upload detector found %d tasks to schedule", len(results))
	return results, nil
}

// matchesPattern checks if a collection matches the pattern
func (d *RemoteUploadDetector) matchesPattern(collection, pattern string) bool {
	if pattern == "" {
		return true
	}

	// Simple pattern matching - could be enhanced with regex
	if pattern == "*" {
		return true
	}

	// Exact match
	if pattern == collection {
		return true
	}

	// Prefix match (pattern ends with *)
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(collection, prefix)
	}

	return false
}

// ScanInterval returns how often this task type should be scanned
func (d *RemoteUploadDetector) ScanInterval() time.Duration {
	return d.scanInterval
}

// IsEnabled returns whether this task type is enabled
func (d *RemoteUploadDetector) IsEnabled() bool {
	return d.enabled
}

// Configuration setters

func (d *RemoteUploadDetector) SetEnabled(enabled bool) {
	d.enabled = enabled
}

func (d *RemoteUploadDetector) SetAgeHours(hours int) {
	d.ageHours = hours
}

func (d *RemoteUploadDetector) SetPattern(pattern string) {
	d.pattern = pattern
}

func (d *RemoteUploadDetector) SetScanInterval(interval time.Duration) {
	d.scanInterval = interval
}

// Backward compatibility aliases
type SimpleDetector = RemoteUploadDetector

func NewSimpleDetector() *RemoteUploadDetector {
	return NewRemoteUploadDetector()
}
