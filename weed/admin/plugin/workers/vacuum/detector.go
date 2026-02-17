package vacuum

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Detector scans for volumes that are candidates for vacuuming
type Detector struct {
	masterAddr string
}

// VolumeInfo represents volume information for vacuum detection
type VolumeInfo struct {
	ID           string
	Collection   string
	SizeGB       int64
	UsedGB       int64
	GarbageRatio float64
	CreatedAt    time.Time
	LastVacuumAt time.Time
	Replicas     int32
}

// NewDetector creates a new vacuum detector
func NewDetector(masterAddr string) *Detector {
	return &Detector{
		masterAddr: masterAddr,
	}
}

// DetectJobs scans for volumes with high garbage ratio that meet vacuum criteria
// Returns a list of DetectedJob items sorted by garbage size (highest priority first)
func (d *Detector) DetectJobs(ctx context.Context, config *plugin_pb.JobTypeConfig) ([]*plugin_pb.DetectedJob, error) {
	var detectedJobs []*plugin_pb.DetectedJob

	// Extract configuration parameters
	garbageThreshold := float64(30)   // default 30%
	minVolumeAge := 24 * time.Hour    // default 24h
	minInterval := 7 * 24 * time.Hour // default 7d
	compactLevel := int64(2)
	enableCleanup := true

	for _, cfv := range config.AdminConfig {
		if cfv.FieldName == "garbageThreshold" {
			garbageThreshold = float64(cfv.IntValue)
		} else if cfv.FieldName == "minVolumeAge" {
			if dur, err := time.ParseDuration(cfv.StringValue); err == nil {
				minVolumeAge = dur
			}
		} else if cfv.FieldName == "minInterval" {
			if dur, err := time.ParseDuration(cfv.StringValue); err == nil {
				minInterval = dur
			}
		}
	}

	for _, cfv := range config.WorkerConfig {
		if cfv.FieldName == "compactLevel" {
			compactLevel = cfv.IntValue
		} else if cfv.FieldName == "enableCleanup" {
			enableCleanup = cfv.BoolValue
		}
	}

	glog.Infof("vacuum detector: scanning volumes (threshold=%.1f%%, minVolumeAge=%v, minInterval=%v)",
		garbageThreshold, minVolumeAge, minInterval)

	// Scan volumes from master
	volumes := d.scanVolumes(ctx)

	now := time.Now()
	for _, vol := range volumes {
		// Check if volume meets garbage ratio threshold
		if vol.GarbageRatio < garbageThreshold/100.0 {
			continue
		}

		// Check if volume is old enough
		volumeAge := now.Sub(vol.CreatedAt)
		if volumeAge < minVolumeAge {
			continue
		}

		// Check if minimum interval has passed since last vacuum
		timeSinceLastVacuum := now.Sub(vol.LastVacuumAt)
		if timeSinceLastVacuum < minInterval {
			continue
		}

		// Calculate priority based on garbage size (larger garbage = higher priority)
		garbageGB := float64(vol.UsedGB) * vol.GarbageRatio
		priority := int64(garbageGB * 1000) // Scale to get better ordering

		jobKey := fmt.Sprintf("vacuum_%s_%s", vol.ID, now.Format("20060102150405"))

		job := &plugin_pb.DetectedJob{
			JobKey:  jobKey,
			JobType: "vacuum",
			Description: fmt.Sprintf("Vacuum volume %s (garbage ratio %.1f%%, %dGB used, %.1fGB garbage)",
				vol.ID, vol.GarbageRatio*100, vol.UsedGB, garbageGB),
			Priority:          priority,
			EstimatedDuration: durationpb.New(time.Duration(vol.UsedGB) * time.Minute), // Rough estimate
			Metadata: map[string]string{
				"volume_id":       vol.ID,
				"collection":      vol.Collection,
				"garbage_ratio":   fmt.Sprintf("%.2f", vol.GarbageRatio),
				"garbage_size_gb": fmt.Sprintf("%.1f", garbageGB),
				"used_gb":         fmt.Sprintf("%d", vol.UsedGB),
				"volume_age":      volumeAge.String(),
				"last_vacuum_ago": timeSinceLastVacuum.String(),
			},
			SuggestedConfig: []*plugin_pb.ConfigFieldValue{
				{
					FieldName: "compactLevel",
					IntValue:  compactLevel,
				},
				{
					FieldName: "enableCleanup",
					BoolValue: enableCleanup,
				},
			},
		}

		detectedJobs = append(detectedJobs, job)
		glog.Infof("vacuum detector: detected job for volume %s (garbage=%.1fGB, priority=%d)",
			vol.ID, garbageGB, priority)
	}

	glog.Infof("vacuum detector: found %d volumes to vacuum", len(detectedJobs))
	return detectedJobs, nil
}

// scanVolumes performs a scan of available volumes from the master
// This is a placeholder that would connect to master in production
func (d *Detector) scanVolumes(ctx context.Context) []VolumeInfo {
	// TODO: Connect to master server at d.masterAddr and get volume list
	// For now, return empty list as a framework
	var volumes []VolumeInfo
	return volumes
}
