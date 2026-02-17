package erasure_coding

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Detector scans for volumes that are candidates for erasure coding
type Detector struct {
	masterAddr string
}

// NewDetector creates a new erasure coding detector
func NewDetector(masterAddr string) *Detector {
	return &Detector{
		masterAddr: masterAddr,
	}
}

// DetectJobs scans the master for volumes meeting EC criteria
// Returns a list of DetectedJob items sorted by priority (volume size)
func (d *Detector) DetectJobs(ctx context.Context, config *plugin_pb.JobTypeConfig) ([]*plugin_pb.DetectedJob, error) {
	var detectedJobs []*plugin_pb.DetectedJob

	// Get destination nodes from config
	var destinationNodes []string
	var ecM, ecN, stripeSize int64

	for _, cfv := range config.AdminConfig {
		if cfv.FieldName == "destination_data_nodes" {
			if cfv.StringValue != "" {
				// Parse comma-separated nodes
				destinationNodes = append(destinationNodes, cfv.StringValue)
			}
		}
	}

	for _, cfv := range config.WorkerConfig {
		if cfv.FieldName == "ec_m" {
			ecM = cfv.IntValue
		} else if cfv.FieldName == "ec_n" {
			ecN = cfv.IntValue
		} else if cfv.FieldName == "stripe_size" {
			stripeSize = cfv.IntValue
		}
	}

	if len(destinationNodes) == 0 {
		glog.Warningf("erasure_coding detector: no destination nodes configured")
		return detectedJobs, nil
	}

	if ecM == 0 {
		ecM = 10
	}
	if ecN == 0 {
		ecN = 4
	}
	if stripeSize == 0 {
		stripeSize = 65536
	}

	glog.Infof("erasure_coding detector: scanning for volumes (ecM=%d, ecN=%d)", ecM, ecN)

	// Scan master for volumes
	// This would typically connect to the master server to get volume information
	// For now, we create a framework that can be extended with actual master connectivity
	volumes := d.scanVolumes(ctx)

	for _, vol := range volumes {
		// Check if volume is a candidate for EC
		// Criteria: less than 90% full, not already encoded
		if vol.fullnessPercent < 90 && !vol.isEncoded {
			jobKey := fmt.Sprintf("ec_%s_%s", vol.id, time.Now().Format("20060102"))

			// Priority is based on volume size (larger volumes get higher priority)
			priority := int64(vol.sizeGB)

			job := &plugin_pb.DetectedJob{
				JobKey:            jobKey,
				JobType:           "erasure_coding",
				Description:       fmt.Sprintf("Encode volume %s (%.1f%% full, %dGB)", vol.id, vol.fullnessPercent, vol.sizeGB),
				Priority:          priority,
				EstimatedDuration: durationpb.New(time.Duration(vol.sizeGB) * time.Hour), // Rough estimate
				Metadata: map[string]string{
					"volume_id":    vol.id,
					"collection":   vol.collection,
					"fullness_pct": fmt.Sprintf("%.1f", vol.fullnessPercent),
					"size_gb":      fmt.Sprintf("%d", vol.sizeGB),
					"data_nodes":   fmt.Sprintf("%d", len(destinationNodes)),
				},
				SuggestedConfig: []*plugin_pb.ConfigFieldValue{
					{
						FieldName: "ec_m",
						IntValue:  ecM,
					},
					{
						FieldName: "ec_n",
						IntValue:  ecN,
					},
					{
						FieldName: "stripe_size",
						IntValue:  stripeSize,
					},
				},
			}

			detectedJobs = append(detectedJobs, job)
		}
	}

	glog.Infof("erasure_coding detector: found %d candidate volumes", len(detectedJobs))
	return detectedJobs, nil
}

// Volume represents a volume on the cluster
type volume struct {
	id               string
	collection       string
	sizeGB           int64
	usedGB           int64
	fullnessPercent  float64
	isEncoded        bool
	replicaPlacement int32
	dataNodes        []string
}

// scanVolumes performs a scan of available volumes
// This is a placeholder implementation that would connect to master in production
func (d *Detector) scanVolumes(ctx context.Context) []volume {
	// TODO: Connect to master server at d.masterAddr and get volume list
	// For now, return empty list as a framework
	var volumes []volume
	return volumes
}
