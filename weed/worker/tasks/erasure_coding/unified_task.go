package erasure_coding

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
)

// UnifiedErasureCodingTask implements the new UnifiedTask interface
type UnifiedErasureCodingTask struct {
	*base.UnifiedBaseTask
	server     string
	volumeID   uint32
	collection string
	workDir    string
	progress   float64

	// EC parameters
	dataShards   int32
	parityShards int32
	destinations []*worker_pb.ECDestination
}

// NewUnifiedErasureCodingTask creates a new unified EC task instance
func NewUnifiedErasureCodingTask(id string, server string, volumeID uint32, collection string) *UnifiedErasureCodingTask {
	return &UnifiedErasureCodingTask{
		UnifiedBaseTask: base.NewUnifiedBaseTask(id, types.TaskTypeErasureCoding),
		server:          server,
		volumeID:        volumeID,
		collection:      collection,
		dataShards:      10, // Default values
		parityShards:    4,  // Default values
	}
}

// Execute implements the UnifiedTask interface
func (t *UnifiedErasureCodingTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		return fmt.Errorf("erasure coding parameters are required")
	}

	t.dataShards = ecParams.DataShards
	t.parityShards = ecParams.ParityShards
	t.workDir = ecParams.WorkingDir
	t.destinations = ecParams.Destinations

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":     t.volumeID,
		"server":        t.server,
		"collection":    t.collection,
		"data_shards":   t.dataShards,
		"parity_shards": t.parityShards,
		"destinations":  len(t.destinations),
	}).Info("Starting erasure coding task")

	// Simulate EC operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Preparing volume for EC", 2 * time.Second, 10},
		{"Copying volume data", 4 * time.Second, 30},
		{"Generating EC shards", 5 * time.Second, 50},
		{"Distributing shards", 6 * time.Second, 80},
		{"Verifying distribution", 2 * time.Second, 90},
		{"Updating metadata", 1 * time.Second, 100},
	}

	for _, step := range steps {
		select {
		case <-ctx.Done():
			t.GetLogger().Warning("EC task cancelled during step: " + step.name)
			return ctx.Err()
		default:
		}

		if t.IsCancelled() {
			t.GetLogger().Warning("EC task cancelled by request during step: " + step.name)
			return fmt.Errorf("EC task cancelled")
		}

		t.GetLogger().WithFields(map[string]interface{}{
			"step":      step.name,
			"progress":  step.progress,
			"duration":  step.duration.String(),
			"volume_id": t.volumeID,
		}).Info("Executing EC step")

		t.progress = step.progress
		t.ReportProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.V(1).Infof("EC task completed successfully: volume %d from %s", t.volumeID, t.server)
	return nil
}

// Validate implements the UnifiedTask interface
func (t *UnifiedErasureCodingTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	ecParams := params.GetErasureCodingParams()
	if ecParams == nil {
		return fmt.Errorf("erasure coding parameters are required")
	}

	if params.VolumeId != t.volumeID {
		return fmt.Errorf("volume ID mismatch: expected %d, got %d", t.volumeID, params.VolumeId)
	}

	if params.Server != t.server {
		return fmt.Errorf("source server mismatch: expected %s, got %s", t.server, params.Server)
	}

	if ecParams.DataShards < 1 {
		return fmt.Errorf("invalid data shards: %d (must be >= 1)", ecParams.DataShards)
	}

	if ecParams.ParityShards < 1 {
		return fmt.Errorf("invalid parity shards: %d (must be >= 1)", ecParams.ParityShards)
	}

	if len(ecParams.Destinations) < int(ecParams.DataShards+ecParams.ParityShards) {
		return fmt.Errorf("insufficient destinations: got %d, need %d", len(ecParams.Destinations), ecParams.DataShards+ecParams.ParityShards)
	}

	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *UnifiedErasureCodingTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Basic estimate based on simulated steps
	return 20 * time.Second // Sum of all step durations
}

// GetProgress returns current progress
func (t *UnifiedErasureCodingTask) GetProgress() float64 {
	return t.progress
}
