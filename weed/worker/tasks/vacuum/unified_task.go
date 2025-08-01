package vacuum

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
)

// VacuumTask implements the Task interface
type VacuumTask struct {
	*base.BaseTask
	server           string
	volumeID         uint32
	collection       string
	garbageThreshold float64
	progress         float64
}

// NewVacuumTask creates a new unified vacuum task instance
func NewVacuumTask(id string, server string, volumeID uint32, collection string) *VacuumTask {
	return &VacuumTask{
		BaseTask:         base.NewBaseTask(id, types.TaskTypeVacuum),
		server:           server,
		volumeID:         volumeID,
		collection:       collection,
		garbageThreshold: 0.3, // Default 30% threshold
	}
}

// Execute implements the UnifiedTask interface
func (t *VacuumTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	vacuumParams := params.GetVacuumParams()
	if vacuumParams == nil {
		return fmt.Errorf("vacuum parameters are required")
	}

	t.garbageThreshold = vacuumParams.GarbageThreshold

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":         t.volumeID,
		"server":            t.server,
		"collection":        t.collection,
		"garbage_threshold": t.garbageThreshold,
	}).Info("Starting vacuum task")

	// Simulate vacuum operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing volume garbage", 2 * time.Second, 15},
		{"Preparing vacuum operation", 3 * time.Second, 35},
		{"Reclaiming space", 6 * time.Second, 75},
		{"Updating metadata", 2 * time.Second, 95},
		{"Verifying volume", 1 * time.Second, 100},
	}

	for _, step := range steps {
		select {
		case <-ctx.Done():
			t.GetLogger().Warning("Vacuum task cancelled during step: " + step.name)
			return ctx.Err()
		default:
		}

		if t.IsCancelled() {
			t.GetLogger().Warning("Vacuum task cancelled by request during step: " + step.name)
			return fmt.Errorf("vacuum task cancelled")
		}

		t.GetLogger().WithFields(map[string]interface{}{
			"step":      step.name,
			"progress":  step.progress,
			"duration":  step.duration.String(),
			"volume_id": t.volumeID,
		}).Info("Executing vacuum step")

		t.progress = step.progress
		t.ReportProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.V(1).Infof("Vacuum task completed successfully: volume %d from %s", t.volumeID, t.server)
	return nil
}

// Validate implements the UnifiedTask interface
func (t *VacuumTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	vacuumParams := params.GetVacuumParams()
	if vacuumParams == nil {
		return fmt.Errorf("vacuum parameters are required")
	}

	if params.VolumeId != t.volumeID {
		return fmt.Errorf("volume ID mismatch: expected %d, got %d", t.volumeID, params.VolumeId)
	}

	if params.Server != t.server {
		return fmt.Errorf("source server mismatch: expected %s, got %s", t.server, params.Server)
	}

	if vacuumParams.GarbageThreshold < 0 || vacuumParams.GarbageThreshold > 1.0 {
		return fmt.Errorf("invalid garbage threshold: %f (must be between 0.0 and 1.0)", vacuumParams.GarbageThreshold)
	}

	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *VacuumTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Basic estimate based on simulated steps
	return 14 * time.Second // Sum of all step durations
}

// GetProgress returns current progress
func (t *VacuumTask) GetProgress() float64 {
	return t.progress
}
