package balance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
)

// BalanceTask implements the Task interface
type BalanceTask struct {
	*base.BaseTask
	server     string
	volumeID   uint32
	collection string
	progress   float64
}

// NewBalanceTask creates a new balance task instance
func NewBalanceTask(id string, server string, volumeID uint32, collection string) *BalanceTask {
	return &BalanceTask{
		BaseTask:   base.NewBaseTask(id, types.TaskTypeBalance),
		server:     server,
		volumeID:   volumeID,
		collection: collection,
	}
}

// Execute implements the Task interface
func (t *BalanceTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	balanceParams := params.GetBalanceParams()
	if balanceParams == nil {
		return fmt.Errorf("balance parameters are required")
	}

	// Get planned destination
	destNode := balanceParams.DestNode
	if destNode != "" {
		t.GetLogger().WithFields(map[string]interface{}{
			"volume_id":   t.volumeID,
			"source":      t.server,
			"destination": destNode,
			"collection":  t.collection,
		}).Info("Starting balance task with planned destination")
	} else {
		t.GetLogger().WithFields(map[string]interface{}{
			"volume_id":  t.volumeID,
			"server":     t.server,
			"collection": t.collection,
		}).Info("Starting balance task without specific destination")
	}

	// Simulate balance operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing cluster state", 2 * time.Second, 15},
		{"Identifying optimal placement", 3 * time.Second, 35},
		{"Moving volume data", 6 * time.Second, 75},
		{"Updating cluster metadata", 2 * time.Second, 95},
		{"Verifying balance", 1 * time.Second, 100},
	}

	for _, step := range steps {
		select {
		case <-ctx.Done():
			t.GetLogger().Warning("Balance task cancelled during step: " + step.name)
			return ctx.Err()
		default:
		}

		if t.IsCancelled() {
			t.GetLogger().Warning("Balance task cancelled by request during step: " + step.name)
			return fmt.Errorf("balance task cancelled")
		}

		t.GetLogger().WithFields(map[string]interface{}{
			"step":      step.name,
			"progress":  step.progress,
			"duration":  step.duration.String(),
			"volume_id": t.volumeID,
		}).Info("Executing balance step")

		t.progress = step.progress
		t.ReportProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.V(1).Infof("Balance task completed successfully: volume %d from %s", t.volumeID, t.server)
	return nil
}

// Validate implements the UnifiedTask interface
func (t *BalanceTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	balanceParams := params.GetBalanceParams()
	if balanceParams == nil {
		return fmt.Errorf("balance parameters are required")
	}

	if params.VolumeId != t.volumeID {
		return fmt.Errorf("volume ID mismatch: expected %d, got %d", t.volumeID, params.VolumeId)
	}

	if params.Server != t.server {
		return fmt.Errorf("source server mismatch: expected %s, got %s", t.server, params.Server)
	}

	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *BalanceTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Basic estimate based on simulated steps
	return 14 * time.Second // Sum of all step durations
}

// GetProgress returns current progress
func (t *BalanceTask) GetProgress() float64 {
	return t.progress
}
