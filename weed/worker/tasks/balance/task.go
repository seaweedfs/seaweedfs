package balance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements balance operation to redistribute volumes across volume servers
type Task struct {
	*tasks.BaseTask
	server     string
	volumeID   uint32
	collection string

	// Task parameters for accessing planned destinations
	taskParams types.TaskParams
}

// NewTask creates a new balance task instance
func NewTask(server string, volumeID uint32, collection string) *Task {
	task := &Task{
		BaseTask:   tasks.NewBaseTask(types.TaskTypeBalance),
		server:     server,
		volumeID:   volumeID,
		collection: collection,
	}
	return task
}

// Execute executes the balance task
func (t *Task) Execute(params types.TaskParams) error {
	// Use BaseTask.ExecuteTask to handle logging initialization
	return t.ExecuteTask(context.Background(), params, t.executeImpl)
}

// executeImpl is the actual balance implementation
func (t *Task) executeImpl(ctx context.Context, params types.TaskParams) error {
	// Store task parameters for accessing planned destinations
	t.taskParams = params

	// Get planned destination
	destNode := t.getPlannedDestination()
	if destNode != "" {
		t.LogWithFields("INFO", "Starting balance task with planned destination", map[string]interface{}{
			"volume_id":   t.volumeID,
			"source":      t.server,
			"destination": destNode,
			"collection":  t.collection,
		})
	} else {
		t.LogWithFields("INFO", "Starting balance task without specific destination", map[string]interface{}{
			"volume_id":  t.volumeID,
			"server":     t.server,
			"collection": t.collection,
		})
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
			t.LogWarning("Balance task cancelled during step: %s", step.name)
			return ctx.Err()
		default:
		}

		if t.IsCancelled() {
			t.LogWarning("Balance task cancelled by request during step: %s", step.name)
			return fmt.Errorf("balance task cancelled")
		}

		t.LogWithFields("INFO", "Executing balance step", map[string]interface{}{
			"step":      step.name,
			"progress":  step.progress,
			"duration":  step.duration.String(),
			"volume_id": t.volumeID,
		})
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	t.LogWithFields("INFO", "Balance task completed successfully", map[string]interface{}{
		"volume_id":      t.volumeID,
		"server":         t.server,
		"collection":     t.collection,
		"final_progress": 100.0,
	})
	return nil
}

// Validate validates the task parameters
func (t *Task) Validate(params types.TaskParams) error {
	if params.VolumeID == 0 {
		return fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return fmt.Errorf("server is required")
	}
	return nil
}

// getPlannedDestination extracts the planned destination node from task parameters
func (t *Task) getPlannedDestination() string {
	if t.taskParams.TypedParams != nil {
		if balanceParams := t.taskParams.TypedParams.GetBalanceParams(); balanceParams != nil {
			if balanceParams.DestNode != "" {
				glog.V(2).Infof("Found planned destination for volume %d: %s", t.volumeID, balanceParams.DestNode)
				return balanceParams.DestNode
			}
		}
	}
	return ""
}

// EstimateTime estimates the time needed for the task
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
	// Base time for balance operation
	baseTime := 35 * time.Second

	// Could adjust based on volume size or cluster state
	return baseTime
}
