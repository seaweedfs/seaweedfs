package balance

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TypedTask implements balance operation with typed protobuf parameters
type TypedTask struct {
	*base.BaseTypedTask

	// Task state from protobuf
	sourceServer       string
	destNode           string
	volumeID           uint32
	collection         string
	estimatedSize      uint64
	placementScore     float64
	forceMove          bool
	timeoutSeconds     int32
	placementConflicts []string
}

// NewTypedTask creates a new typed balance task
func NewTypedTask() types.TypedTaskInterface {
	task := &TypedTask{
		BaseTypedTask: base.NewBaseTypedTask(types.TaskTypeBalance),
	}
	return task
}

// ValidateTyped validates the typed parameters for balance task
func (t *TypedTask) ValidateTyped(params *worker_pb.TaskParams) error {
	// Basic validation from base class
	if err := t.BaseTypedTask.ValidateTyped(params); err != nil {
		return err
	}

	// Check that we have balance-specific parameters
	balanceParams := params.GetBalanceParams()
	if balanceParams == nil {
		return fmt.Errorf("balance_params is required for balance task")
	}

	// Validate destination node
	if balanceParams.DestNode == "" {
		return fmt.Errorf("dest_node is required for balance task")
	}

	// Validate estimated size
	if balanceParams.EstimatedSize == 0 {
		return fmt.Errorf("estimated_size must be greater than 0")
	}

	// Validate timeout
	if balanceParams.TimeoutSeconds <= 0 {
		return fmt.Errorf("timeout_seconds must be greater than 0")
	}

	return nil
}

// EstimateTimeTyped estimates the time needed for balance operation based on protobuf parameters
func (t *TypedTask) EstimateTimeTyped(params *worker_pb.TaskParams) time.Duration {
	balanceParams := params.GetBalanceParams()
	if balanceParams != nil {
		// Use the timeout from parameters if specified
		if balanceParams.TimeoutSeconds > 0 {
			return time.Duration(balanceParams.TimeoutSeconds) * time.Second
		}

		// Estimate based on volume size (1 minute per GB)
		if balanceParams.EstimatedSize > 0 {
			gbSize := balanceParams.EstimatedSize / (1024 * 1024 * 1024)
			return time.Duration(gbSize) * time.Minute
		}
	}

	// Default estimation
	return 10 * time.Minute
}

// ExecuteTyped implements the balance operation with typed parameters
func (t *TypedTask) ExecuteTyped(params *worker_pb.TaskParams) error {
	// Extract basic parameters
	t.volumeID = params.VolumeId
	t.sourceServer = params.Server
	t.collection = params.Collection

	// Extract balance-specific parameters
	balanceParams := params.GetBalanceParams()
	if balanceParams != nil {
		t.destNode = balanceParams.DestNode
		t.estimatedSize = balanceParams.EstimatedSize
		t.placementScore = balanceParams.PlacementScore
		t.forceMove = balanceParams.ForceMove
		t.timeoutSeconds = balanceParams.TimeoutSeconds
		t.placementConflicts = balanceParams.PlacementConflicts
	}

	glog.Infof("Starting typed balance task for volume %d: %s -> %s (collection: %s, size: %d bytes)",
		t.volumeID, t.sourceServer, t.destNode, t.collection, t.estimatedSize)

	// Log placement information
	if t.placementScore > 0 {
		glog.V(1).Infof("Placement score: %.2f", t.placementScore)
	}
	if len(t.placementConflicts) > 0 {
		glog.V(1).Infof("Placement conflicts: %v", t.placementConflicts)
		if !t.forceMove {
			return fmt.Errorf("placement conflicts detected and force_move is false: %v", t.placementConflicts)
		}
		glog.Warningf("Proceeding with balance despite conflicts (force_move=true): %v", t.placementConflicts)
	}

	// Simulate balance operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing cluster state", 2 * time.Second, 15},
		{"Verifying destination capacity", 1 * time.Second, 25},
		{"Starting volume migration", 1 * time.Second, 35},
		{"Moving volume data", 6 * time.Second, 75},
		{"Updating cluster metadata", 2 * time.Second, 95},
		{"Verifying balance completion", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("balance task cancelled during: %s", step.name)
		}

		glog.V(1).Infof("Balance task step: %s", step.name)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("Typed balance task completed successfully for volume %d: %s -> %s",
		t.volumeID, t.sourceServer, t.destNode)
	return nil
}

// Register the typed task in the global registry
func init() {
	types.RegisterGlobalTypedTask(types.TaskTypeBalance, NewTypedTask)
	glog.V(1).Infof("Registered typed balance task")
}
