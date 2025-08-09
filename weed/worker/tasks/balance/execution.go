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
	sourceServer   string
	destNode       string
	volumeID       uint32
	collection     string
	estimatedSize  uint64
	forceMove      bool
	timeoutSeconds int32
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

	// Validate sources and targets
	if len(params.Sources) == 0 {
		return fmt.Errorf("at least one source is required for balance task")
	}
	if len(params.Targets) == 0 {
		return fmt.Errorf("at least one target is required for balance task")
	}

	// Validate that source and target have volume IDs
	if params.Sources[0].VolumeId == 0 {
		return fmt.Errorf("source volume_id is required for balance task")
	}
	if params.Targets[0].VolumeId == 0 {
		return fmt.Errorf("target volume_id is required for balance task")
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
	}

	// Estimate based on volume size from sources (1 minute per GB)
	if len(params.Sources) > 0 {
		source := params.Sources[0]
		if source.EstimatedSize > 0 {
			gbSize := source.EstimatedSize / (1024 * 1024 * 1024)
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
	t.collection = params.Collection

	// Ensure sources and targets are present (should be guaranteed by validation)
	if len(params.Sources) == 0 {
		return fmt.Errorf("at least one source is required for balance task (ExecuteTyped)")
	}
	if len(params.Targets) == 0 {
		return fmt.Errorf("at least one target is required for balance task (ExecuteTyped)")
	}

	// Extract source and target information
	t.sourceServer = params.Sources[0].Node
	t.estimatedSize = params.Sources[0].EstimatedSize
	t.destNode = params.Targets[0].Node
	// Extract balance-specific parameters
	balanceParams := params.GetBalanceParams()
	if balanceParams != nil {
		t.forceMove = balanceParams.ForceMove
		t.timeoutSeconds = balanceParams.TimeoutSeconds
	}

	glog.Infof("Starting typed balance task for volume %d: %s -> %s (collection: %s, size: %d bytes)",
		t.volumeID, t.sourceServer, t.destNode, t.collection, t.estimatedSize)

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
