package balance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// BalanceTask implements the Task interface
type BalanceTask struct {
	*base.BaseTask
	server         string
	volumeID       uint32
	collection     string
	progress       float64
	grpcDialOption grpc.DialOption
}

// NewBalanceTask creates a new balance task instance
func NewBalanceTask(id string, server string, volumeID uint32, collection string, grpcDialOption grpc.DialOption) *BalanceTask {
	return &BalanceTask{
		BaseTask:       base.NewBaseTask(id, types.TaskTypeBalance),
		server:         server,
		volumeID:       volumeID,
		collection:     collection,
		grpcDialOption: grpcDialOption,
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

	// Get source and destination from unified arrays
	if len(params.Sources) == 0 {
		return fmt.Errorf("source is required for balance task")
	}
	if len(params.Targets) == 0 {
		return fmt.Errorf("target is required for balance task")
	}

	sourceNode := params.Sources[0].Node
	destNode := params.Targets[0].Node

	if sourceNode == "" {
		return fmt.Errorf("source node is required for balance task")
	}
	if destNode == "" {
		return fmt.Errorf("destination node is required for balance task")
	}

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":   t.volumeID,
		"source":      sourceNode,
		"destination": destNode,
		"collection":  t.collection,
	}).Info("Starting balance task - moving volume")

	// The move sequence — freeze the source, copy, tail, verify the target
	// matches the source before the destructive delete — is shared with the
	// shell's volume.move/volume.balance commands.
	mover := volume_move.NewMover(t.grpcDialOption)
	err := mover.LiveMoveVolume(ctx, needle.VolumeId(t.volumeID), pb.ServerAddress(sourceNode), pb.ServerAddress(destNode), volume_move.VolumeMoveOptions{
		IdleTimeout:     60 * time.Second,
		IoBytePerSecond: balanceParams.IoBytePerSecond,
		Progress: func(percent float64, stage string) {
			t.ReportProgress(percent)
			t.GetLogger().Info("move stage: %s", stage)
		},
	})
	if err != nil {
		return fmt.Errorf("move volume %d from %s to %s: %w", t.volumeID, sourceNode, destNode, err)
	}

	glog.Infof("Balance task completed successfully: volume %d moved from %s to %s",
		t.volumeID, sourceNode, destNode)
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

	// Validate that at least one source matches our server
	found := false
	for _, source := range params.Sources {
		if source.Node == t.server {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("no source matches expected server %s", t.server)
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
