package ec_balance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// ECBalanceTask implements a single EC shard move operation.
// The move sequence is: copy+mount on dest → unmount on source → delete on source.
type ECBalanceTask struct {
	*base.BaseTask
	volumeID       uint32
	collection     string
	grpcDialOption grpc.DialOption
	progress       float64
}

// NewECBalanceTask creates a new EC balance task instance
func NewECBalanceTask(id string, volumeID uint32, collection string, grpcDialOption grpc.DialOption) *ECBalanceTask {
	return &ECBalanceTask{
		BaseTask:       base.NewBaseTask(id, types.TaskTypeECBalance),
		volumeID:       volumeID,
		collection:     collection,
		grpcDialOption: grpcDialOption,
	}
}

// Execute performs the EC shard move operation using the same RPC sequence
// as the shell ec.balance command's moveMountedShardToEcNode function.
func (t *ECBalanceTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	if len(params.Sources) == 0 || len(params.Targets) == 0 {
		return fmt.Errorf("sources and targets are required for EC shard move")
	}
	if len(params.Sources) > 1 || len(params.Targets) > 1 {
		return fmt.Errorf("batch EC shard moves not supported: got %d sources and %d targets, expected 1 each", len(params.Sources), len(params.Targets))
	}

	source := params.Sources[0]
	target := params.Targets[0]

	if len(source.ShardIds) == 0 || len(target.ShardIds) == 0 {
		return fmt.Errorf("shard IDs are required in sources and targets")
	}

	sourceAddr := pb.ServerAddress(source.Node)
	targetAddr := pb.ServerAddress(target.Node)

	ecParams := params.GetEcBalanceParams()

	// Apply configured timeout to the context for all RPC operations
	if ecParams != nil && ecParams.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(ecParams.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	isDedupDelete := ecParams != nil && isDedupPhase(params)

	glog.Infof("EC balance: moving shard(s) %v of volume %d from %s to %s",
		source.ShardIds, params.VolumeId, source.Node, target.Node)

	// For dedup, we only unmount+delete from source (no copy needed)
	if isDedupDelete {
		return t.executeDedupDelete(ctx, params.VolumeId, sourceAddr, source.ShardIds)
	}

	// Step 1: Copy shard to destination and mount
	t.reportProgress(10.0, "Copying EC shard to destination")
	if err := t.copyAndMountShard(ctx, params.VolumeId, sourceAddr, targetAddr, source.ShardIds, target.DiskId); err != nil {
		return fmt.Errorf("copy and mount shard: %v", err)
	}

	// Step 2: Unmount shard on source
	t.reportProgress(50.0, "Unmounting EC shard from source")
	if err := t.unmountShard(ctx, params.VolumeId, sourceAddr, source.ShardIds); err != nil {
		return fmt.Errorf("unmount shard on source: %v", err)
	}

	// Step 3: Delete shard from source
	t.reportProgress(75.0, "Deleting EC shard from source")
	if err := t.deleteShard(ctx, params.VolumeId, params.Collection, sourceAddr, source.ShardIds); err != nil {
		return fmt.Errorf("delete shard on source: %v", err)
	}

	t.reportProgress(100.0, "EC shard move complete")
	glog.Infof("EC balance: successfully moved shard(s) %v of volume %d from %s to %s",
		source.ShardIds, params.VolumeId, source.Node, target.Node)
	return nil
}

// executeDedupDelete removes a duplicate shard without copying
func (t *ECBalanceTask) executeDedupDelete(ctx context.Context, volumeID uint32, sourceAddr pb.ServerAddress, shardIDs []uint32) error {
	t.reportProgress(25.0, "Unmounting duplicate EC shard")
	if err := t.unmountShard(ctx, volumeID, sourceAddr, shardIDs); err != nil {
		return fmt.Errorf("unmount duplicate shard: %v", err)
	}

	t.reportProgress(75.0, "Deleting duplicate EC shard")
	if err := t.deleteShard(ctx, volumeID, t.collection, sourceAddr, shardIDs); err != nil {
		return fmt.Errorf("delete duplicate shard: %v", err)
	}

	t.reportProgress(100.0, "Duplicate shard removed")
	return nil
}

// copyAndMountShard copies EC shard from source to destination and mounts it
func (t *ECBalanceTask) copyAndMountShard(ctx context.Context, volumeID uint32, sourceAddr, targetAddr pb.ServerAddress, shardIDs []uint32, destDiskID uint32) error {
	return operation.WithVolumeServerClient(false, targetAddr, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			// Copy shard data (if source != target)
			if sourceAddr != targetAddr {
				_, err := client.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
					VolumeId:       volumeID,
					Collection:     t.collection,
					ShardIds:       shardIDs,
					CopyEcxFile:    true,
					CopyEcjFile:    true,
					CopyVifFile:    true,
					SourceDataNode: string(sourceAddr),
					DiskId:         destDiskID,
				})
				if err != nil {
					return fmt.Errorf("copy shard(s) %v from %s to %s: %v", shardIDs, sourceAddr, targetAddr, err)
				}
			}

			// Mount the shard on destination
			_, err := client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
				VolumeId:   volumeID,
				Collection: t.collection,
				ShardIds:   shardIDs,
			})
			if err != nil {
				return fmt.Errorf("mount shard(s) %v on %s: %v", shardIDs, targetAddr, err)
			}

			return nil
		})
}

// unmountShard unmounts EC shards from a server
func (t *ECBalanceTask) unmountShard(ctx context.Context, volumeID uint32, addr pb.ServerAddress, shardIDs []uint32) error {
	return operation.WithVolumeServerClient(false, addr, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
				VolumeId: volumeID,
				ShardIds: shardIDs,
			})
			return err
		})
}

// deleteShard deletes EC shards from a server
func (t *ECBalanceTask) deleteShard(ctx context.Context, volumeID uint32, collection string, addr pb.ServerAddress, shardIDs []uint32) error {
	return operation.WithVolumeServerClient(false, addr, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
				VolumeId:   volumeID,
				Collection: collection,
				ShardIds:   shardIDs,
			})
			return err
		})
}

// Validate validates the task parameters.
// ECBalanceTask handles exactly one source→target shard move per execution.
func (t *ECBalanceTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("ECBalanceTask.Validate: TaskParams are required")
	}
	if len(params.Sources) != 1 {
		return fmt.Errorf("ECBalanceTask.Validate: expected exactly 1 source, got %d", len(params.Sources))
	}
	if len(params.Targets) != 1 {
		return fmt.Errorf("ECBalanceTask.Validate: expected exactly 1 target, got %d", len(params.Targets))
	}
	if len(params.Sources[0].ShardIds) == 0 {
		return fmt.Errorf("ECBalanceTask.Validate: Sources[0].ShardIds is empty")
	}
	if len(params.Targets[0].ShardIds) == 0 {
		return fmt.Errorf("ECBalanceTask.Validate: Targets[0].ShardIds is empty")
	}
	return nil
}

// EstimateTime estimates the time for an EC shard move
func (t *ECBalanceTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	return 30 * time.Second
}

// GetProgress returns current progress
func (t *ECBalanceTask) GetProgress() float64 {
	return t.progress
}

// reportProgress updates the stored progress and reports it via the callback
func (t *ECBalanceTask) reportProgress(progress float64, stage string) {
	t.progress = progress
	t.ReportProgressWithStage(progress, stage)
	glog.Infof("EC balance volume %d: [%.2f] %s", t.volumeID, progress, stage)
}

// isDedupPhase checks if this is a dedup-phase task (source and target are the same node)
func isDedupPhase(params *worker_pb.TaskParams) bool {
	if len(params.Sources) > 0 && len(params.Targets) > 0 {
		return params.Sources[0].Node == params.Targets[0].Node
	}
	return false
}
