package ec_balance

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// ECBalanceTask implements a single EC shard move operation.
// The move sequence — copy+mount on dest, verify the dest registered the
// shards, then unmount+delete on the source — is shared with the shell's
// ec.balance command via weed/operation/volume_move.
type ECBalanceTask struct {
	*base.BaseTask
	volumeID       uint32
	collection     string
	grpcDialOption grpc.DialOption
	progress       uint64 // atomic; stores float64 bits via math.Float64bits
	reporting      int32  // atomic; re-entry guard to prevent recursive reportProgress calls
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

// Execute performs the EC shard move operation.
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
	// Range-check before the uint8 narrowing in Uint32ToShardIds: a malformed
	// id like 259 would otherwise alias shard 3 and copy/delete a real,
	// unrelated shard.
	if err := checkShardIdRange(source.ShardIds); err != nil {
		return err
	}
	shardIds := erasure_coding.Uint32ToShardIds(source.ShardIds)

	ecParams := params.GetEcBalanceParams()

	// Apply configured timeout to the context for all RPC operations
	if ecParams != nil && ecParams.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(ecParams.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	isDedupDelete := ecParams != nil && isDedupPhase(params)

	// Guard against a same-node, cross-disk "move". The shared mover skips the
	// copy when source and target addresses match, but the EC shard delete is
	// node-wide (it removes the shard from every disk on the node), so this
	// sequence would erase the shard after never copying it. EC shards also
	// cannot be relocated between disks of one node via these RPCs, so such a
	// move is meaningless. Reject it rather than lose data.
	if source.Node == target.Node && source.DiskId != target.DiskId {
		return fmt.Errorf("refusing same-node cross-disk EC shard move for volume %d shard(s) %v on %s (source disk %d, target disk %d): EC shard delete is node-wide and would erase the shard after a skipped copy",
			params.VolumeId, source.ShardIds, source.Node, source.DiskId, target.DiskId)
	}

	glog.Infof("EC balance: moving shard(s) %v of volume %d from %s to %s",
		source.ShardIds, params.VolumeId, source.Node, target.Node)

	mover := volume_move.NewMover(t.grpcDialOption)

	// For dedup, we only unmount+delete from source (no copy needed)
	if isDedupDelete {
		// Nothing is copied first, so the shard surviving elsewhere is the
		// only thing making this safe — and the plan asserting so is not
		// evidence. The topology can name a location that holds nothing, and
		// deleting on that basis removes the last copy. Confirm the keep node
		// has it before deleting here.
		if err := t.verifyShardsOnKeepNode(ctx, params.VolumeId, ecParams.GetDedupKeepNode(), source.ShardIds); err != nil {
			return err
		}
		t.reportProgress(25.0, "Removing duplicate EC shard")
		if err := mover.RemoveEcShards(ctx, needle.VolumeId(params.VolumeId), t.collection, sourceAddr, shardIds); err != nil {
			return fmt.Errorf("remove duplicate shard: %w", err)
		}
		t.reportProgress(100.0, "Duplicate shard removed")
		return nil
	}

	err := mover.MoveEcShards(ctx, volume_move.EcShardMove{
		VolumeId:   needle.VolumeId(params.VolumeId),
		Collection: params.Collection,
		ShardIds:   shardIds,
		Source:     sourceAddr,
		Target:     targetAddr,
		TargetDisk: target.DiskId,
	}, volume_move.EcMoveOptions{
		Progress: t.reportProgress,
	})
	if err != nil {
		return err
	}

	glog.Infof("EC balance: successfully moved shard(s) %v of volume %d from %s to %s",
		source.ShardIds, params.VolumeId, source.Node, target.Node)
	return nil
}

// verifyShardsOnKeepNode confirms the node the plan wants to keep the shard on
// actually has every shard about to be deleted elsewhere, for this collection.
func (t *ECBalanceTask) verifyShardsOnKeepNode(ctx context.Context, volumeID uint32, keepNode string, shardIDs []uint32) error {
	if keepNode == "" {
		return fmt.Errorf("refusing dedup delete of volume %d shard(s) %v: no keep node recorded, so no surviving copy can be confirmed", volumeID, shardIDs)
	}
	if err := erasure_coding.VerifyShardsOnServer(ctx, t.collection, volumeID,
		string(pb.ServerAddress(keepNode)), shardIDs, t.grpcDialOption); err != nil {
		return fmt.Errorf("refusing dedup delete: %w", err)
	}
	return nil
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
	if err := checkShardIdRange(params.Sources[0].ShardIds); err != nil {
		return fmt.Errorf("ECBalanceTask.Validate: %v", err)
	}
	if err := checkShardIdRange(params.Targets[0].ShardIds); err != nil {
		return fmt.Errorf("ECBalanceTask.Validate: %v", err)
	}
	// A same-node, cross-disk move is unsafe: the node-wide EC shard delete would
	// erase the shard after the skipped same-address copy. Such a move cannot be
	// expressed by these RPCs anyway. Dedup (same node and disk) is allowed.
	if params.Sources[0].Node == params.Targets[0].Node && params.Sources[0].DiskId != params.Targets[0].DiskId {
		return fmt.Errorf("ECBalanceTask.Validate: refusing same-node cross-disk move on %s (source disk %d, target disk %d): EC shard delete is node-wide",
			params.Sources[0].Node, params.Sources[0].DiskId, params.Targets[0].DiskId)
	}
	return nil
}

// EstimateTime estimates the time for an EC shard move
func (t *ECBalanceTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	return 30 * time.Second
}

// GetProgress returns current progress
func (t *ECBalanceTask) GetProgress() float64 {
	return math.Float64frombits(atomic.LoadUint64(&t.progress))
}

// reportProgress updates the stored progress and reports it via the callback
func (t *ECBalanceTask) reportProgress(progress float64, stage string) {
	if !atomic.CompareAndSwapInt32(&t.reporting, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&t.reporting, 0)
	atomic.StoreUint64(&t.progress, math.Float64bits(progress))
	t.ReportProgressWithStage(progress, stage)
	glog.Infof("EC balance volume %d: [%.2f] %s", t.volumeID, progress, stage)
}

// checkShardIdRange rejects shard ids that would alias a real shard when
// narrowed to the uint8 ShardId (e.g. 259 → 3).
func checkShardIdRange(ids []uint32) error {
	for _, id := range ids {
		if id >= erasure_coding.MaxShardCount {
			return fmt.Errorf("shard id %d out of range (max %d)", id, erasure_coding.MaxShardCount-1)
		}
	}
	return nil
}

// isDedupPhase checks if this is a dedup-phase task: an unmount+delete on a
// single location, encoded by detection as source==target on the same node AND
// the same disk. Comparing the disk too is essential — VolumeEcShardsDelete is
// node-wide (it removes the shard from every disk on the node), so a same-node
// but cross-disk request must NOT be treated as a benign dedup; see Validate
// and Execute, which reject it outright.
func isDedupPhase(params *worker_pb.TaskParams) bool {
	if len(params.Sources) > 0 && len(params.Targets) > 0 {
		s, t := params.Sources[0], params.Targets[0]
		return s.Node == t.Node && s.DiskId == t.DiskId
	}
	return false
}
