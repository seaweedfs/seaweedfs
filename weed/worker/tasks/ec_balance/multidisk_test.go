package ec_balance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// These cover the worker execution layer (ec_balance_task.go). The shard-balance
// policy and multi-disk placement are tested in the shared ecbalancer package.

// TestValidateRejectsSameNodeCrossDiskMove covers the data-loss trap of a
// same-node, cross-disk shard move. copyAndMountShard skips the copy when source
// and target addresses match, but VolumeEcShardsDelete is node-wide, so executing
// such a move would erase the shard. Validate must reject it, while allowing a
// cross-node move and a same-node/same-disk dedup.
func TestValidateRejectsSameNodeCrossDiskMove(t *testing.T) {
	task := NewECBalanceTask("t1", 100, "col1", nil)

	mk := func(srcNode string, srcDisk uint32, dstNode string, dstDisk uint32) *worker_pb.TaskParams {
		return &worker_pb.TaskParams{
			VolumeId: 100,
			Sources:  []*worker_pb.TaskSource{{Node: srcNode, DiskId: srcDisk, ShardIds: []uint32{3}}},
			Targets:  []*worker_pb.TaskTarget{{Node: dstNode, DiskId: dstDisk, ShardIds: []uint32{3}}},
		}
	}

	if err := task.Validate(mk("node1", 0, "node1", 3)); err == nil {
		t.Error("Validate accepted a same-node cross-disk move; it must reject it to avoid node-wide delete data loss")
	}
	if err := task.Validate(mk("node1", 0, "node2", 0)); err != nil {
		t.Errorf("Validate rejected a legitimate cross-node move: %v", err)
	}
	if err := task.Validate(mk("node1", 2, "node1", 2)); err != nil {
		t.Errorf("Validate rejected a same-node/same-disk dedup: %v", err)
	}
}

// TestIsDedupPhaseRequiresSameDisk confirms dedup classification keys on both
// node and disk, so a same-node cross-disk request is never silently routed to
// the unmount+delete path.
func TestIsDedupPhaseRequiresSameDisk(t *testing.T) {
	withParams := func(srcDisk, dstDisk uint32) *worker_pb.TaskParams {
		return &worker_pb.TaskParams{
			Sources: []*worker_pb.TaskSource{{Node: "node1", DiskId: srcDisk}},
			Targets: []*worker_pb.TaskTarget{{Node: "node1", DiskId: dstDisk}},
			TaskParams: &worker_pb.TaskParams_EcBalanceParams{
				EcBalanceParams: &worker_pb.EcBalanceTaskParams{},
			},
		}
	}
	if !isDedupPhase(withParams(2, 2)) {
		t.Error("same node and disk should be a dedup phase")
	}
	if isDedupPhase(withParams(0, 3)) {
		t.Error("same node but different disk must NOT be a dedup phase")
	}
}
