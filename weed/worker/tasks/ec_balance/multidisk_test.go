package ec_balance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// shardBitsFor builds an EcIndexBits bitmask from a list of shard ids.
func shardBitsFor(shardIds ...int) uint32 {
	var bits uint32
	for _, id := range shardIds {
		bits |= 1 << uint(id)
	}
	return bits
}

// multiDiskNode builds a DataNodeInfo whose single (type-keyed) DiskInfo carries
// one EC shard message per physical disk, mirroring how the master collapses
// several same-type disks into one DiskInfo while preserving each shard's
// physical DiskId.
func multiDiskNode(id, collection string, vid uint32, shardsByDisk map[uint32][]int) *master_pb.DataNodeInfo {
	var ecShardInfos []*master_pb.VolumeEcShardInformationMessage
	for diskID, shards := range shardsByDisk {
		ecShardInfos = append(ecShardInfos, &master_pb.VolumeEcShardInformationMessage{
			Id:          vid,
			Collection:  collection,
			DiskId:      diskID,
			EcIndexBits: shardBitsFor(shards...),
		})
	}
	return &master_pb.DataNodeInfo{
		Id: id,
		DiskInfos: map[string]*master_pb.DiskInfo{
			"": {Type: "", MaxVolumeCount: 100, EcShardInfos: ecShardInfos},
		},
	}
}

// TestSourceDiskAttributionAcrossDisks covers multi-disk source attribution:
// when a volume's shards are spread across several physical disks of one node,
// every emitted move must report the disk that actually holds the shard.
// buildECTopology used to keep only the first disk id per (node, volume), so
// cross-rack moves of shards living on other disks carried the wrong source
// disk — which then mis-reserves per-disk capacity downstream.
func TestSourceDiskAttributionAcrossDisks(t *testing.T) {
	// node1 (rack1) holds all 14 shards of volume 100 spread across 6 disks.
	shardsByDisk := map[uint32][]int{
		0: {0, 1, 2},
		1: {3, 4, 5},
		2: {6, 7},
		3: {8, 9},
		4: {10, 11},
		5: {12, 13},
	}
	// Reverse index: shard -> disk, for assertions.
	shardToDisk := map[int]uint32{}
	for diskID, shards := range shardsByDisk {
		for _, s := range shards {
			shardToDisk[s] = diskID
		}
	}

	topo := &master_pb.TopologyInfo{
		Id: "attr_topo",
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{
				{
					Id:            "rack1",
					DataNodeInfos: []*master_pb.DataNodeInfo{multiDiskNode("node1", "col1", 100, shardsByDisk)},
				},
				{
					Id: "rack2",
					DataNodeInfos: []*master_pb.DataNodeInfo{{
						Id: "node2",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"": {Type: "", MaxVolumeCount: 100},
						},
					}},
				},
			},
		}},
	}

	config := &Config{}
	config.Enabled = true
	nodes, racks := buildECTopology(topo, config)

	// Verify per-disk attribution survived the type-keyed collapse.
	info := nodes["node1"].ecShards[100]
	if info == nil {
		t.Fatal("node1 has no EC shard info for volume 100")
	}
	if got := info.shardBits.Count(); got != 14 {
		t.Fatalf("node1 sees %d shards for volume 100, want 14", got)
	}
	for shard, wantDisk := range shardToDisk {
		if gotDisk := ecShardDiskIDForShard(nodes["node1"], 100, shard); gotDisk != wantDisk {
			t.Errorf("shard %d resolved to disk %d, want %d", shard, gotDisk, wantDisk)
		}
	}

	// A cross-rack rebalance from the loaded rack must carry the correct source
	// disk for every moved shard.
	moves := detectCrossRackImbalance(100, "col1", nodes, racks, "", 0.01)
	if len(moves) == 0 {
		t.Fatal("expected cross-rack moves, got none")
	}
	for _, m := range moves {
		wantDisk := shardToDisk[m.shardID]
		if m.sourceDisk != wantDisk {
			t.Errorf("cross-rack move of shard %d reports source disk %d, want %d (the disk that holds it)",
				m.shardID, m.sourceDisk, wantDisk)
		}
	}
}

// TestValidateRejectsSameNodeCrossDiskMove covers the data-loss trap of a
// same-node, cross-disk shard move. copyAndMountShard skips the copy when
// source and target addresses match, but VolumeEcShardsDelete is node-wide, so
// executing such a move erases the shard. Validate must reject it, while still
// allowing a legitimate cross-node move and a same-node/same-disk dedup.
func TestValidateRejectsSameNodeCrossDiskMove(t *testing.T) {
	task := NewECBalanceTask("t1", 100, "col1", nil)

	mk := func(srcNode string, srcDisk uint32, dstNode string, dstDisk uint32) *worker_pb.TaskParams {
		return &worker_pb.TaskParams{
			VolumeId: 100,
			Sources:  []*worker_pb.TaskSource{{Node: srcNode, DiskId: srcDisk, ShardIds: []uint32{3}}},
			Targets:  []*worker_pb.TaskTarget{{Node: dstNode, DiskId: dstDisk, ShardIds: []uint32{3}}},
		}
	}

	// Unsafe: same node, different disk.
	if err := task.Validate(mk("node1", 0, "node1", 3)); err == nil {
		t.Error("Validate accepted a same-node cross-disk move; it must reject it to avoid node-wide delete data loss")
	}

	// Safe: cross-node move.
	if err := task.Validate(mk("node1", 0, "node2", 0)); err != nil {
		t.Errorf("Validate rejected a legitimate cross-node move: %v", err)
	}

	// Safe: same node and same disk (dedup unmount+delete).
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
