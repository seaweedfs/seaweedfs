package ec_balance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
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

// TestPickBestDiskOnNode covers the ported diversity-aware disk selection:
// it skips full disks, spreads a volume's shards across disks, and applies
// data/parity anti-affinity.
func TestPickBestDiskOnNode(t *testing.T) {
	const vid = uint32(100)
	const ds = erasure_coding.DataShardsCount // data shards are ids [0, ds)

	t.Run("skips disks with no free slots", func(t *testing.T) {
		node := &ecNodeInfo{
			disks: map[uint32]*ecDiskInfo{
				1: {diskID: 1, freeSlots: 0},  // full
				2: {diskID: 2, freeSlots: 10}, // available
			},
		}
		if got := pickBestDiskOnNode(node, vid, "", 0, ds); got != 2 {
			t.Errorf("got disk %d, want 2 (the only disk with free slots)", got)
		}
	})

	t.Run("spreads a volume's shards across disks", func(t *testing.T) {
		node := &ecNodeInfo{
			ecShards: map[uint32]*ecVolumeInfo{
				vid: {diskShardBits: map[uint32]erasure_coding.ShardBits{
					1: shardBitsOf(0), // disk 1 already holds a shard of this volume
				}},
			},
			disks: map[uint32]*ecDiskInfo{
				1: {diskID: 1, freeSlots: 10, ecShardCount: 1},
				2: {diskID: 2, freeSlots: 10, ecShardCount: 0},
			},
		}
		if got := pickBestDiskOnNode(node, vid, "", 5, ds); got != 2 {
			t.Errorf("got disk %d, want 2 (disk 1 already holds this volume)", got)
		}
	})

	t.Run("data shard avoids a disk holding parity shards", func(t *testing.T) {
		node := &ecNodeInfo{
			ecShards: map[uint32]*ecVolumeInfo{
				vid: {diskShardBits: map[uint32]erasure_coding.ShardBits{
					1: shardBitsOf(ds), // disk 1 holds a parity shard
				}},
			},
			disks: map[uint32]*ecDiskInfo{
				1: {diskID: 1, freeSlots: 10, ecShardCount: 1},
				2: {diskID: 2, freeSlots: 10, ecShardCount: 0},
			},
		}
		// Placing a data shard (id 0) should avoid the parity-bearing disk 1.
		if got := pickBestDiskOnNode(node, vid, "", 0, ds); got != 2 {
			t.Errorf("got disk %d, want 2 (anti-affinity: data avoids parity disk)", got)
		}
	})

	t.Run("anti-affinity follows the volume's own EC ratio", func(t *testing.T) {
		// A 6+3 volume: shard ids >= 6 are parity. Disk 1 holds shard 7 (parity
		// at 6+3) and is otherwise emptier; disk 2 holds shard 2 (data) plus an
		// unrelated shard. Placing a data shard must avoid disk 1's parity.
		node := &ecNodeInfo{
			ecShards: map[uint32]*ecVolumeInfo{
				vid: {
					dataShards: 6,
					diskShardBits: map[uint32]erasure_coding.ShardBits{
						1: shardBitsOf(7), // parity at 6+3
						2: shardBitsOf(2), // data
					},
				},
			},
			disks: map[uint32]*ecDiskInfo{
				1: {diskID: 1, freeSlots: 10, ecShardCount: 1},
				2: {diskID: 2, freeSlots: 10, ecShardCount: 2},
			},
		}
		// With the volume's real ratio (6) a data shard avoids disk 1's parity.
		if got := pickBestDiskOnNode(node, vid, "", 1, node.ecShards[vid].dataShardCount()); got != 2 {
			t.Errorf("with ratio 6: got disk %d, want 2 (data shard avoids parity-bearing disk 1)", got)
		}
		// With the wrong hardcoded boundary (10) shard 7 looks like data, the
		// anti-affinity penalty vanishes, and the emptier disk 1 wins — the bug.
		if got := pickBestDiskOnNode(node, vid, "", 1, erasure_coding.DataShardsCount); got != 1 {
			t.Errorf("with boundary 10: got disk %d, want 1 (demonstrates why the constant is wrong)", got)
		}
	})

	t.Run("only matching disk type when type is set", func(t *testing.T) {
		node := &ecNodeInfo{
			disks: map[uint32]*ecDiskInfo{
				1: {diskID: 1, diskType: "ssd", freeSlots: 10},
				2: {diskID: 2, diskType: "hdd", freeSlots: 10},
			},
		}
		if got := pickBestDiskOnNode(node, vid, "hdd", 0, ds); got != 2 {
			t.Errorf("got disk %d, want 2 (only hdd matches)", got)
		}
	})
}

// TestCrossRackMoveSpreadsAcrossDestinationDisks verifies the end-to-end effect:
// when several shards of one volume move into a multi-disk destination node, the
// planned moves spread across the destination's disks instead of all landing on
// one disk.
func TestCrossRackMoveSpreadsAcrossDestinationDisks(t *testing.T) {
	// Source node (rack1) holds all 14 shards of volume 100 on a single disk.
	src := &master_pb.DataNodeInfo{
		Id: "node1",
		DiskInfos: map[string]*master_pb.DiskInfo{
			"": {
				Type:           "",
				MaxVolumeCount: 100,
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
					{Id: 100, Collection: "col1", DiskId: 0, EcIndexBits: shardBitsFor(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)},
				},
			},
		},
	}
	// Destination node (rack2) is EC-empty but has six physical disks (ids 1..6),
	// discovered from regular volumes, with ample free capacity.
	var destVolumes []*master_pb.VolumeInformationMessage
	for diskID := uint32(1); diskID <= 6; diskID++ {
		destVolumes = append(destVolumes, &master_pb.VolumeInformationMessage{Id: diskID, DiskId: diskID})
	}
	dst := &master_pb.DataNodeInfo{
		Id: "node2",
		DiskInfos: map[string]*master_pb.DiskInfo{
			"": {Type: "", MaxVolumeCount: 100, VolumeCount: 6, VolumeInfos: destVolumes},
		},
	}
	topo := &master_pb.TopologyInfo{
		Id: "spread_topo",
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{
				{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{src}},
				{Id: "rack2", DataNodeInfos: []*master_pb.DataNodeInfo{dst}},
			},
		}},
	}

	config := &Config{}
	config.Enabled = true
	nodes, racks := buildECTopology(topo, config)

	moves := detectCrossRackImbalance(100, "col1", nodes, racks, "", 0.01)
	if len(moves) != 7 { // 14 shards, 2 racks -> max 7 per rack -> move 7
		t.Fatalf("expected 7 cross-rack moves, got %d", len(moves))
	}

	distinct := map[uint32]bool{}
	for _, m := range moves {
		if m.target.nodeID != "node2" {
			t.Errorf("move targets %s, want node2", m.target.nodeID)
		}
		distinct[m.targetDisk] = true
	}
	// Seven shards across six disks must use all six (six distinct, one doubles up),
	// not pile onto a single disk.
	if len(distinct) != 6 {
		t.Errorf("moves used %d distinct destination disks, want 6 (spread across all disks): %v", len(distinct), distinct)
	}
}

// shardBitsOf builds a ShardBits from shard ids.
func shardBitsOf(ids ...int) erasure_coding.ShardBits {
	var b erasure_coding.ShardBits
	for _, id := range ids {
		b = b.Set(erasure_coding.ShardId(id))
	}
	return b
}
