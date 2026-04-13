package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestCollectCollectionStatsECUnevenShards verifies that EC shards spread
// across multiple disks are aggregated correctly, including the case where
// the last data shard is smaller than the rest.
func TestCollectCollectionStatsECUnevenShards(t *testing.T) {
	// Standard 10+4 EC layout. Shards 0..9 are data, 10..13 are parity.
	// Data shards 0..8 are 1000 bytes; data shard 9 is 500 bytes (uneven tail).
	// Parity shards 10..13 are 1000 bytes each.
	//
	// Physical (raw) = 9*1000 + 500 + 4*1000 = 13500
	// Logical (data only) = 9*1000 + 500 = 9500

	nodeA := &master_pb.DataNodeInfo{
		DiskInfos: map[string]*master_pb.DiskInfo{
			"disk1": {
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
					{
						Id:         42,
						Collection: "bucket-a",
						// Shards 0..6 held here.
						EcIndexBits: (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 6),
						ShardSizes:  []int64{1000, 1000, 1000, 1000, 1000, 1000, 1000},
					},
				},
			},
		},
	}
	nodeB := &master_pb.DataNodeInfo{
		DiskInfos: map[string]*master_pb.DiskInfo{
			"disk1": {
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
					{
						Id:         42,
						Collection: "bucket-a",
						// Shards 7..13 held here. Shard 9 (data) is the short tail.
						EcIndexBits: (1 << 7) | (1 << 8) | (1 << 9) | (1 << 10) | (1 << 11) | (1 << 12) | (1 << 13),
						ShardSizes:  []int64{1000, 1000, 500, 1000, 1000, 1000, 1000},
					},
				},
			},
		},
	}

	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				RackInfos: []*master_pb.RackInfo{
					{
						DataNodeInfos: []*master_pb.DataNodeInfo{nodeA, nodeB},
					},
				},
			},
		},
	}

	stats := collectCollectionStats(topo)

	got, ok := stats["bucket-a"]
	if !ok {
		t.Fatalf("expected collection bucket-a in stats, got: %v", stats)
	}

	const wantPhysical int64 = 13500
	const wantLogical int64 = 9500

	if got.PhysicalSize != wantPhysical {
		t.Errorf("PhysicalSize: got %d, want %d", got.PhysicalSize, wantPhysical)
	}
	if got.LogicalSize != wantLogical {
		t.Errorf("LogicalSize: got %d, want %d", got.LogicalSize, wantLogical)
	}
}

// TestCollectCollectionStatsECEmptyCollection verifies that EC shards with
// an empty collection name are bucketed under "default".
func TestCollectCollectionStatsECEmptyCollection(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				RackInfos: []*master_pb.RackInfo{
					{
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								DiskInfos: map[string]*master_pb.DiskInfo{
									"disk1": {
										EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
											{
												Id:          1,
												Collection:  "",
												EcIndexBits: (1 << 0) | (1 << 10),
												ShardSizes:  []int64{2000, 2000},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	stats := collectCollectionStats(topo)

	got, ok := stats["default"]
	if !ok {
		t.Fatalf("expected collection default in stats, got: %v", stats)
	}
	if got.PhysicalSize != 4000 {
		t.Errorf("PhysicalSize: got %d, want 4000", got.PhysicalSize)
	}
	// Only shard 0 is a data shard; shard 10 is parity.
	if got.LogicalSize != 2000 {
		t.Errorf("LogicalSize: got %d, want 2000", got.LogicalSize)
	}
}

// TestCollectCollectionStatsECFileCountDedup verifies that EC volume file
// counts are applied once per volume id even though every node holding shards
// reports the same (file_count, delete_count) — summing them across nodes
// would otherwise multiply the object count by the number of holders.
func TestCollectCollectionStatsECFileCountDedup(t *testing.T) {
	// Same volume (id=7) reported by three nodes, each with different shards.
	// file_count=100, delete_count=10 → expected FileCount = 90 (applied once).
	makeNode := func(bits uint32, sizes []int64) *master_pb.DataNodeInfo {
		return &master_pb.DataNodeInfo{
			DiskInfos: map[string]*master_pb.DiskInfo{
				"disk1": {
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{
							Id:          7,
							Collection:  "bucket-a",
							EcIndexBits: bits,
							ShardSizes:  sizes,
							FileCount:   100,
							DeleteCount: 10,
						},
					},
				},
			},
		}
	}

	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				RackInfos: []*master_pb.RackInfo{
					{
						DataNodeInfos: []*master_pb.DataNodeInfo{
							makeNode((1<<0)|(1<<1)|(1<<2)|(1<<3), []int64{1000, 1000, 1000, 1000}),
							makeNode((1<<4)|(1<<5)|(1<<6)|(1<<7), []int64{1000, 1000, 1000, 1000}),
							makeNode((1<<8)|(1<<9)|(1<<10)|(1<<11)|(1<<12)|(1<<13), []int64{1000, 1000, 1000, 1000, 1000, 1000}),
						},
					},
				},
			},
		},
	}

	stats := collectCollectionStats(topo)

	got, ok := stats["bucket-a"]
	if !ok {
		t.Fatalf("expected collection bucket-a in stats, got: %v", stats)
	}
	if got.FileCount != 90 {
		t.Errorf("FileCount: got %d, want 90 (dedup across nodes)", got.FileCount)
	}
	// Sanity: 14 shards × 1000 bytes physical, 10 data shards logical.
	if got.PhysicalSize != 14000 {
		t.Errorf("PhysicalSize: got %d, want 14000", got.PhysicalSize)
	}
	if got.LogicalSize != 10000 {
		t.Errorf("LogicalSize: got %d, want 10000", got.LogicalSize)
	}
}
