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

// TestCollectCollectionStatsECFileAndDeleteCountAggregation verifies that
// FileCount for an EC volume is deduped across nodes (every shard holder has
// an identical .ecx, so the total entry count is taken once) while
// DeleteCount is summed (each needle delete tombstones exactly one node's
// .ecx, so the true delete total is the sum of every holder's local count).
func TestCollectCollectionStatsECFileAndDeleteCountAggregation(t *testing.T) {
	// Volume id=7 reported by three nodes. Every node reports file_count=100
	// (same .ecx). Local delete counts: 5 + 3 + 2 = 10 deletes total.
	// Expected live object count = 100 - 10 = 90.
	makeNode := func(bits uint32, sizes []int64, deleteCount uint64) *master_pb.DataNodeInfo {
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
							DeleteCount: deleteCount,
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
							makeNode((1<<0)|(1<<1)|(1<<2)|(1<<3), []int64{1000, 1000, 1000, 1000}, 5),
							makeNode((1<<4)|(1<<5)|(1<<6)|(1<<7), []int64{1000, 1000, 1000, 1000}, 3),
							makeNode((1<<8)|(1<<9)|(1<<10)|(1<<11)|(1<<12)|(1<<13), []int64{1000, 1000, 1000, 1000, 1000, 1000}, 2),
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
		t.Errorf("FileCount: got %d, want 90 (100 total - 10 deletes summed)", got.FileCount)
	}
	// Sanity: 14 shards × 1000 bytes physical, 10 data shards logical.
	if got.PhysicalSize != 14000 {
		t.Errorf("PhysicalSize: got %d, want 14000", got.PhysicalSize)
	}
	if got.LogicalSize != 10000 {
		t.Errorf("LogicalSize: got %d, want 10000", got.LogicalSize)
	}
}

// TestCollectCollectionStatsECFileCountMaxDedupe verifies that EC file_count
// is taken as the max across reporting nodes rather than the first-seen
// value. A node that has not yet finished loading .ecx reports file_count=0,
// which previously poisoned the aggregate and rendered buckets backed by EC
// volumes as "0 objects".
func TestCollectCollectionStatsECFileCountMaxDedupe(t *testing.T) {
	makeNode := func(bits uint32, sizes []int64, fileCount uint64) *master_pb.DataNodeInfo {
		return &master_pb.DataNodeInfo{
			DiskInfos: map[string]*master_pb.DiskInfo{
				"disk1": {
					EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
						{
							Id:          11,
							Collection:  "bucket-b",
							EcIndexBits: bits,
							ShardSizes:  sizes,
							FileCount:   fileCount,
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
						// First-reporting node has a stale fileCount of 0,
						// second node reports the authoritative 6.
						DataNodeInfos: []*master_pb.DataNodeInfo{
							makeNode((1<<0)|(1<<1)|(1<<2)|(1<<3)|(1<<4)|(1<<5)|(1<<6), []int64{1, 1, 1, 1, 1, 1, 1}, 0),
							makeNode((1<<7)|(1<<8)|(1<<9)|(1<<10)|(1<<11)|(1<<12)|(1<<13), []int64{1, 1, 1, 1, 1, 1, 1}, 6),
						},
					},
				},
			},
		},
	}

	stats := collectCollectionStats(topo)
	got, ok := stats["bucket-b"]
	if !ok {
		t.Fatalf("expected collection bucket-b in stats, got: %v", stats)
	}
	if got.FileCount != 6 {
		t.Errorf("FileCount: got %d, want 6 (max across reporters)", got.FileCount)
	}
}
