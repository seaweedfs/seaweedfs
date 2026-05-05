package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestCollectCollectionInfoFromTopologyEC verifies that EC-encoded volumes
// contribute to per-collection logical/physical size, file count, and volume
// count. Before this fix, encoding a volume to EC caused the bucket size
// metrics exported to Prometheus to drop to zero for that volume.
//
// Layout: one 10+4 EC volume in collection "crm-docs-storage", 14 shards of
// 1000 bytes each split across two nodes.
//   - nodeA holds data shards 0..6 (7 * 1000 = 7000)
//   - nodeB holds data shards 7..9 (3 * 1000 = 3000) and parity 10..13 (4 * 1000 = 4000)
//
// Expected:
//   - PhysicalSize = 14 * 1000 = 14000
//   - Size (logical, data shards) = 10 * 1000 = 10000
//   - FileCount = 100 total - (2 + 3) local deletes = 95 is NOT what we check; the
//     collector reports raw file_count and delete_count as separate gauges, so
//     we assert FileCount = 100 (max across reporters) and DeleteCount = 5 (sum).
//   - VolumeCount = 1 (one unique EC volume)
func TestCollectCollectionInfoFromTopologyEC(t *testing.T) {
	nodeA := &master_pb.DataNodeInfo{
		DiskInfos: map[string]*master_pb.DiskInfo{
			"disk1": {
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
					{
						Id:          42,
						Collection:  "crm-docs-storage",
						EcIndexBits: (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 6),
						ShardSizes:  []int64{1000, 1000, 1000, 1000, 1000, 1000, 1000},
						FileCount:   100,
						DeleteCount: 2,
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
						Id:          42,
						Collection:  "crm-docs-storage",
						EcIndexBits: (1 << 7) | (1 << 8) | (1 << 9) | (1 << 10) | (1 << 11) | (1 << 12) | (1 << 13),
						ShardSizes:  []int64{1000, 1000, 1000, 1000, 1000, 1000, 1000},
						FileCount:   100,
						DeleteCount: 3,
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

	got := make(map[string]*CollectionInfo)
	collectCollectionInfoFromTopology(topo, got)

	info, ok := got["crm-docs-storage"]
	if !ok {
		t.Fatalf("expected collection crm-docs-storage, got: %v", got)
	}
	if info.PhysicalSize != 14000 {
		t.Errorf("PhysicalSize: got %.0f, want 14000", info.PhysicalSize)
	}
	if info.Size != 10000 {
		t.Errorf("Size (logical): got %.0f, want 10000", info.Size)
	}
	if info.FileCount != 100 {
		t.Errorf("FileCount: got %.0f, want 100 (max across reporters)", info.FileCount)
	}
	if info.DeleteCount != 5 {
		t.Errorf("DeleteCount: got %.0f, want 5 (sum across reporters)", info.DeleteCount)
	}
	if info.VolumeCount != 1 {
		t.Errorf("VolumeCount: got %d, want 1", info.VolumeCount)
	}
}

// TestCollectCollectionInfoFromTopologyMixed verifies that regular and EC
// volumes accumulate under the same collection without one clobbering the
// other, which is the state during an in-progress EC conversion.
func TestCollectCollectionInfoFromTopologyMixed(t *testing.T) {
	node := &master_pb.DataNodeInfo{
		DiskInfos: map[string]*master_pb.DiskInfo{
			"disk1": {
				VolumeInfos: []*master_pb.VolumeInformationMessage{
					{
						Id:               1,
						Collection:       "bucket-mix",
						Size:             5000,
						FileCount:        50,
						DeleteCount:      1,
						DeletedByteCount: 100,
					},
				},
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
					{
						Id:          2,
						Collection:  "bucket-mix",
						EcIndexBits: (1 << 0) | (1 << 1) | (1 << 10), // 2 data + 1 parity
						ShardSizes:  []int64{3000, 3000, 3000},
						FileCount:   80,
						DeleteCount: 4,
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
						DataNodeInfos: []*master_pb.DataNodeInfo{node},
					},
				},
			},
		},
	}

	got := make(map[string]*CollectionInfo)
	collectCollectionInfoFromTopology(topo, got)

	info, ok := got["bucket-mix"]
	if !ok {
		t.Fatalf("expected collection bucket-mix, got: %v", got)
	}
	// Regular volume: 5000 physical + logical. EC shards: 9000 physical,
	// 6000 logical (data shards 0 and 1).
	if info.PhysicalSize != 5000+9000 {
		t.Errorf("PhysicalSize: got %.0f, want 14000", info.PhysicalSize)
	}
	if info.Size != 5000+6000 {
		t.Errorf("Size: got %.0f, want 11000", info.Size)
	}
	if info.FileCount != 50+80 {
		t.Errorf("FileCount: got %.0f, want 130", info.FileCount)
	}
	if info.DeleteCount != 1+4 {
		t.Errorf("DeleteCount: got %.0f, want 5", info.DeleteCount)
	}
	if info.VolumeCount != 2 {
		t.Errorf("VolumeCount: got %d, want 2", info.VolumeCount)
	}
}

// TestCollectCollectionInfoFromTopologyECFileCountMaxDedupe verifies that a
// slow shard holder reporting file_count=0 (because it has not yet finished
// loading .ecx) does not pin the per-volume FileCount at 0.
func TestCollectCollectionInfoFromTopologyECFileCountMaxDedupe(t *testing.T) {
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
						DataNodeInfos: []*master_pb.DataNodeInfo{
							makeNode((1<<0)|(1<<1)|(1<<2)|(1<<3)|(1<<4)|(1<<5)|(1<<6), []int64{1, 1, 1, 1, 1, 1, 1}, 0),
							makeNode((1<<7)|(1<<8)|(1<<9)|(1<<10)|(1<<11)|(1<<12)|(1<<13), []int64{1, 1, 1, 1, 1, 1, 1}, 6),
						},
					},
				},
			},
		},
	}

	got := make(map[string]*CollectionInfo)
	collectCollectionInfoFromTopology(topo, got)
	info, ok := got["bucket-b"]
	if !ok {
		t.Fatalf("expected collection bucket-b, got: %v", got)
	}
	if info.FileCount != 6 {
		t.Errorf("FileCount: got %.0f, want 6 (max across reporters)", info.FileCount)
	}
}
