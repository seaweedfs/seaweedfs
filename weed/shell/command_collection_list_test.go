package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestCollectCollectionInfoEC verifies that EC-encoded volumes contribute to
// collection.list totals and s3.bucket.quota.enforce sums. Before this fix
// the EC branch was a no-op, so encoded volumes silently dropped out of
// collection size accounting.
func TestCollectCollectionInfoEC(t *testing.T) {
	// One 10+4 EC volume split across two nodes: 14 shards * 1000 bytes.
	// Data shards 0..9 live on (nodeA: 0..6, nodeB: 7..9), parity 10..13
	// on nodeB. Every shard holder reports file_count=100; node-local
	// delete counts are 2 + 3 = 5.
	nodeA := &master_pb.DataNodeInfo{
		DiskInfos: map[string]*master_pb.DiskInfo{
			"disk1": {
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
					{
						Id:          42,
						Collection:  "bucket-a",
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
						Collection:  "bucket-a",
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

	infos := make(map[string]*CollectionInfo)
	collectCollectionInfo(topo, infos)

	cif, ok := infos["bucket-a"]
	if !ok {
		t.Fatalf("expected collection bucket-a in infos, got: %v", infos)
	}
	// 10 data shards * 1000 bytes.
	if cif.Size != 10000 {
		t.Errorf("Size: got %.0f, want 10000", cif.Size)
	}
	if cif.FileCount != 100 {
		t.Errorf("FileCount: got %.0f, want 100 (max across reporters)", cif.FileCount)
	}
	if cif.DeleteCount != 5 {
		t.Errorf("DeleteCount: got %.0f, want 5 (sum across reporters)", cif.DeleteCount)
	}
	if cif.VolumeCount != 1 {
		t.Errorf("VolumeCount: got %d, want 1", cif.VolumeCount)
	}
}

// TestCollectCollectionInfoMixed verifies that a collection with both a
// regular volume and an EC volume sums both branches without either clobbering
// the other, matching the state mid-EC-conversion.
func TestCollectCollectionInfoMixed(t *testing.T) {
	node := &master_pb.DataNodeInfo{
		DiskInfos: map[string]*master_pb.DiskInfo{
			"disk1": {
				VolumeInfos: []*master_pb.VolumeInformationMessage{
					{
						Id:          1,
						Collection:  "bucket-mix",
						Size:        5000,
						FileCount:   50,
						DeleteCount: 1,
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

	infos := make(map[string]*CollectionInfo)
	collectCollectionInfo(topo, infos)

	cif, ok := infos["bucket-mix"]
	if !ok {
		t.Fatalf("expected collection bucket-mix in infos, got: %v", infos)
	}
	// Regular: 5000 (replicaCount=1). EC data shards: 6000.
	if cif.Size != 5000+6000 {
		t.Errorf("Size: got %.0f, want 11000", cif.Size)
	}
	if cif.FileCount != 50+80 {
		t.Errorf("FileCount: got %.0f, want 130", cif.FileCount)
	}
	if cif.DeleteCount != 1+4 {
		t.Errorf("DeleteCount: got %.0f, want 5", cif.DeleteCount)
	}
	if cif.VolumeCount != 2 {
		t.Errorf("VolumeCount: got %d, want 2", cif.VolumeCount)
	}
}
