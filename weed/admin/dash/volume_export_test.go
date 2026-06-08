package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestBuildExportVolumeDerivedFields(t *testing.T) {
	v := buildExportVolume(&master_pb.VolumeInformationMessage{
		Id:               7,
		Collection:       "photos",
		Size:             100,
		DeletedByteCount: 25,
		ReplicaPlacement: 0,
		ModifiedAtSecond: 1700000000,
	}, 1000)

	if v.GarbageRatio != 0.25 {
		t.Errorf("garbage ratio = %v, want 0.25", v.GarbageRatio)
	}
	if v.FullnessRatio != 0.1 {
		t.Errorf("fullness ratio = %v, want 0.1", v.FullnessRatio)
	}
	if v.ReplicaPlacement != "000" {
		t.Errorf("replica placement = %q, want 000", v.ReplicaPlacement)
	}
	if v.ModifiedAt == "" {
		t.Error("modified_at should be set when modified_at_second > 0")
	}
}

func TestBuildExportDiskFilterAndTotals(t *testing.T) {
	disk := &master_pb.DiskInfo{
		Type:        "hdd",
		VolumeCount: 2,
		VolumeInfos: []*master_pb.VolumeInformationMessage{
			{Id: 1, Collection: "keep", Size: 10, FileCount: 3, DeleteCount: 1, DeletedByteCount: 2},
			{Id: 2, Collection: "drop", Size: 99, FileCount: 9},
		},
		EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
			{Id: 3, Collection: "keep", EcIndexBits: 0b111, ShardSizes: []int64{4, 4, 4}, FileCount: 5},
			{Id: 4, Collection: "drop", EcIndexBits: 0b1, ShardSizes: []int64{7}},
		},
	}

	var totals VolumeExportTotals
	got := buildExportDisk(disk, "keep", 1000, &totals)
	if got == nil {
		t.Fatal("expected disk with matching records, got nil")
	}
	if len(got.Volumes) != 1 || got.Volumes[0].Id != 1 {
		t.Errorf("expected only volume 1 after filter, got %+v", got.Volumes)
	}
	if len(got.EcShards) != 1 || got.EcShards[0].Id != 3 {
		t.Errorf("expected only ec shard 3 after filter, got %+v", got.EcShards)
	}
	if got.VolumeCount != 2 {
		t.Errorf("disk VolumeCount should stay physical (2), got %d", got.VolumeCount)
	}
	if got.EcShards[0].TotalSize != 12 {
		t.Errorf("ec shard total size = %d, want 12", got.EcShards[0].TotalSize)
	}

	if totals.VolumeCount != 1 || totals.EcShardCount != 1 {
		t.Errorf("totals counts = vol %d ec %d, want 1/1", totals.VolumeCount, totals.EcShardCount)
	}
	// 10 (volume size) + 12 (kept ec shard sizes).
	if totals.TotalSize != 22 {
		t.Errorf("totals TotalSize = %d, want 22", totals.TotalSize)
	}
	if totals.FileCount != 3 || totals.DeletedFileCount != 1 || totals.DeletedBytes != 2 {
		t.Errorf("totals files = %d del %d delBytes %d, want 3/1/2", totals.FileCount, totals.DeletedFileCount, totals.DeletedBytes)
	}
}

func TestBuildExportDiskPrunesWhenNoMatch(t *testing.T) {
	disk := &master_pb.DiskInfo{
		Type:        "hdd",
		VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 1, Collection: "other"}},
	}
	var totals VolumeExportTotals
	if got := buildExportDisk(disk, "keep", 0, &totals); got != nil {
		t.Errorf("expected nil for disk with no matching records, got %+v", got)
	}
}

func TestFindDuplicateVolumeIdsForExport(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			RackInfos: []*master_pb.RackInfo{{
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{DiskInfos: map[string]*master_pb.DiskInfo{"hdd": {
						VolumeInfos: []*master_pb.VolumeInformationMessage{
							{Id: 5, Collection: "a"},
							{Id: 7, Collection: "shared"}, // replica below, same collection
						},
					}}},
					{DiskInfos: map[string]*master_pb.DiskInfo{"hdd": {
						VolumeInfos: []*master_pb.VolumeInformationMessage{
							{Id: 5, Collection: "b"},      // 5 now spans a + b -> duplicate
							{Id: 7, Collection: "shared"}, // replica, not a duplicate
						},
						EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
							{Id: 5, Collection: "c"}, // 5 also as EC in c
						},
					}}},
				},
			}},
		}},
	}

	dups := findDuplicateVolumeIdsForExport(topo)
	if len(dups) != 1 {
		t.Fatalf("expected exactly 1 duplicate, got %+v", dups)
	}
	if dups[0].VolumeId != 5 {
		t.Errorf("duplicate volume id = %d, want 5", dups[0].VolumeId)
	}
	want := []string{"a", "b", "c"}
	if len(dups[0].Collections) != len(want) {
		t.Fatalf("collections = %v, want %v", dups[0].Collections, want)
	}
	for i := range want {
		if dups[0].Collections[i] != want[i] {
			t.Errorf("collections[%d] = %q, want %q", i, dups[0].Collections[i], want[i])
		}
	}
}
