package dash

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestCollectTierStats(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{
						Id: "node1:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"": {
								Type:           "",
								MaxVolumeCount: 10,
								DiskTotalBytes: 1_000_000,
								DiskFreeBytes:  400_000,
								VolumeInfos: []*master_pb.VolumeInformationMessage{
									{Id: 1, Size: 1000},
									{Id: 2, Size: 5000, RemoteStorageName: "s3.backup", RemoteStorageKey: "/2.dat"},
								},
								EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
									{Id: 7, EcIndexBits: 0b111, ShardSizes: []int64{10, 20, 30}},
								},
							},
						},
					},
					{
						Id: "node2:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"ssd": {
								Type:           "ssd",
								MaxVolumeCount: 5,
								// No statfs numbers: capacity falls back to
								// slots and usage to logical bytes.
								VolumeInfos: []*master_pb.VolumeInformationMessage{
									{Id: 3, Size: 2000, DiskType: "ssd"},
									{Id: 4, Size: 7000, DiskType: "ssd", RemoteStorageName: "s3.backup", RemoteStorageKey: "/4.dat"},
									{Id: 5, Size: 100, DiskType: "ssd", RemoteStorageName: "gcs.archive", RemoteStorageKey: "/5.dat"},
								},
							},
						},
					},
					{
						// An old server without statfs numbers sharing the
						// hdd tier with node1: its logical bytes must still
						// count toward the tier's DiskUsed.
						Id: "node3:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"": {
								Type:           "",
								MaxVolumeCount: 2,
								VolumeInfos: []*master_pb.VolumeInformationMessage{
									{Id: 6, Size: 800},
								},
							},
						},
					},
				},
			}},
		}},
	}

	got := CollectTierStats(topo, 30)

	want := []TierStats{
		{Name: "hdd", VolumeCount: 2, EcShardCount: 3, DataSize: 1860, DiskUsed: 600_800, DiskCapacity: 1_000_000 + 2*30*1024*1024, MaxVolumes: 12},
		{Name: "ssd", VolumeCount: 1, DataSize: 2000, DiskUsed: 2000, DiskCapacity: 5 * 30 * 1024 * 1024, MaxVolumes: 5},
		{Name: "gcs.archive", IsRemote: true, VolumeCount: 1, DataSize: 100},
		{Name: "s3.backup", IsRemote: true, VolumeCount: 2, DataSize: 12000},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("CollectTierStats mismatch:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestCollectTierStatsEmpty(t *testing.T) {
	if got := CollectTierStats(nil, 30); got != nil {
		t.Errorf("expected nil for nil topology, got %+v", got)
	}
	if got := CollectTierStats(&master_pb.TopologyInfo{}, 30); len(got) != 0 {
		t.Errorf("expected no tiers for empty topology, got %+v", got)
	}
}
