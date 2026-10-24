package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestDiskInfoReportsEveryVolume(t *testing.T) {
	topo := NewTopology("diskinfo", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})

	volumes := []*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: "c", Size: 1000, FileCount: 10, Version: 3, DiskId: 2},
		{Id: 2, Collection: "c", Size: 2000, FileCount: 20, Version: 3, DiskId: 2},
		{Id: 3, Collection: "other", Size: 3000, FileCount: 30, Version: 3, DiskId: 2},
	}
	topo.SyncDataNodeRegistration(volumes, dn)
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 9, Collection: "c", EcIndexBits: 0x3fff, DiskId: 2},
	}, dn)

	var info *master_pb.DiskInfo
	for _, c := range dn.Children() {
		info = c.(*Disk).ToDiskInfo(VolumeFilter{})
	}
	if info == nil {
		t.Fatal("the node reported no disk")
	}
	if len(info.VolumeInfos) != len(volumes) {
		t.Fatalf("reported %d volumes, want %d", len(info.VolumeInfos), len(volumes))
	}
	if len(info.EcShardInfos) != 1 {
		t.Fatalf("reported %d ec volumes, want 1", len(info.EcShardInfos))
	}
	if info.DiskId != 2 {
		t.Errorf("reported disk id %d, want the one its volumes are on", info.DiskId)
	}

	bySize := map[uint32]uint64{}
	for _, v := range info.VolumeInfos {
		bySize[v.Id] = v.Size
	}
	for _, want := range volumes {
		if got := bySize[want.Id]; got != want.Size {
			t.Errorf("volume %d reported size %d, want %d", want.Id, got, want.Size)
		}
	}
}

func TestDiskInfoReportsTheDiskIdOfEcOnlyDisks(t *testing.T) {
	topo := NewTopology("diskinfo", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 9, Collection: "c", EcIndexBits: 0x3fff, DiskId: 5},
	}, dn)

	for _, c := range dn.Children() {
		info := c.(*Disk).ToDiskInfo(VolumeFilter{})
		if len(info.VolumeInfos) != 0 {
			t.Fatalf("expected no regular volumes, got %d", len(info.VolumeInfos))
		}
		if info.DiskId != 5 {
			t.Errorf("reported disk id %d, want the one its shards are on", info.DiskId)
		}
	}
}
