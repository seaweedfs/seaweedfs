package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestWritableVolumesPerDisk(t *testing.T) {
	topo := NewTopology("perdisk", nil, 10000, 5, false)
	dn1 := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	dn2 := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack2").
		GetOrCreateDataNode("127.0.0.2", 8080, 18080, "", "", map[string]uint32{"": 100})

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: "c", Version: 3, ReplicaPlacement: 10, DiskId: 0},
		{Id: 2, Collection: "c", Version: 3, ReplicaPlacement: 10, DiskId: 0},
		{Id: 3, Collection: "c", Version: 3, ReplicaPlacement: 10, DiskId: 1, ReadOnly: true},
	}, dn1)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: "c", Version: 3, ReplicaPlacement: 10, DiskId: 0},
		{Id: 2, Collection: "c", Version: 3, ReplicaPlacement: 10, DiskId: 1},
		{Id: 3, Collection: "c", Version: 3, ReplicaPlacement: 10, DiskId: 1, ReadOnly: true},
	}, dn2)

	rp, _ := super_block.NewReplicaPlacementFromString("010")
	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.SetVolumeCrowded(needle.VolumeId(2))

	writable, crowded := vl.WritableVolumesPerDisk()
	d1 := WritableDisk{DataCenter: "dc1", Rack: "rack1", Node: string(dn1.Id()), DiskId: 0}
	d20 := WritableDisk{DataCenter: "dc1", Rack: "rack2", Node: string(dn2.Id()), DiskId: 0}
	d21 := WritableDisk{DataCenter: "dc1", Rack: "rack2", Node: string(dn2.Id()), DiskId: 1}
	wantWritable := map[WritableDisk]int{d1: 2, d20: 1, d21: 1}
	wantCrowded := map[WritableDisk]int{d1: 1, d21: 1}
	if len(writable) != len(wantWritable) {
		t.Fatalf("writable = %v, want %v", writable, wantWritable)
	}
	for d, n := range wantWritable {
		if writable[d] != n {
			t.Errorf("writable[%+v] = %d, want %d", d, writable[d], n)
		}
	}
	if len(crowded) != len(wantCrowded) {
		t.Fatalf("crowded = %v, want %v", crowded, wantCrowded)
	}
	for d, n := range wantCrowded {
		if crowded[d] != n {
			t.Errorf("crowded[%+v] = %d, want %d", d, crowded[d], n)
		}
	}
}
