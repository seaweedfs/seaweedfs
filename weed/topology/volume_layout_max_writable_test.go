package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func maxWritableLayout(t *testing.T, n int) (*Topology, *DataNode, *VolumeLayout) {
	t.Helper()
	topo := NewTopology("mw", nil, 32*1024, 5, false)
	topo.SetMaxWritableVolumes(n)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	return topo, dn, topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
}

// With a cap, assignments concentrate on the fullest writable volumes so at
// most cap volumes are written in parallel.
func TestPickForWriteCapsWritableVolumes(t *testing.T) {
	topo, dn, vl := maxWritableLayout(t, 1)

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		sizeTrackingVolume(1, 30000, false),
		sizeTrackingVolume(2, 1000, false),
		sizeTrackingVolume(3, 500, false),
	}, dn)

	for i := 0; i < 20; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, &VolumeGrowOption{})
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		if vid != needle.VolumeId(1) {
			t.Fatalf("picked volume %d, want the fullest writable 1", vid)
		}
	}
}

// Once the fullest volume leaves the writable list, the next fullest takes over.
func TestPickForWriteCapFallsToNextWritable(t *testing.T) {
	topo, dn, vl := maxWritableLayout(t, 1)

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		sizeTrackingVolume(1, 30000, false),
		sizeTrackingVolume(2, 1000, false),
	}, dn)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		sizeTrackingVolume(1, 30000, true),
		sizeTrackingVolume(2, 1000, false),
	}, dn)

	vid, _, _, _, err := vl.PickForWrite(1, &VolumeGrowOption{})
	if err != nil {
		t.Fatalf("PickForWrite: %v", err)
	}
	if vid != needle.VolumeId(2) {
		t.Fatalf("picked volume %d, want 2 after 1 went read-only", vid)
	}
}

// A cap of two still concentrates on the two fullest writables; zero disables
// the cap so every writable volume is a candidate.
func TestPickForWriteCapOfTwoAndDisabled(t *testing.T) {
	topo, dn, vl := maxWritableLayout(t, 2)

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		sizeTrackingVolume(1, 30000, false),
		sizeTrackingVolume(2, 20000, false),
		sizeTrackingVolume(3, 500, false),
	}, dn)

	picked := map[needle.VolumeId]bool{}
	for i := 0; i < 40; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, &VolumeGrowOption{})
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		picked[vid] = true
	}
	if len(picked) != 2 || !picked[1] || !picked[2] {
		t.Fatalf("capped pick spread to %v, want only volumes 1 and 2", picked)
	}

	vl.maxWritableVolumes = 0
	picked = map[needle.VolumeId]bool{}
	for i := 0; i < 400; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, &VolumeGrowOption{})
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		picked[vid] = true
	}
	if len(picked) != 3 {
		t.Fatalf("uncapped pick reached %v, want all three volumes", picked)
	}
}
