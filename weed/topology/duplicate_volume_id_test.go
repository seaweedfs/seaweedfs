package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func TestDuplicateVolumeIdAcrossCollections(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")

	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}

	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v2, dn2)
	topo.flagDuplicateVolumeId(v2, dn2)

	vi, err := dn2.GetVolumesById(7)
	if err != nil {
		t.Fatalf("volume info not stored on dn2: %v", err)
	}
	if !vi.ReadOnly {
		t.Fatal("duplicate newcomer should be marked read-only")
	}

	vi, err = dn1.GetVolumesById(7)
	if err != nil {
		t.Fatalf("volume info not stored on dn1: %v", err)
	}
	if !vi.ReadOnly {
		t.Fatal("either copy could be the impostor; the earlier copy must go read-only too")
	}
}

func TestSameLayoutReplicaIsNotDuplicate(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("001")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")

	v := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v})
	dn2.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn1)
	topo.RegisterVolumeLayout(v, dn2)
	topo.flagDuplicateVolumeId(v, dn2)

	vi, err := dn2.GetVolumesById(7)
	if err != nil {
		t.Fatalf("volume info not stored on dn2: %v", err)
	}
	if vi.ReadOnly {
		t.Fatal("a same-layout replica is a legitimate copy, not a duplicate")
	}
}

func TestSameNodeVolumeMoveIsNotDuplicate(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	dn := NewDataNode("server1")

	old := storage.VolumeInfo{Id: 7, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	moved := storage.VolumeInfo{Id: 7, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: "ssd"}
	dn.UpdateVolumes([]storage.VolumeInfo{old})
	topo.RegisterVolumeLayout(old, dn)

	// the move registers the new layout before the old entry is removed
	dn.UpdateVolumes([]storage.VolumeInfo{moved})
	topo.RegisterVolumeLayout(moved, dn)
	topo.flagDuplicateVolumeId(moved, dn)

	vi, err := dn.GetVolumesById(7)
	if err != nil {
		t.Fatalf("volume info not stored on dn: %v", err)
	}
	if vi.ReadOnly {
		t.Fatal("a same-node disk move is not a duplicate")
	}
}

func TestQuarantineSurvivesResync(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")
	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v2, dn2)
	topo.flagDuplicateVolumeId(v2, dn2)

	// a resync from the same server re-reports the volume as writable;
	// the quarantine must keep it read-only
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{{
		Id: 7, Collection: "b", ReplicaPlacement: 0, Version: 3,
	}}, dn2)

	vi, err := dn2.GetVolumesById(7)
	if err != nil {
		t.Fatalf("volume info not stored on dn2: %v", err)
	}
	if !vi.ReadOnly {
		t.Fatal("resync must not clear the quarantine's read-only mark")
	}
}
