package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// The oversized verdict must outlive the location list: when every replica
// leaves, vid2location drops the entry and its per-replica marks with it, and
// a replica re-registering from a size-less report must not put the volume
// back on the writable list until a heartbeat re-measures it.
func TestOversizedVolumeStaysUnwritableAcrossLocationReset(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("010")

	dn1 := NewDataNode("server1")
	dn1.Ip, dn1.Port = "10.0.0.1", 8080
	dn2 := NewDataNode("server2")
	dn2.Ip, dn2.Port = "10.0.0.2", 8080

	v := storage.VolumeInfo{Id: 35, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, Size: 300}
	dn1.UpdateVolumes([]storage.VolumeInfo{v})
	dn2.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn1)
	topo.RegisterVolumeLayout(v, dn2)

	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.UpdateOversizedState(&v, dn1)
	vl.UpdateOversizedState(&v, dn2)
	vl.EnsureCorrectWritables(&v)
	if isWritable(t, topo, v) {
		t.Fatal("an oversized volume should not be writable")
	}

	// Every replica leaves: the location list and its per-replica marks go.
	vl.SetVolumeUnavailable(dn1, v.Id)
	vl.SetVolumeUnavailable(dn2, v.Id)

	// Both replicas re-register from a report that carries no size.
	small := v
	small.Size = 0
	topo.RegisterVolumeLayout(small, dn1)
	topo.RegisterVolumeLayout(small, dn2)

	if isWritable(t, topo, v) {
		t.Fatal("an oversized volume was re-admitted to the writable list before its size was re-measured")
	}

	// A report under the limit clears the remembered verdict.
	shrunk := v
	shrunk.Size = 10
	vl.UpdateOversizedState(&shrunk, dn1)
	vl.UpdateOversizedState(&shrunk, dn2)
	vl.EnsureCorrectWritables(&shrunk)
	if !isWritable(t, topo, v) {
		t.Fatal("a shrunk volume should be writable again")
	}
}

// The writable admission gate itself must refuse a known-oversized volume:
// paths like quarantine release and mark-writable do not consult the marks.
func TestOversizedVolumeRejectsExplicitWritableMark(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("010")

	dn1 := NewDataNode("server1")
	dn1.Ip, dn1.Port = "10.0.0.1", 8080
	dn2 := NewDataNode("server2")
	dn2.Ip, dn2.Port = "10.0.0.2", 8080

	v := storage.VolumeInfo{Id: 35, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, Size: 300}
	dn1.UpdateVolumes([]storage.VolumeInfo{v})
	dn2.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn1)
	topo.RegisterVolumeLayout(v, dn2)

	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.UpdateOversizedState(&v, dn1)
	vl.UpdateOversizedState(&v, dn2)
	vl.EnsureCorrectWritables(&v)

	if vl.SetVolumeWritable(dn1, v.Id) || isWritable(t, topo, v) {
		t.Fatal("mark-writable re-admitted an oversized volume")
	}

	// A vacuum commit asks the node whether the volume fits; its fresh
	// answer supersedes the remembered verdict once every replica reports.
	if vl.SetVolumeAvailable(dn1, v.Id, false, false, false) {
		t.Fatal("the other replica's oversized mark still stands")
	}
	if !vl.SetVolumeAvailable(dn2, v.Id, false, false, false) || !isWritable(t, topo, v) {
		t.Fatal("a committed volume under the limit should be writable")
	}
}
