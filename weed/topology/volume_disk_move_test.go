package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func diskMoveNode(t *testing.T) (*Topology, *DataNode) {
	t.Helper()
	topo := NewTopology("move", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100, "ssd": 100})
	return topo, dn
}

func diskMoveVolume(id uint32, diskType string, diskId uint32) *master_pb.VolumeInformationMessage {
	return &master_pb.VolumeInformationMessage{
		Id: id, Size: 1024, Collection: "c", Version: 3, DiskType: diskType, DiskId: diskId,
	}
}

func heldCopies(dn *DataNode) int {
	total := 0
	for _, c := range dn.Children() {
		total += c.(*Disk).VolumeCount()
	}
	return total
}

// Moving a volume between disks of one server only changes the disk it is
// reported on. The master has to follow it rather than keep the disk it left.
func TestVolumeMovedBetweenDisks(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from, to string
	}{
		{"SameDiskType", "", ""},
		{"DifferentDiskType", "", "ssd"},
		{"BackAgain", "ssd", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topo, dn := diskMoveNode(t)
			topo.SyncDataNodeRegistration(
				[]*master_pb.VolumeInformationMessage{diskMoveVolume(1, tc.from, 0)}, dn)
			topo.SyncDataNodeRegistration(
				[]*master_pb.VolumeInformationMessage{diskMoveVolume(1, tc.to, 1)}, dn)

			if got := heldCopies(dn); got != 1 {
				t.Errorf("master holds %d copies of a volume that moved, want 1", got)
			}
			stored, err := dn.GetVolumesById(needle.VolumeId(1))
			if err != nil {
				t.Fatalf("moved volume is no longer on the node: %v", err)
			}
			if stored.DiskType != tc.to || stored.DiskId != 1 {
				t.Errorf("master has the volume on disk %q/%d, server reports %q/1",
					stored.DiskType, stored.DiskId, tc.to)
			}
			if !dn.HasConsistentVolumeIndex() {
				t.Error("the move left the lookup index disagreeing with the disks")
			}
		})
	}
}

// A server can legitimately hold one volume id on two disks when a stale twin is
// re-attached. Those are two reports, not a move, and dropping one would tell
// the master a replica vanished.
func TestVolumeReportedOnTwoDiskTypesIsKept(t *testing.T) {
	topo, dn := diskMoveNode(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		diskMoveVolume(1, "", 0), diskMoveVolume(1, "ssd", 1),
	}, dn)

	if got := heldCopies(dn); got != 2 {
		t.Errorf("master holds %d copies of a volume reported on two disks, want 2", got)
	}
	if dn.HasDuplicateVolumeIds() {
		t.Error("a volume on two disk types is representable, so it should not disable digest comparison")
	}

	// Once the twin is gone the master follows.
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{diskMoveVolume(1, "ssd", 1)}, dn)
	if got := heldCopies(dn); got != 1 {
		t.Errorf("master holds %d copies after the twin was unmounted, want 1", got)
	}
}

// Twice on one disk type is the case the master genuinely cannot represent.
func TestVolumeReportedTwiceOnOneDiskTypeIsFlagged(t *testing.T) {
	topo, dn := diskMoveNode(t)
	first := diskMoveVolume(1, "ssd", 0)
	second := diskMoveVolume(1, "ssd", 1)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{first, second}, dn)

	if !dn.HasDuplicateVolumeIds() {
		t.Error("a volume reported twice on one disk type went unflagged")
	}
}

func TestVolumeDigestSurvivesADiskMove(t *testing.T) {
	topo, dn := diskMoveNode(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{diskMoveVolume(1, "", 0)}, dn)

	moved := diskMoveVolume(1, "ssd", 1)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{moved}, dn)

	reference, referenceNode := diskMoveNode(t)
	reference.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{moved}, referenceNode)

	if dn.VolumeDigest() != referenceNode.VolumeDigest() {
		t.Errorf("after a disk move the digest is %d, a server holding only the moved volume reports %d",
			dn.VolumeDigest(), referenceNode.VolumeDigest())
	}
}
