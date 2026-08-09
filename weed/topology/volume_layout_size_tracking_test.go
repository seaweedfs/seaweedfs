package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func sizeTrackingLayout(t *testing.T) (*Topology, *DataNode, *VolumeLayout) {
	t.Helper()
	topo := NewTopology("st", nil, 32*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	return topo, dn, topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
}

func sizeTrackingVolume(id uint32, size uint64, readOnly bool) *master_pb.VolumeInformationMessage {
	return &master_pb.VolumeInformationMessage{
		Id: id, Size: size, Collection: "c", Version: 3, ReadOnly: readOnly,
	}
}

func TestSizeTrackingSkipsReadOnlyVolumes(t *testing.T) {
	topo, dn, vl := sizeTrackingLayout(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		sizeTrackingVolume(1, 1000, false), sizeTrackingVolume(2, 1000, true),
	}, dn)

	vl.accessLock.RLock()
	_, writableTracked := vl.sizeTracking[needle.VolumeId(1)]
	_, readOnlyTracked := vl.sizeTracking[needle.VolumeId(2)]
	vl.accessLock.RUnlock()

	if !writableTracked {
		t.Error("a writable volume lost its size tracking, so assignments stop being accounted for")
	}
	if readOnlyTracked {
		t.Error("a read-only volume is tracked for writes it can never take")
	}
}

// A volume that goes read-only after the master already tracked it has to give
// the entry back, or a cluster that tiers its volumes keeps paying for them.
func TestSizeTrackingReleasedWhenAVolumeGoesReadOnly(t *testing.T) {
	topo, dn, vl := sizeTrackingLayout(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{sizeTrackingVolume(1, 1000, false)}, dn)

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{sizeTrackingVolume(1, 1000, true)}, dn)

	vl.accessLock.RLock()
	_, tracked := vl.sizeTracking[needle.VolumeId(1)]
	vl.accessLock.RUnlock()
	if tracked {
		t.Error("a volume that became read-only kept its size tracking")
	}
}

func TestSizeTrackingReturnsWhenAVolumeBecomesWritable(t *testing.T) {
	topo, dn, vl := sizeTrackingLayout(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{sizeTrackingVolume(1, 1000, true)}, dn)

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{sizeTrackingVolume(1, 2000, false)}, dn)

	vl.accessLock.RLock()
	st, tracked := vl.sizeTracking[needle.VolumeId(1)]
	vl.accessLock.RUnlock()
	if !tracked {
		t.Fatal("a volume that became writable was not tracked again")
	}
	if st.reportedSize != 2000 {
		t.Errorf("tracking resumed at size %d, want the reported 2000", st.reportedSize)
	}
}

// A volume held out of the writable list for capacity is not read-only, and its
// entry is what enforces the recovery delay.
func TestSizeTrackingSurvivesAFullVolume(t *testing.T) {
	topo, dn, vl := sizeTrackingLayout(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{sizeTrackingVolume(1, 1000, false)}, dn)

	vl.RecordAssign(needle.VolumeId(1), int64(32*1024))

	vl.accessLock.RLock()
	st, tracked := vl.sizeTracking[needle.VolumeId(1)]
	vl.accessLock.RUnlock()
	if !tracked {
		t.Fatal("a full volume lost its tracking, so nothing holds it out of the writable list")
	}
	if st.fullSince.IsZero() {
		t.Error("a full volume did not record when it filled, so the recovery delay cannot apply")
	}
}

// A volume is unwritable if any replica is read-only, so the answer must not
// depend on which replica's heartbeat arrives first.
func TestSizeTrackingIgnoresReplicaReportOrder(t *testing.T) {
	for _, readOnlyFirst := range []bool{true, false} {
		name := "WritableFirst"
		if readOnlyFirst {
			name = "ReadOnlyFirst"
		}
		t.Run(name, func(t *testing.T) {
			topo := NewTopology("st", nil, 32*1024, 5, false)
			rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
			writableNode := rack.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "a", map[string]uint32{"": 100})
			readOnlyNode := rack.GetOrCreateDataNode("10.0.0.2", 8080, 18080, "", "b", map[string]uint32{"": 100})

			report := func(dn *DataNode, readOnly bool) {
				topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
					{Id: 1, Size: 1000, Collection: "c", Version: 3, ReplicaPlacement: 1, ReadOnly: readOnly},
				}, dn)
			}
			if readOnlyFirst {
				report(readOnlyNode, true)
				report(writableNode, false)
			} else {
				report(writableNode, false)
				report(readOnlyNode, true)
			}

			rp, _ := super_block.NewReplicaPlacementFromString("001")
			vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
			vl.accessLock.RLock()
			_, tracked := vl.sizeTracking[needle.VolumeId(1)]
			_, crowded := vl.crowded[needle.VolumeId(1)]
			vl.accessLock.RUnlock()

			if tracked {
				t.Error("a volume with a read-only replica is tracked for writes it cannot take")
			}
			if crowded {
				t.Error("a volume with a read-only replica was left in the crowded set")
			}
		})
	}
}

// The crowded entry has to go with the tracking, or the memory this releases
// is only moved.
func TestCrowdedEntryReleasedWithSizeTracking(t *testing.T) {
	topo, dn, vl := sizeTrackingLayout(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{sizeTrackingVolume(1, 31000, false)}, dn)

	vl.accessLock.RLock()
	_, crowdedWhileWritable := vl.crowded[needle.VolumeId(1)]
	vl.accessLock.RUnlock()
	if !crowdedWhileWritable {
		t.Fatal("expected a nearly full writable volume to be crowded")
	}

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{sizeTrackingVolume(1, 31000, true)}, dn)

	vl.accessLock.RLock()
	_, stillCrowded := vl.crowded[needle.VolumeId(1)]
	vl.accessLock.RUnlock()
	if stillCrowded {
		t.Error("a volume that became read-only kept its crowded entry")
	}
}
