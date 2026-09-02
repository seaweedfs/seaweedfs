package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func changedTestCluster(t *testing.T) (*topology.Topology, *topology.DataNode) {
	t.Helper()
	topo := topology.NewTopology("test", sequence.NewMemorySequencer(), 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	return topo, dn
}

func changedTestVolume(id uint32, size uint64) *master_pb.VolumeInformationMessage {
	return &master_pb.VolumeInformationMessage{
		Id: id, Size: size, Collection: "c", Version: 3, FileCount: 1,
	}
}

// Applying only the volumes a heartbeat named has to leave the master holding
// what the server holds, or the digest it is checked against means nothing.
func TestChangedVolumesBringTheMasterCurrent(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024), changedTestVolume(2, 1024), changedTestVolume(3, 1024),
	}, dn)

	grown := changedTestVolume(2, 8192)
	topo.ApplyVolumeChanges([]*master_pb.VolumeInformationMessage{grown}, dn)

	reference, referenceNode := changedTestCluster(t)
	reference.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024), grown, changedTestVolume(3, 1024),
	}, referenceNode)

	if dn.VolumeDigest() != referenceNode.VolumeDigest() {
		t.Errorf("after applying the change the master digests %d, the server reports %d",
			dn.VolumeDigest(), referenceNode.VolumeDigest())
	}
}

// Silence about a volume in a changed-only heartbeat says nothing about whether
// the server still has it, unlike a full report.
func TestChangedVolumesDoNotRemoveUnmentionedVolumes(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024), changedTestVolume(2, 1024),
	}, dn)

	topo.ApplyVolumeChanges([]*master_pb.VolumeInformationMessage{changedTestVolume(1, 4096)}, dn)

	if _, err := dn.GetVolumesById(needle.VolumeId(2)); err != nil {
		t.Errorf("a volume the heartbeat did not mention was dropped: %v", err)
	}
	if !dn.HasConsistentVolumeIndex() {
		t.Error("applying changes left the lookup index disagreeing with the disks")
	}
}

// A volume the master has never seen can arrive as a change.
func TestChangedVolumesRegisterUnknownVolumes(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{changedTestVolume(1, 1024)}, dn)

	topo.ApplyVolumeChanges([]*master_pb.VolumeInformationMessage{changedTestVolume(9, 1024)}, dn)

	if locations := topo.Lookup("c", needle.VolumeId(9)); len(locations) != 1 {
		t.Errorf("a volume first seen as a change is not servable: %v", locations)
	}
	if !dn.HasConsistentVolumeIndex() {
		t.Error("applying changes left the lookup index disagreeing with the disks")
	}
}

// Volumes grow constantly, and a growth moves no location. Telling every
// connected client about each one would flood bounded broadcast queues and push
// out the topology updates that do matter.
func TestChangedVolumesAnnounceOnlyArrivals(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024), changedTestVolume(2, 1024),
	}, dn)

	grown := topo.ApplyVolumeChanges([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 8192), changedTestVolume(2, 9216),
	}, dn)
	if len(grown) != 0 {
		t.Errorf("volumes that only grew were announced as new locations: %v", grown)
	}

	arrived := topo.ApplyVolumeChanges([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 16384), changedTestVolume(7, 1024),
	}, dn)
	if len(arrived) != 1 || arrived[0].Id != needle.VolumeId(7) {
		t.Errorf("expected only the volume that arrived, got %v", arrived)
	}
}

// A node dropping out tells clients its volumes went. If the lookup index then
// loses one while the disk map keeps it, repairing the index has to announce it
// as well: nothing else will, and the clients that heard it go would never hear
// otherwise.
func TestRepairedLookupEntryIsAnnounced(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*topology.Topology, *topology.DataNode, *master_pb.VolumeInformationMessage) []storage.VolumeInfo
	}{
		{"ViaChanges", func(topo *topology.Topology, dn *topology.DataNode, v *master_pb.VolumeInformationMessage) []storage.VolumeInfo {
			return topo.ApplyVolumeChanges([]*master_pb.VolumeInformationMessage{v}, dn)
		}},
		{"ViaFullList", func(topo *topology.Topology, dn *topology.DataNode, v *master_pb.VolumeInformationMessage) []storage.VolumeInfo {
			announced, _, _ := topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{v}, dn)
			return announced
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topo, dn := changedTestCluster(t)
			volume := changedTestVolume(1, 1024)
			topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{volume}, dn)

			rp, _ := super_block.NewReplicaPlacementFromString("000")
			vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
			vl.SetVolumeUnavailable(dn, needle.VolumeId(1))
			if locations := topo.Lookup("c", needle.VolumeId(1)); locations != nil {
				t.Fatalf("expected the volume to be unservable, got %v", locations)
			}

			announced := tc.apply(topo, dn, changedTestVolume(1, 1024))

			if locations := topo.Lookup("c", needle.VolumeId(1)); len(locations) != 1 {
				t.Fatalf("the repair did not make the volume servable again: %v", locations)
			}
			if len(announced) != 1 || announced[0].Id != needle.VolumeId(1) {
				t.Errorf("the repair was not announced to clients, so those told it went stay stale: %v", announced)
			}
		})
	}
}

// Deleting a collection throws its layouts away wholesale. The lookup bits
// they held must go with them: leaked bits keep the node's held and servable
// digests apart forever, and the master then asks for the full volume list on
// every heartbeat for the rest of the process's life.
func TestDeletedCollectionReleasesTheLookupIndex(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024), changedTestVolume(2, 1024),
	}, dn)
	if !dn.HasConsistentVolumeIndex() {
		t.Fatal("expected a freshly synced node to be consistent")
	}

	topo.DeleteCollection("c")
	// The node still holds the volumes until a heartbeat names their
	// departure; this is that heartbeat.
	topo.IncrementalSyncDataNodeRegistration(nil, []*master_pb.VolumeShortInformationMessage{
		{Id: 1, Collection: "c", Version: 3},
		{Id: 2, Collection: "c", Version: 3},
	}, dn)

	if !dn.HasConsistentVolumeIndex() {
		t.Fatal("the deleted collection's lookup entries were not released")
	}
}

// A full list races the growth that runs while it is in flight: collected
// before the grow finished, it cannot name the volumes the grow registered.
// Erasing them strands their collection without writable volumes until a
// later report happens to re-add them.
func TestStaleFullListDoesNotEraseAFreshGrow(t *testing.T) {
	topo, dn := changedTestCluster(t)
	full := []*master_pb.VolumeInformationMessage{changedTestVolume(1, 1024)}
	topo.SyncDataNodeRegistration(full, dn)

	// what volume growth registers, after the list above was collected
	vi, err := storage.NewVolumeInfo(changedTestVolume(2, 8))
	if err != nil {
		t.Fatal(err)
	}
	dn.AddProvisionalVolume(vi)
	topo.RegisterVolumeLayout(vi, dn)

	// the stale list arrives
	topo.SyncDataNodeRegistration(full, dn)
	if _, err := dn.GetVolumesById(needle.VolumeId(2)); err != nil {
		t.Fatal("a stale full list erased a freshly grown volume")
	}

	// once a report names it, a list without it means it is really gone
	confirmed := append(append([]*master_pb.VolumeInformationMessage{}, full...), changedTestVolume(2, 8))
	topo.SyncDataNodeRegistration(confirmed, dn)
	topo.SyncDataNodeRegistration(full, dn)
	if _, err := dn.GetVolumesById(needle.VolumeId(2)); err == nil {
		t.Fatal("a confirmed volume survived a list that dropped it")
	}
}

// A registration can resolve a collection's layout just before the collection
// is deleted. Bits it sets afterwards would never be released, and the node's
// held and servable digests would disagree forever.
func TestRegistrationRacingCollectionDeleteDoesNotLeak(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{changedTestVolume(1, 1024)}, dn)

	vi, err := storage.NewVolumeInfo(changedTestVolume(1, 1024))
	if err != nil {
		t.Fatal(err)
	}
	stale := topo.GetVolumeLayout("c", vi.ReplicaPlacement, vi.Ttl, types.ToDiskType(vi.DiskType))

	topo.DeleteCollection("c")

	// the interleaved registration must refuse the dropped layout
	if stale.RegisterVolume(&vi, dn) {
		t.Fatal("registered into a layout dropped with its collection")
	}

	// the node prunes its copy when the departure is named
	topo.IncrementalSyncDataNodeRegistration(nil, []*master_pb.VolumeShortInformationMessage{
		{Id: 1, Collection: "c", Version: 3},
	}, dn)

	if !dn.HasConsistentVolumeIndex() {
		t.Fatal("the racing registration leaked lookup ownership")
	}
}
