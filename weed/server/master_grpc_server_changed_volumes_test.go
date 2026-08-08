package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
