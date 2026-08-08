package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func digestTestCluster(t *testing.T) (*MasterServer, *topology.DataNode) {
	t.Helper()
	topo := topology.NewTopology("test", sequence.NewMemorySequencer(), 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	return &MasterServer{Topo: topo}, dn
}

func digestTestVolumeMessage(id uint32) *master_pb.VolumeInformationMessage {
	return &master_pb.VolumeInformationMessage{
		Id: id, Size: 1024, Collection: "c", Version: 3,
	}
}

// A volume server that predates the digest keeps sending its whole list and
// must never be asked for anything, whatever the master computes. This is what
// lets the two sides be upgraded in either order.
func TestDigestCheckIgnoresServersThatReportNone(t *testing.T) {
	ms, dn := digestTestCluster(t)
	ms.Topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1)}, dn)

	if ms.checkVolumeDigest(&master_pb.Heartbeat{
		Volumes: []*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1)},
	}, dn) {
		t.Error("a server reporting no digest was asked to resend")
	}
}

func TestDigestCheckAcceptsAMatchingReport(t *testing.T) {
	ms, dn := digestTestCluster(t)
	volumes := []*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1), digestTestVolumeMessage(2)}
	ms.Topo.SyncDataNodeRegistration(volumes, dn)

	digest := dn.VolumeDigest()
	if ms.checkVolumeDigest(&master_pb.Heartbeat{Volumes: volumes, VolumeDigest: &digest}, dn) {
		t.Error("a matching digest was asked to resend")
	}
}

// A heartbeat that already carried the whole list has nothing further to give,
// so a mismatch there is a genuine disagreement to report rather than something
// to ask about again.
func TestDigestCheckDoesNotReaskAfterAFullList(t *testing.T) {
	ms, dn := digestTestCluster(t)
	volumes := []*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1)}
	ms.Topo.SyncDataNodeRegistration(volumes, dn)

	wrong := dn.VolumeDigest() ^ 1
	if ms.checkVolumeDigest(&master_pb.Heartbeat{Volumes: volumes, VolumeDigest: &wrong}, dn) {
		t.Error("a full volume list that still disagreed was asked to resend, which would repeat forever")
	}
}

// The case the request exists for: a heartbeat carrying no list whose digest
// disagrees means the master has drifted and needs the list back.
func TestDigestCheckAsksForTheListWhenADeltaDisagrees(t *testing.T) {
	ms, dn := digestTestCluster(t)
	ms.Topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1)}, dn)

	wrong := dn.VolumeDigest() ^ 1
	if !ms.checkVolumeDigest(&master_pb.Heartbeat{VolumeDigest: &wrong}, dn) {
		t.Error("a disagreeing digest with no list to fall back on was not asked to resend")
	}
}

// A node reporting one volume id twice is stored once, so the digests cannot
// agree however often the list is resent.
// A node reporting one volume id twice is stored once, so no digest can ever
// agree. It has to keep sending its whole list: nothing else would tell the
// master what it stopped holding.
func TestDigestCheckKeepsDuplicateNodesOnFullLists(t *testing.T) {
	ms, dn := digestTestCluster(t)
	duplicated := digestTestVolumeMessage(1)
	duplicated.DiskId = 1
	ms.Topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		digestTestVolumeMessage(1), duplicated,
	}, dn)

	digest := dn.VolumeDigest()
	if !ms.checkVolumeDigest(&master_pb.Heartbeat{VolumeDigest: &digest}, dn) {
		t.Error("a node whose digest can never be verified was left sending only changes")
	}
	// And is not asked again for a list it just sent.
	if ms.checkVolumeDigest(&master_pb.Heartbeat{
		Volumes:      []*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1), duplicated},
		VolumeDigest: &digest,
	}, dn) {
		t.Error("a node that just sent its whole list was asked for it again")
	}
}

// The lookup index can drift from the disks without the volume server seeing
// anything, so its digest still matches. Only a full report re-registers the
// volumes that stopped being servable, and in delta mode nothing else asks for
// one.
func TestDigestCheckAsksForTheListWhenTheLookupIndexDrifts(t *testing.T) {
	ms, dn := digestTestCluster(t)
	volumes := []*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1), digestTestVolumeMessage(2)}
	ms.Topo.SyncDataNodeRegistration(volumes, dn)

	digest := dn.VolumeDigest()
	if ms.checkVolumeDigest(&master_pb.Heartbeat{VolumeDigest: &digest}, dn) {
		t.Fatal("a healthy node was asked to resend")
	}

	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := ms.Topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.SetVolumeUnavailable(dn, needle.VolumeId(1))

	if dn.VolumeDigest() != digest {
		t.Fatal("expected the reported digest to be unaffected, which is why the index has to be checked")
	}
	if !ms.checkVolumeDigest(&master_pb.Heartbeat{VolumeDigest: &digest}, dn) {
		t.Error("a volume that stopped being servable left the node sending only changes, so nothing would repair it")
	}
}

// A volume server takes the options from every response it receives, and
// preallocate is a bare bool with no way to tell "off" from "not mentioned". A
// response that left it out would turn preallocation off until reconnect.
func TestHeartbeatResponsesCarryTheVolumeOptions(t *testing.T) {
	ms := &MasterServer{option: &MasterOption{VolumeSizeLimitMB: 1024}, preallocateSize: 1}

	resend := ms.heartbeatResponse()
	resend.ResendFullVolumeList = true

	if !resend.Preallocate {
		t.Error("a resend request would turn off preallocation on the volume server")
	}
	if resend.VolumeSizeLimit != 1024*1024*1024 {
		t.Errorf("a resend request carried volume size limit %d, want the configured one", resend.VolumeSizeLimit)
	}
}
