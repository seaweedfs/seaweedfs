package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
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

// What lets the two sides be upgraded in either order.
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

func TestDigestCheckDoesNotReaskAfterAFullList(t *testing.T) {
	ms, dn := digestTestCluster(t)
	volumes := []*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1)}
	ms.Topo.SyncDataNodeRegistration(volumes, dn)

	wrong := dn.VolumeDigest() ^ 1
	if ms.checkVolumeDigest(&master_pb.Heartbeat{Volumes: volumes, VolumeDigest: &wrong}, dn) {
		t.Error("a full volume list that still disagreed was asked to resend, which would repeat forever")
	}
}

func TestDigestCheckAsksForTheListWhenADeltaDisagrees(t *testing.T) {
	ms, dn := digestTestCluster(t)
	ms.Topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{digestTestVolumeMessage(1)}, dn)

	wrong := dn.VolumeDigest() ^ 1
	if !ms.checkVolumeDigest(&master_pb.Heartbeat{VolumeDigest: &wrong}, dn) {
		t.Error("a disagreeing digest with no list to fall back on was not asked to resend")
	}
}

func TestDigestCheckSkipsNodesWithDuplicateVolumeIds(t *testing.T) {
	ms, dn := digestTestCluster(t)
	duplicated := digestTestVolumeMessage(1)
	duplicated.DiskId = 1
	ms.Topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		digestTestVolumeMessage(1), duplicated,
	}, dn)

	wrong := dn.VolumeDigest() ^ 1
	if ms.checkVolumeDigest(&master_pb.Heartbeat{VolumeDigest: &wrong}, dn) {
		t.Error("a node the master cannot represent was asked to resend, which would repeat forever")
	}
}
