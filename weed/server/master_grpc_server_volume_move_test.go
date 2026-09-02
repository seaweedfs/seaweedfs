package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func moveTestNode(t *testing.T) (*topology.Topology, *topology.DataNode) {
	t.Helper()
	topo := topology.NewTopology("test", sequence.NewMemorySequencer(), 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100, "ssd": 100})
	return topo, dn
}

func moveTestVolume(diskType string, diskId uint32) *master_pb.VolumeInformationMessage {
	return &master_pb.VolumeInformationMessage{
		Id: 1, Size: 1024, Collection: "c", Version: 3, DiskType: diskType, DiskId: diskId,
	}
}

// Clients apply additions before deletions, so a move reported as both would
// leave them with no location for a volume that never went anywhere.
func TestVolumeMovedBetweenDisksIsNotBroadcastAsRemoved(t *testing.T) {
	topo, dn := moveTestNode(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{moveTestVolume("", 0)}, dn)

	_, deleted, _ := topo.SyncDataNodeRegistration(
		[]*master_pb.VolumeInformationMessage{moveTestVolume("ssd", 1)}, dn)

	if len(deleted) != 1 {
		t.Fatalf("expected the move to remove the volume from the disk it left, got %d removals", len(deleted))
	}
	if shouldBroadcastVolumeRemoval(dn, needle.VolumeId(1)) {
		t.Error("clients would be told a volume left a node that still has it")
	}
}

func TestVolumeGoneFromTheNodeIsBroadcastAsRemoved(t *testing.T) {
	topo, dn := moveTestNode(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{moveTestVolume("", 0)}, dn)

	if _, deleted, _ := topo.SyncDataNodeRegistration(nil, dn); len(deleted) != 1 {
		t.Fatalf("expected the volume to be removed, got %d removals", len(deleted))
	}
	if !shouldBroadcastVolumeRemoval(dn, needle.VolumeId(1)) {
		t.Error("clients were not told about a volume that really did leave the node")
	}
}

func TestVolumeRemountedOnAnotherDiskIsNotBroadcastAsRemoved(t *testing.T) {
	topo, dn := moveTestNode(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{moveTestVolume("", 0)}, dn)

	topo.IncrementalSyncDataNodeRegistration(
		[]*master_pb.VolumeShortInformationMessage{{Id: 1, Collection: "c", DiskType: "ssd"}},
		[]*master_pb.VolumeShortInformationMessage{{Id: 1, Collection: "c", DiskType: ""}},
		dn)

	if shouldBroadcastVolumeRemoval(dn, needle.VolumeId(1)) {
		t.Error("clients would be told a volume left a node that still has it on another disk")
	}
}

// Fails if the topology update stops running before the removals are judged.
func TestVolumeUnmountedViaDeltaIsBroadcastAsRemoved(t *testing.T) {
	topo, dn := moveTestNode(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{moveTestVolume("", 0)}, dn)

	topo.IncrementalSyncDataNodeRegistration(nil,
		[]*master_pb.VolumeShortInformationMessage{{Id: 1, Collection: "c", DiskType: ""}}, dn)

	if !shouldBroadcastVolumeRemoval(dn, needle.VolumeId(1)) {
		t.Error("clients were not told about a volume that really was unmounted")
	}
}

// Clients track normal and ec locations separately, and prefer the normal one
// when both come from the same generation. A replica that became ec shards has
// genuinely left, so the removal must still go out.
func TestVolumeReplacedByEcShardsIsBroadcastAsRemoved(t *testing.T) {
	topo, dn := moveTestNode(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{moveTestVolume("", 0)}, dn)
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 1, Collection: "c", EcIndexBits: 0x3fff},
	}, dn)

	if _, deleted, _ := topo.SyncDataNodeRegistration(nil, dn); len(deleted) != 1 {
		t.Fatalf("expected the normal volume to be removed, got %d removals", len(deleted))
	}
	if !shouldBroadcastVolumeRemoval(dn, needle.VolumeId(1)) {
		t.Error("clients kept a normal-volume location for a replica that became ec shards")
	}
}
