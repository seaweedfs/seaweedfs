package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

func digestTestNode(t *testing.T) (*Topology, *DataNode) {
	t.Helper()
	topo := NewTopology("digest", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 1000})
	return topo, dn
}

func digestTestVolume(id uint32) *master_pb.VolumeInformationMessage {
	return &master_pb.VolumeInformationMessage{
		Id:               id,
		Size:             1024 * 1024,
		Collection:       "c",
		FileCount:        10,
		DeleteCount:      1,
		DeletedByteCount: 128,
		ReplicaPlacement: 0,
		Version:          3,
		CompactRevision:  1,
		ModifiedAtSecond: 1700000000,
	}
}

func TestVolumeDigestIsStableAcrossRepeatedHeartbeats(t *testing.T) {
	topo, dn := digestTestNode(t)
	volumes := []*master_pb.VolumeInformationMessage{digestTestVolume(1), digestTestVolume(2), digestTestVolume(3)}

	topo.SyncDataNodeRegistration(volumes, dn)
	first := dn.VolumeDigest()
	if first == 0 {
		t.Fatal("expected a non-zero digest for a node holding volumes")
	}
	for i := 0; i < 3; i++ {
		topo.SyncDataNodeRegistration(volumes, dn)
		if got := dn.VolumeDigest(); got != first {
			t.Fatalf("heartbeat %d changed the digest with no change to report: %d != %d", i, got, first)
		}
	}
}

func TestVolumeDigestIsIndependentOfReportOrder(t *testing.T) {
	topoA, dnA := digestTestNode(t)
	topoA.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		digestTestVolume(1), digestTestVolume(2), digestTestVolume(3),
	}, dnA)

	topoB, dnB := digestTestNode(t)
	topoB.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		digestTestVolume(3), digestTestVolume(1), digestTestVolume(2),
	}, dnB)

	if dnA.VolumeDigest() != dnB.VolumeDigest() {
		t.Errorf("digest depends on report order: %d != %d", dnA.VolumeDigest(), dnB.VolumeDigest())
	}
}

func TestVolumeDigestTracksEveryReportedField(t *testing.T) {
	base := digestTestVolume(1)
	mutations := map[string]func(*master_pb.VolumeInformationMessage){
		"Id":                func(m *master_pb.VolumeInformationMessage) { m.Id = 2 },
		"Size":              func(m *master_pb.VolumeInformationMessage) { m.Size++ },
		"Collection":        func(m *master_pb.VolumeInformationMessage) { m.Collection = "other" },
		"FileCount":         func(m *master_pb.VolumeInformationMessage) { m.FileCount++ },
		"DeleteCount":       func(m *master_pb.VolumeInformationMessage) { m.DeleteCount++ },
		"DeletedByteCount":  func(m *master_pb.VolumeInformationMessage) { m.DeletedByteCount++ },
		"ReadOnly":          func(m *master_pb.VolumeInformationMessage) { m.ReadOnly = true },
		"ReplicaPlacement":  func(m *master_pb.VolumeInformationMessage) { m.ReplicaPlacement = 10 },
		"Version":           func(m *master_pb.VolumeInformationMessage) { m.Version = 2 },
		"Ttl":               func(m *master_pb.VolumeInformationMessage) { m.Ttl = 3 << 8 },
		"CompactRevision":   func(m *master_pb.VolumeInformationMessage) { m.CompactRevision++ },
		"ModifiedAtSecond":  func(m *master_pb.VolumeInformationMessage) { m.ModifiedAtSecond++ },
		"RemoteStorageName": func(m *master_pb.VolumeInformationMessage) { m.RemoteStorageName = "s3" },
		"RemoteStorageKey":  func(m *master_pb.VolumeInformationMessage) { m.RemoteStorageKey = "k" },
		"DiskType":          func(m *master_pb.VolumeInformationMessage) { m.DiskType = "ssd" },
		"DiskId":            func(m *master_pb.VolumeInformationMessage) { m.DiskId = 1 },
	}

	baseInfo, err := storage.NewVolumeInfo(base)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := digestTestVolume(1)
			mutate(changed)
			changedInfo, err := storage.NewVolumeInfo(changed)
			if err != nil {
				t.Fatal(err)
			}
			if baseInfo.ReportHash() == changedInfo.ReportHash() {
				t.Errorf("a change to %s is invisible to the digest, so the master would never be told about it", name)
			}
		})
	}
}

func TestVolumeDigestFollowsVolumeChanges(t *testing.T) {
	topo, dn := digestTestNode(t)
	volumes := []*master_pb.VolumeInformationMessage{digestTestVolume(1), digestTestVolume(2)}
	topo.SyncDataNodeRegistration(volumes, dn)
	original := dn.VolumeDigest()

	grown := []*master_pb.VolumeInformationMessage{digestTestVolume(1), digestTestVolume(2)}
	grown[1].Size += 4096
	topo.SyncDataNodeRegistration(grown, dn)
	if dn.VolumeDigest() == original {
		t.Error("a volume that grew left the digest unchanged")
	}

	topo.SyncDataNodeRegistration(volumes, dn)
	if dn.VolumeDigest() != original {
		t.Error("reverting a volume did not restore the digest")
	}

	topo.SyncDataNodeRegistration(volumes[:1], dn)
	if dn.VolumeDigest() == original {
		t.Error("dropping a volume left the digest unchanged")
	}

	topo.SyncDataNodeRegistration(volumes, dn)
	if dn.VolumeDigest() != original {
		t.Error("restoring a dropped volume did not restore the digest")
	}
}

func TestVolumeDigestEmptiesWithTheNode(t *testing.T) {
	topo, dn := digestTestNode(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		digestTestVolume(1), digestTestVolume(2),
	}, dn)

	topo.SyncDataNodeRegistration(nil, dn)
	if got := dn.VolumeDigest(); got != 0 {
		t.Errorf("expected an empty node to digest to 0, got %d", got)
	}
}

func TestVolumeDigestFollowsDeltaRegistration(t *testing.T) {
	topo, dn := digestTestNode(t)
	full := []*master_pb.VolumeInformationMessage{digestTestVolume(1), digestTestVolume(2)}
	topo.SyncDataNodeRegistration(full, dn)
	both := dn.VolumeDigest()

	topo.IncrementalSyncDataNodeRegistration(nil, []*master_pb.VolumeShortInformationMessage{{Id: 2}}, dn)
	if dn.VolumeDigest() == both {
		t.Error("unmounting a volume left the digest unchanged")
	}

	topo.SyncDataNodeRegistration(full, dn)
	if dn.VolumeDigest() != both {
		t.Error("a full heartbeat did not restore the digest after an unmount")
	}
}

// The point of the digest is not to detect that volumes changed -- in any live
// cluster some always have. It is to confirm that after applying the changes a
// heartbeat did carry, the master holds what the volume server holds. So a
// heartbeat reporting only the volumes that moved must still reconcile.
func TestVolumeDigestMatchesAfterApplyingOnlyChangedVolumes(t *testing.T) {
	const total = 50
	full := make([]*master_pb.VolumeInformationMessage, 0, total)
	for i := 1; i <= total; i++ {
		v := digestTestVolume(uint32(i))
		v.ReadOnly = i > 5 // only the first few are writable, as in a tiered cluster
		full = append(full, v)
	}

	topo, dn := digestTestNode(t)
	topo.SyncDataNodeRegistration(full, dn)

	// Three writable volumes take writes between two heartbeats.
	changed := make([]storage.VolumeInfo, 0, 3)
	for _, v := range full[:3] {
		v.Size += 4096
		v.FileCount++
		v.ModifiedAtSecond += 5
		vi, err := storage.NewVolumeInfo(v)
		if err != nil {
			t.Fatal(err)
		}
		changed = append(changed, vi)
	}

	// What the volume server would now report for its whole set.
	reference, referenceNode := digestTestNode(t)
	reference.SyncDataNodeRegistration(full, referenceNode)
	want := referenceNode.VolumeDigest()

	if dn.VolumeDigest() == want {
		t.Fatal("expected the master to be behind before the changes are applied")
	}

	// The heartbeat carries three volumes, not fifty.
	dn.DeltaUpdateVolumes(changed, nil)

	if got := dn.VolumeDigest(); got != want {
		t.Errorf("digest still disagrees after applying the reported changes: %d != %d", got, want)
	}
}

// A volume that disappears without a delta is exactly what the full list exists
// to catch, and is the case the digest has to keep catching.
func TestVolumeDigestCatchesASilentlyLostVolume(t *testing.T) {
	full := []*master_pb.VolumeInformationMessage{
		digestTestVolume(1), digestTestVolume(2), digestTestVolume(3),
	}

	topo, dn := digestTestNode(t)
	topo.SyncDataNodeRegistration(full, dn)

	// The volume server no longer has volume 2 and never got to say so.
	reference, referenceNode := digestTestNode(t)
	reference.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{full[0], full[2]}, referenceNode)

	if dn.VolumeDigest() == referenceNode.VolumeDigest() {
		t.Error("a volume lost without a delta went undetected, which is what the full list is for")
	}
}
