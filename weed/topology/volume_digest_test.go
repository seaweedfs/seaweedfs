package topology

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

// Every field of VolumeInformationMessage has to reach the digest: one the hash
// skips is a change the master would never be told about. Enumerated from the
// message rather than listed here, so a field added later cannot quietly fall
// outside the digest while this still passes.
// Fields a server reports that the master deliberately does not keep, so a
// change to one alters nothing it holds and the digest is right not to move.
var notRetainedByTheMaster = map[string]string{
	"remote_storage_key": "unique per volume, and the master decides nothing from it",
}

func TestVolumeDigestTracksEveryReportedField(t *testing.T) {
	base := digestTestVolume(1)
	baseInfo, err := storage.NewVolumeInfo(base)
	if err != nil {
		t.Fatal(err)
	}

	fields := base.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		t.Run(string(fd.Name()), func(t *testing.T) {
			why, skipped := notRetainedByTheMaster[string(fd.Name())]
			candidates := distinctValuesFor(t, fd, base.ProtoReflect().Get(fd))
			for _, candidate := range candidates {
				changed := proto.Clone(base).(*master_pb.VolumeInformationMessage)
				changed.ProtoReflect().Set(fd, candidate)
				changedInfo, err := storage.NewVolumeInfo(changed)
				if err != nil {
					t.Fatal(err)
				}
				if baseInfo.ReportHash() != changedInfo.ReportHash() {
					if skipped {
						t.Errorf("%s moves the digest, but is listed as not retained by the master (%s)", fd.Name(), why)
					}
					return
				}
			}
			if skipped {
				return
			}
			t.Errorf("no change to %s moves the digest, so the master would never be told about one", fd.Name())
		})
	}
}

// distinctValuesFor offers values that differ from current. Several, because
// some fields are narrowed or normalised on the way into VolumeInfo and the
// smallest change to the wire value can land back on the stored one.
func distinctValuesFor(t *testing.T, fd protoreflect.FieldDescriptor, current protoreflect.Value) []protoreflect.Value {
	t.Helper()
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return []protoreflect.Value{protoreflect.ValueOfBool(!current.Bool())}
	case protoreflect.Uint32Kind:
		return []protoreflect.Value{
			protoreflect.ValueOfUint32(uint32(current.Uint()) + 1),
			protoreflect.ValueOfUint32(uint32(current.Uint()) + 1<<8),
			protoreflect.ValueOfUint32(uint32(current.Uint()) + 1<<16),
		}
	case protoreflect.Uint64Kind:
		return []protoreflect.Value{protoreflect.ValueOfUint64(current.Uint() + 1)}
	case protoreflect.Int64Kind:
		return []protoreflect.Value{protoreflect.ValueOfInt64(current.Int() + 1)}
	case protoreflect.StringKind:
		return []protoreflect.Value{protoreflect.ValueOfString(current.String() + "x")}
	}
	t.Fatalf("field %s has kind %s, which this test does not know how to vary", fd.Name(), fd.Kind())
	return nil
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

// The disk map and the lookup index are maintained separately, and a disconnect
// racing a reconnect has been seen to drop a volume from the lookup index while
// leaving it on the node. The volume server's report is identical either way, so
// the heartbeat digest cannot see it and the master has to notice on its own.
func TestVolumeIndexDigestSeesLookupDivergence(t *testing.T) {
	topo, dn := digestTestNode(t)
	v := digestTestVolume(1)
	v.Collection = "drr"
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{v}, dn)

	if !dn.HasConsistentVolumeIndex() {
		t.Fatal("a freshly registered node should have a consistent index")
	}
	reported := dn.VolumeDigest()

	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("drr", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.SetVolumeUnavailable(dn, needle.VolumeId(1))

	if got := topo.Lookup("drr", needle.VolumeId(1)); got != nil {
		t.Fatalf("expected the volume to have become unservable, got %v", got)
	}
	if _, err := dn.GetVolumesById(needle.VolumeId(1)); err != nil {
		t.Fatalf("the volume should still be on the node: %v", err)
	}
	if dn.VolumeDigest() != reported {
		t.Error("the reported digest should not move: the volume server sees no change")
	}
	if dn.HasConsistentVolumeIndex() {
		t.Error("a volume held but not servable left the index digests agreeing, so nothing would repair it")
	}

	// The full heartbeat self-heal puts it back.
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{v}, dn)
	if !dn.HasConsistentVolumeIndex() {
		t.Error("the self-heal did not restore index consistency")
	}
}

func TestVolumeIndexDigestFollowsNodeLifecycle(t *testing.T) {
	topo, dn := digestTestNode(t)
	full := []*master_pb.VolumeInformationMessage{digestTestVolume(1), digestTestVolume(2), digestTestVolume(3)}
	topo.SyncDataNodeRegistration(full, dn)
	if !dn.HasConsistentVolumeIndex() {
		t.Fatal("registration left the indexes disagreeing")
	}

	topo.SyncDataNodeRegistration(full[:2], dn)
	if !dn.HasConsistentVolumeIndex() {
		t.Error("dropping a volume left the indexes disagreeing")
	}

	topo.IncrementalSyncDataNodeRegistration(
		[]*master_pb.VolumeShortInformationMessage{{Id: 9}}, nil, dn)
	if !dn.HasConsistentVolumeIndex() {
		t.Error("a mount delta left the indexes disagreeing")
	}

	topo.IncrementalSyncDataNodeRegistration(
		nil, []*master_pb.VolumeShortInformationMessage{{Id: 9}}, dn)
	if !dn.HasConsistentVolumeIndex() {
		t.Error("an unmount delta left the indexes disagreeing")
	}

	topo.UnRegisterDataNode(dn)
	held, servable := dn.VolumeIndexDigests()
	if held != 0 || servable != 0 {
		t.Errorf("an unregistered node should hold nothing: held=%d servable=%d", held, servable)
	}
}

// A volume id mounted on two disks of one server is reported twice with
// different disk ids, but the master keys volumes by id alone, so it keeps only
// one copy and its digest can never equal the server's. Resending the full list
// cannot fix that, so the node has to be excluded from digest comparison
// entirely rather than resend forever.
func TestVolumeDigestRefusesDuplicateVolumeIds(t *testing.T) {
	topo, dn := digestTestNode(t)

	first := digestTestVolume(1)
	second := digestTestVolume(1)
	second.DiskId = 1
	second.Size = first.Size * 2

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{first, second}, dn)
	if !dn.HasDuplicateVolumeIds() {
		t.Fatal("a volume id reported twice went unnoticed, so the digest would be trusted and never reconcile")
	}

	firstInfo, err := storage.NewVolumeInfo(first)
	if err != nil {
		t.Fatal(err)
	}
	secondInfo, err := storage.NewVolumeInfo(second)
	if err != nil {
		t.Fatal(err)
	}
	if dn.VolumeDigest() == firstInfo.ReportHash()^secondInfo.ReportHash() {
		t.Error("expected the master to be unable to represent both copies; if it now can, the guard is no longer needed")
	}

	// Once the stale twin is gone the node is comparable again.
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{first}, dn)
	if dn.HasDuplicateVolumeIds() {
		t.Error("the node stayed marked as duplicated after reporting a clean list")
	}
	if dn.VolumeDigest() != firstInfo.ReportHash() {
		t.Error("digest did not settle on the surviving copy")
	}
}

// Two volume servers can hold one address. GetOrCreateDataNode keys on the id a
// server reports and deliberately refuses to merge a new id onto an address an
// older node still claims, while the lookup list keys on address alone -- so
// registering the second server displaces the first from the lookup entry
// without either node being told. The digest has to follow the entry, not the
// node that was passed in.
func addressSharingNodes(t *testing.T) (*Topology, *DataNode, *DataNode) {
	t.Helper()
	topo := NewTopology("digest", nil, 32*1024*1024*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	counts := map[string]uint32{"": 1000}
	old := rack.GetOrCreateDataNode("10.1.2.3", 8080, 18080, "", "n1", counts)
	fresh := rack.GetOrCreateDataNode("10.1.2.3", 8080, 18080, "", "n2", counts)
	if old == fresh {
		t.Skip("address reuse no longer produces two nodes")
	}
	return topo, old, fresh
}

func TestVolumeIndexDigestFollowsDisplacedLookupEntry(t *testing.T) {
	topo, old, fresh := addressSharingNodes(t)
	v := digestTestVolume(1)

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{v}, old)
	if !old.HasConsistentVolumeIndex() {
		t.Fatal("the first node should be consistent before it is displaced")
	}

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{v}, fresh)

	servable := map[*DataNode]bool{}
	for _, dn := range topo.Lookup("", needle.VolumeId(1)) {
		servable[dn] = true
	}
	if servable[old] || !servable[fresh] {
		t.Fatalf("expected the lookup entry to move to the new node, got old=%v fresh=%v",
			servable[old], servable[fresh])
	}

	if old.HasConsistentVolumeIndex() {
		t.Error("the displaced node still holds the volume and can no longer serve it, so its index must read as inconsistent")
	}
	if !fresh.HasConsistentVolumeIndex() {
		t.Error("the node the lookup entry now names reads as inconsistent")
	}
}

func TestVolumeIndexDigestFollowsRemovedLookupEntry(t *testing.T) {
	topo, old, fresh := addressSharingNodes(t)
	v := digestTestVolume(1)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{v}, old)

	// fresh shares old's address, so unregistering through it removes old's
	// lookup entry. The digest must come off the node that was actually removed.
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.SetVolumeUnavailable(fresh, needle.VolumeId(1))

	if got := topo.Lookup("c", needle.VolumeId(1)); got != nil {
		t.Fatalf("expected the lookup entry to be gone, got %v", got)
	}
	if old.HasConsistentVolumeIndex() {
		t.Error("the removed node still holds the volume and cannot serve it, so its index must read as inconsistent")
	}
	if _, servable := fresh.VolumeIndexDigests(); servable != 0 {
		t.Error("the node that was merely passed in should never have gained the entry")
	}
}

// The two ends must agree on real heartbeat data, not just on hand-built
// messages: the volume server hashes what it is about to send, the master
// hashes what it stored from it.
func TestMasterDigestMatchesWhatAVolumeServerReports(t *testing.T) {
	dir := t.TempDir()
	loc := storage.NewDiskLocation(dir, 100, util.MinFreeSpace{}, "", types.HardDriveType, nil,
		stats.DefaultDiskIOProbeConfig())
	for _, vid := range []needle.VolumeId{1, 2, 3} {
		v, err := storage.NewVolume(loc.Directory, loc.IdxDirectory, "", vid, storage.NeedleMapInMemory,
			&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		loc.SetVolume(vid, v)
	}

	reported := make([]*master_pb.VolumeInformationMessage, 0, 3)
	var serverDigest uint64
	for _, vid := range []needle.VolumeId{1, 2, 3} {
		v, _ := loc.FindVolume(vid)
		_, m := v.ToVolumeInformationMessage()
		if m == nil {
			t.Fatalf("volume %d reported nothing", vid)
		}
		vi, err := storage.NewVolumeInfo(m)
		if err != nil {
			t.Fatal(err)
		}
		serverDigest ^= vi.ReportHash()
		reported = append(reported, m)
	}

	topo, dn := digestTestNode(t)
	topo.SyncDataNodeRegistration(reported, dn)

	if got := dn.VolumeDigest(); got != serverDigest {
		t.Errorf("master digest %d does not match the reporting server's %d", got, serverDigest)
	}
}
