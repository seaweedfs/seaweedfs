package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// changedTierVolume returns a VolumeInformationMessage that mirrors
// changedTestVolume plus a remote-storage name, so a heartbeat can flip a
// replica's tier classification.
func changedTierVolume(id uint32, size uint64, remoteStorageName string) *master_pb.VolumeInformationMessage {
	v := changedTestVolume(id, size)
	v.RemoteStorageName = remoteStorageName
	return v
}

// announceChangedVolumes drives the message SendHeartbeat builds on a
// ChangedVolumes heartbeat through announceVolume, the routing the server
// itself uses, so the tests below assert what would really be broadcast
// without standing up a gRPC stream.
func announceChangedVolumes(topo *topology.Topology, dn *topology.DataNode, changed []*master_pb.VolumeInformationMessage) (newVids, remoteVids, readOnlyVids []uint32) {
	message := &master_pb.VolumeLocation{}
	for _, v := range topo.ApplyVolumeChanges(changed, dn) {
		announceVolume(message, uint32(v.Id), v.IsRemote(), v.ReadOnly, v.ReadOnlyCanDelete)
	}
	return message.NewVids, message.RemoteVids, message.ReadOnlyVids
}

// A replica that the heartbeat says has just been tiered to remote storage is
// still servable from the same node, but every connected client is holding
// stale DataInRemote=false and would prefer it over a real local replica. The
// master must rebroadcast it on RemoteVids so the wdclient replaces the entry.
func TestChangedVolumesAnnounceLocalToRemoteTierTransition(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024),
	}, dn)

	v := changedTierVolume(1, 1024, "s3-bucket")
	v.ReadOnly = true
	newVids, remoteVids, readOnlyVids := announceChangedVolumes(topo, dn, []*master_pb.VolumeInformationMessage{v})
	if !containsUint32(remoteVids, 1) {
		t.Errorf("a local-to-remote transition was not announced as RemoteVids: %v", remoteVids)
	}
	if !containsUint32(newVids, 1) {
		t.Errorf("a remote volume must stay on NewVids for clients that cannot read RemoteVids: %v", newVids)
	}
	if !containsUint32(readOnlyVids, 1) {
		t.Errorf("a read-only transition was not announced as ReadOnlyVids: %v", readOnlyVids)
	}
}

// A replica restored from remote storage back to a local disk flips the other
// way. The wdclient is currently demoting it behind same-DC remote replicas;
// the master must announce it on NewVids so the wdclient hoists it back to
// the front of the read order.
func TestChangedVolumesAnnounceRemoteToLocalTierTransition(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTierVolume(1, 1024, "s3-bucket"),
	}, dn)

	newVids, remoteVids, readOnlyVids := announceChangedVolumes(topo, dn, []*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024),
	})
	if !containsUint32(newVids, 1) {
		t.Errorf("a remote-to-local transition was not announced as NewVids: %v", newVids)
	}
	if containsUint32(remoteVids, 1) {
		t.Errorf("a remote-to-local transition was routed as RemoteVids: %v", remoteVids)
	}
	if containsUint32(readOnlyVids, 1) {
		t.Errorf("a writable transition was routed as ReadOnlyVids: %v", readOnlyVids)
	}
}

// A heartbeat that re-reports an existing replica with the same tier
// classification is not a change worth broadcasting. Volumes grow constantly
// and a steady stream of those would flood every client's bounded queue.
func TestChangedVolumesSuppressNoOpTierReport(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024),
	}, dn)

	newVids, remoteVids, readOnlyVids := announceChangedVolumes(topo, dn, []*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 4096),
	})
	if containsUint32(newVids, 1) || containsUint32(remoteVids, 1) || containsUint32(readOnlyVids, 1) {
		t.Errorf("a no-op tier report was broadcast: newVids=%v remoteVids=%v", newVids, remoteVids)
	}
}

func TestChangedVolumesAnnounceSameTierReadOnlyTransition(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{changedTestVolume(1, 1024)}, dn)

	readOnly := changedTestVolume(1, 1024)
	readOnly.ReadOnly = true
	_, _, readOnlyVids := announceChangedVolumes(topo, dn, []*master_pb.VolumeInformationMessage{readOnly})
	if !containsUint32(readOnlyVids, 1) {
		t.Fatalf("writable-to-read-only transition was not announced: %v", readOnlyVids)
	}
	_, _, readOnlyVids = announceChangedVolumes(topo, dn, []*master_pb.VolumeInformationMessage{changedTestVolume(1, 1024)})
	if containsUint32(readOnlyVids, 1) {
		t.Fatalf("read-only-to-writable transition retained ReadOnlyVids: %v", readOnlyVids)
	}
}

func TestChangedVolumesAnnounceReadOnlyDeleteCapabilityTransition(t *testing.T) {
	topo, dn := changedTestCluster(t)
	initial := changedTestVolume(1, 1024)
	initial.ReadOnly, initial.ReadOnlyCanDelete = true, true
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{initial}, dn)
	next := changedTestVolume(1, 1024)
	next.ReadOnly = true
	_, _, readOnlyVids := announceChangedVolumes(topo, dn, []*master_pb.VolumeInformationMessage{next})
	if !containsUint32(readOnlyVids, 1) {
		t.Fatalf("read-only delete-capability transition was not announced: %v", readOnlyVids)
	}
}

// A heartbeat that mixes a pure growth with a tier transition only announces
// the tier-transitioned replica: a growth is local state, not a re-route, and
// must not push other topology updates out of a bounded client queue.
func TestChangedVolumesAnnounceOnlyTierTransitions(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024),
		changedTestVolume(2, 1024),
	}, dn)

	newVids, remoteVids, _ := announceChangedVolumes(topo, dn, []*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 4096),              // pure growth
		changedTierVolume(2, 1024, "s3-bucket"), // tier transition
	})
	if containsUint32(newVids, 1) || containsUint32(remoteVids, 1) {
		t.Errorf("a volume that only grew was broadcast: newVids=%v remoteVids=%v", newVids, remoteVids)
	}
	if !containsUint32(remoteVids, 2) {
		t.Errorf("the tier-transitioned replica was missing from RemoteVids: newVids=%v remoteVids=%v", newVids, remoteVids)
	}
	if _, err := dn.GetVolumesById(needle.VolumeId(1)); err != nil {
		t.Errorf("a pure-growth heartbeat should not have removed the volume: %v", err)
	}
}

func containsUint32(haystack []uint32, needle uint32) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

// A full Volumes reconciliation (the digest mismatch recovery path) is the
// only way a re-tiered replica reaches the master without a separate
// ChangedVolumes heartbeat. The routing in master_grpc_server.go has to
// re-announce it on NewVids/RemoteVids or the wdclient keeps the stale
// DataInRemote classification forever.
func TestFullReconciliationAnnouncesTierTransition(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024),
	}, dn)

	v := changedTierVolume(1, 1024, "s3-bucket")
	v.ReadOnly = true
	newVids, remoteVids, readOnlyVids := announceFullReconciliation(topo, dn, []*master_pb.VolumeInformationMessage{v})
	if !containsUint32(remoteVids, 1) {
		t.Errorf("a tier transition reported in a full reconciliation was not announced: newVids=%v remoteVids=%v", newVids, remoteVids)
	}
	if !containsUint32(newVids, 1) {
		t.Errorf("a remote volume must stay on NewVids for clients that cannot read RemoteVids: %v", newVids)
	}
	if !containsUint32(readOnlyVids, 1) {
		t.Errorf("a read-only transition was not announced as ReadOnlyVids: %v", readOnlyVids)
	}
}

func TestFullReconciliationAnnounceSameTierReadOnlyTransition(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{changedTestVolume(1, 1024)}, dn)

	readOnly := changedTestVolume(1, 1024)
	readOnly.ReadOnly = true
	_, _, readOnlyVids := announceFullReconciliation(topo, dn, []*master_pb.VolumeInformationMessage{readOnly})
	if !containsUint32(readOnlyVids, 1) {
		t.Fatalf("writable-to-read-only reconciliation was not announced: %v", readOnlyVids)
	}
	_, _, readOnlyVids = announceFullReconciliation(topo, dn, []*master_pb.VolumeInformationMessage{changedTestVolume(1, 1024)})
	if containsUint32(readOnlyVids, 1) {
		t.Fatalf("read-only-to-writable reconciliation retained ReadOnlyVids: %v", readOnlyVids)
	}
}

func TestFullReconciliationAnnounceReadOnlyDeleteCapabilityTransition(t *testing.T) {
	topo, dn := changedTestCluster(t)
	initial := changedTestVolume(1, 1024)
	initial.ReadOnly, initial.ReadOnlyCanDelete = true, true
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{initial}, dn)
	next := changedTestVolume(1, 1024)
	next.ReadOnly = true
	_, _, readOnlyVids := announceFullReconciliation(topo, dn, []*master_pb.VolumeInformationMessage{next})
	if !containsUint32(readOnlyVids, 1) {
		t.Fatalf("read-only delete-capability reconciliation was not announced: %v", readOnlyVids)
	}
}

// announceFullReconciliation runs the same routing loop master_grpc_server's
// SendHeartbeat does on a full Volumes heartbeat, including the changed-set
// re-route added so digest-mismatch recovery propagates tier transitions.
func announceFullReconciliation(topo *topology.Topology, dn *topology.DataNode, volumes []*master_pb.VolumeInformationMessage) (newVids, remoteVids, readOnlyVids []uint32) {
	message := &master_pb.VolumeLocation{}
	newOnes, _, changedOnes := topo.SyncDataNodeRegistration(volumes, dn)
	for _, v := range append(newOnes, changedOnes...) {
		announceVolume(message, uint32(v.Id), v.IsRemote(), v.ReadOnly, v.ReadOnlyCanDelete)
	}
	return message.NewVids, message.RemoteVids, message.ReadOnlyVids
}
