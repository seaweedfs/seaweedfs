package topology

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func isWritable(t *testing.T, topo *Topology, v storage.VolumeInfo) bool {
	t.Helper()
	vl := topo.FindVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, types.ToDiskType(v.DiskType))
	for _, id := range vl.CloneWritableVolumes() {
		if id == v.Id {
			return true
		}
	}
	return false
}

func TestDuplicateVolumeIdAcrossCollections(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")

	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}

	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v2, dn2)
	topo.flagDuplicateVolumeId(v2, dn2)

	if isWritable(t, topo, v2) {
		t.Fatal("duplicate newcomer should be quarantined out of the writable list")
	}
	if isWritable(t, topo, v1) {
		t.Fatal("either copy could be the impostor; the earlier copy must be quarantined too")
	}
}

func TestSameLayoutReplicaIsNotDuplicate(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("001")

	dn1 := NewDataNode("server1")
	dn1.Ip = "10.0.0.1"
	dn1.Port = 8080
	dn2 := NewDataNode("server2")
	dn2.Ip = "10.0.0.2"
	dn2.Port = 8080

	v := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v})
	dn2.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn1)
	topo.RegisterVolumeLayout(v, dn2)
	topo.flagDuplicateVolumeId(v, dn2)

	if !isWritable(t, topo, v) {
		t.Fatal("a same-layout replica is a legitimate copy, not a duplicate")
	}
}

func TestSameNodeVolumeMoveIsNotDuplicate(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	dn := NewDataNode("server1")

	old := storage.VolumeInfo{Id: 7, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	moved := storage.VolumeInfo{Id: 7, Collection: "c", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: "ssd"}
	dn.UpdateVolumes([]storage.VolumeInfo{old})
	topo.RegisterVolumeLayout(old, dn)

	// the move registers the new layout before the old entry is removed
	dn.UpdateVolumes([]storage.VolumeInfo{moved})
	topo.RegisterVolumeLayout(moved, dn)
	topo.flagDuplicateVolumeId(moved, dn)

	if !isWritable(t, topo, moved) {
		t.Fatal("a same-node disk move is not a duplicate")
	}
}

func TestQuarantineReleasedWhenConflictResolves(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")
	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v2, dn2)
	topo.flagDuplicateVolumeId(v2, dn2)

	topo.UnRegisterVolumeLayout(v2, dn2)

	// Registration flags duplicates asynchronously, so a flag that was still
	// in flight when the conflict resolved releases the surviving copy only
	// once it settles. Wait for that release instead of reading once.
	deadline := time.Now().Add(5 * time.Second)
	for !isWritable(t, topo, v1) {
		if time.Now().After(deadline) {
			t.Fatal("the surviving copy must be released once the conflict is gone")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFailedReleasePushRestoresQuarantineMark(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")

	var pushFails atomic.Bool
	topo.QuarantineVolumeFn = func(dn *DataNode, vid needle.VolumeId, readOnly bool) error {
		if !readOnly && pushFails.Load() {
			return errors.New("volume server unreachable")
		}
		return nil
	}

	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v2, dn2)
	// Registrations flag duplicates asynchronously; let them settle so no
	// late mark can land after the release assertions below.
	topo.duplicateFlagChecks.Wait()
	topo.flagDuplicateVolumeId(v2, dn2)

	// the conflict resolves while the surviving copy is unreachable: the
	// writable push fails, so the record and the layout read-only mark must
	// both come back instead of advertising a writable replica the server
	// still holds read-only
	pushFails.Store(true)
	topo.UnRegisterVolumeLayout(v2, dn2)

	if !topo.isQuarantined(dn1, v1.Id) {
		t.Fatal("a failed writable push must keep the quarantine record for the retry")
	}
	if isWritable(t, topo, v1) {
		t.Fatal("the layout must not advertise a replica the server still holds read-only")
	}

	pushFails.Store(false)
	// An idle replica drops out of the delta reports once its stored flag
	// matches the server's, so the release pass retries off the leader pulse
	// rather than waiting for the volume to be reported again.
	topo.retryPendingQuarantineReleases()
	require.Eventually(t, func() bool {
		return !topo.isQuarantined(dn1, v1.Id) && isWritable(t, topo, v1)
	}, 5*time.Second, time.Millisecond, "the pending release must retry without another volume report")
}

func TestQuarantineKeptWhenReplicaUnreadable(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")

	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v2, dn2)
	topo.duplicateFlagChecks.Wait()
	topo.flagDuplicateVolumeId(v2, dn2)

	// the surviving copy drops out of its node's disk map while the lookup
	// index still holds it; its layout key is then unknown, so the release
	// pass must keep the mark rather than assume the conflict resolved
	dn1.UpdateVolumes(nil)
	topo.UnRegisterVolumeLayout(v2, dn2)

	if !topo.isQuarantined(dn1, v1.Id) {
		t.Fatal("an unreadable replica must not lose its quarantine record")
	}
	if isWritable(t, topo, v1) {
		t.Fatal("a replica whose conflict cannot be ruled out must stay read-only")
	}

	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.duplicateFlagChecks.Wait()
	if !isWritable(t, topo, v1) {
		t.Fatal("once readable again and sole, the copy must be released to writable")
	}
}

func TestStaleDuplicateReportDoesNotQuarantineSurvivor(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")
	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	topo.RegisterVolumeLayout(v1, dn1)

	// dn2's copy is gone before its delayed duplicate check runs; the report
	// still names dn2 but its layout no longer holds it.
	topo.flagDuplicateVolumeId(v2, dn2)

	if topo.isQuarantined(dn1, v1.Id) || topo.isQuarantined(dn2, v2.Id) {
		t.Fatal("a report for a copy that is already gone must not quarantine anything")
	}
	if !isWritable(t, topo, v1) {
		t.Fatal("the surviving copy must stay writable")
	}
}

func TestQuarantineSurvivesResync(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")
	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v2, dn2)
	topo.flagDuplicateVolumeId(v2, dn2)

	// a resync from the same server re-reports the volume as writable;
	// the quarantine must keep it out of the writable list
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{{
		Id: 7, Collection: "b", ReplicaPlacement: 0, Version: 3,
	}}, dn2)

	if isWritable(t, topo, v2) {
		t.Fatal("resync must not clear the quarantine")
	}
}

func TestQuarantineSurvivesReplicaRejoin(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("001")

	dn1 := NewDataNode("server1")
	dn1.Ip = "10.0.0.1"
	dn1.Port = 8080
	dn2 := NewDataNode("server2")
	dn2.Ip = "10.0.0.2"
	dn2.Port = 8080
	dn3 := NewDataNode("server3")
	dn3.Ip = "10.0.0.3"
	dn3.Port = 8080

	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v1})
	dn3.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v1, dn2)
	topo.RegisterVolumeLayout(v2, dn3)
	topo.flagDuplicateVolumeId(v2, dn3)

	// a replica dropped from the lookup index re-registers through the
	// self-heal path; rebuilding location flags from reported state must
	// not lift the quarantine on any copy
	vl := topo.FindVolumeLayout(v1.Collection, v1.ReplicaPlacement, v1.Ttl, types.ToDiskType(v1.DiskType))
	vl.SetVolumeUnavailable(dn1, v1.Id)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{{
		Id: 7, Collection: "a", ReplicaPlacement: uint32(rp.Byte()), Version: 3,
	}}, dn1)

	if isWritable(t, topo, v1) {
		t.Fatal("a re-registering replica must not clear the quarantine on the surviving copies")
	}
	if isWritable(t, topo, v2) {
		t.Fatal("the conflicting copy must stay quarantined too")
	}
}

func TestReplicationChangeIsNotDuplicate(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rpOld, _ := super_block.NewReplicaPlacementFromString("000")
	rpNew, _ := super_block.NewReplicaPlacementFromString("001")

	dn1 := NewDataNode("server1")
	dn1.Ip = "10.0.0.1"
	dn1.Port = 8080
	dn2 := NewDataNode("server2")
	dn2.Ip = "10.0.0.2"
	dn2.Port = 8080

	vOld := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rpOld, Ttl: needle.EMPTY_TTL}
	vNew := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rpNew, Ttl: needle.EMPTY_TTL}

	dn1.UpdateVolumes([]storage.VolumeInfo{vOld})
	dn2.UpdateVolumes([]storage.VolumeInfo{vNew})
	topo.RegisterVolumeLayout(vOld, dn1)
	topo.RegisterVolumeLayout(vNew, dn2)
	topo.flagDuplicateVolumeId(vNew, dn2)

	if topo.isQuarantined(dn2, vNew.Id) || topo.isQuarantined(dn1, vOld.Id) {
		t.Fatal("an rp-only layout difference is a replication change in flight, not a duplicate")
	}
}

func TestDuplicateVolumeIdAcrossTtls(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	ttl, _ := needle.ReadTTL("3d")

	dn1 := NewDataNode("server1")
	dn1.Ip = "10.0.0.1"
	dn1.Port = 8080
	dn2 := NewDataNode("server2")
	dn2.Ip = "10.0.0.2"
	dn2.Port = 8080

	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: ttl}

	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	topo.RegisterVolumeLayout(v1, dn1)
	topo.RegisterVolumeLayout(v2, dn2)
	topo.flagDuplicateVolumeId(v2, dn2)

	if !topo.isQuarantined(dn2, v2.Id) || !topo.isQuarantined(dn1, v1.Id) {
		t.Fatal("same collection with a different ttl is still a reused volume id")
	}
}

func TestFailedWritablePushRetriedOnNextReport(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	var failWritable atomic.Bool
	var writablePushes atomic.Int32
	topo.QuarantineVolumeFn = func(dn *DataNode, vid needle.VolumeId, readOnly bool) error {
		if !readOnly {
			writablePushes.Add(1)
			if failWritable.Load() {
				return errors.New("volume server unreachable")
			}
		}
		return nil
	}

	dn1 := NewDataNode("server1")
	dn2 := NewDataNode("server2")
	v1 := storage.VolumeInfo{Id: 7, Collection: "a", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	v2 := storage.VolumeInfo{Id: 7, Collection: "b", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL}
	dn1.UpdateVolumes([]storage.VolumeInfo{v1})
	dn2.UpdateVolumes([]storage.VolumeInfo{v2})
	// Register on the layouts directly: RegisterVolumeLayout would spawn an
	// async flagDuplicateVolumeId per call, and a delayed one can re-quarantine
	// the survivor at any later point in this test.
	topo.GetVolumeLayout(v1.Collection, v1.ReplicaPlacement, v1.Ttl, types.ToDiskType(v1.DiskType)).RegisterVolume(&v1, dn1, nil)
	topo.GetVolumeLayout(v2.Collection, v2.ReplicaPlacement, v2.Ttl, types.ToDiskType(v2.DiskType)).RegisterVolume(&v2, dn2, nil)
	topo.flagDuplicateVolumeId(v2, dn2)

	// the conflict resolves while the survivor's writable push cannot land
	failWritable.Store(true)
	topo.UnRegisterVolumeLayout(v2, dn2)
	require.Eventually(t, func() bool {
		// The record reappears only after the failed writable push, and the
		// restore arms it for retry in the same store - so this pair means the
		// next report will take the retry branch rather than observing the
		// release mid-flight.
		return writablePushes.Load() >= 1 && topo.isQuarantined(dn1, v1.Id)
	}, 5*time.Second, time.Millisecond, "a failed writable push must re-arm the record")

	reported := v1
	reported.ReadOnly = true
	failWritable.Store(false)
	topo.enforceQuarantine(dn1, reported)

	require.Eventually(t, func() bool {
		// The release clears the record and the layout mark before the remote
		// push lands, so the count has to be part of the condition or the poll
		// can pass before the retried push runs.
		return !topo.isQuarantined(dn1, v1.Id) && isWritable(t, topo, v1) && writablePushes.Load() >= 2
	}, 5*time.Second, time.Millisecond, "the next volume report must retry the release and its writable push")
}
