package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A volume that grew past the limit keeps bouncing between writable and
// unwritable when the oversized mark is not refreshed on heartbeat: RegisterVolume
// sets it once, but the delta heartbeat path (ApplyVolumeChanges) never re-sets
// it, so ensureCorrectWritables sees a stale "not oversized" mark and re-adds
// the volume to writables on the next heartbeat, while RecordAssign removes it
// on the next assign. UpdateOversizedState must keep the mark current.
func TestUpdateOversizedStateKeepsOversizedVolumeUnwritable(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":4000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// Grow the volume past the limit, as RecordAssign would see it.
	if !vl.RecordAssign(1, 9000) {
		t.Fatalf("RecordAssign should report the volume reached capacity")
	}
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected 0 writable after RecordAssign, got %d", w)
	}

	// A heartbeat that reports the now-oversized volume must refresh the mark
	// so ensureCorrectWritables does not re-add it to writables. Mirrors the
	// heartbeat order: refresh, then decay, then correct.
	dn := vl.vid2location[1].list[0]
	vi, err := dn.GetVolumesById(1)
	if err != nil {
		t.Fatalf("GetVolumesById: %v", err)
	}
	oversized := vi
	oversized.Size = 12000
	vl.UpdateOversizedState(&oversized, dn)
	vl.UpdateVolumeSize(1, oversized.Size, 0)
	vl.EnsureCorrectWritables(&oversized)

	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected volume to stay unwritable after heartbeat refresh, got %d writable", w)
	}
	if !vl.vid2location[1].AnyOversized() {
		t.Fatalf("expected AnyOversized to be true after heartbeat refresh")
	}
}

// A volume that shrank back under the limit must have its oversized mark
// cleared by the heartbeat refresh, or it stays locked out of writables
// forever.
func TestUpdateOversizedStateClearsMarkWhenVolumeShrinks(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":4000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	dn := vl.vid2location[1].list[0]
	vi, err := dn.GetVolumesById(1)
	if err != nil {
		t.Fatalf("GetVolumesById: %v", err)
	}

	// Mark oversized via a heartbeat that reports a huge size.
	big := vi
	big.Size = 12000
	vl.UpdateOversizedState(&big, dn)
	if !vl.vid2location[1].AnyOversized() {
		t.Fatalf("expected AnyOversized after a huge report")
	}

	// A later heartbeat with a normal size clears the mark.
	vl.UpdateOversizedState(&vi, dn)
	if vl.vid2location[1].AnyOversized() {
		t.Fatalf("expected AnyOversized cleared after the volume shrank")
	}
}

// A volume removed for capacity must not be re-added to writables by
// ensureCorrectWritables before capacityRecoveryDelay elapses, even after its
// oversized mark is cleared by a heartbeat that reports a smaller size.
func TestEnsureCorrectWritablesHonorsRecoveryCooldown(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":4000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// Remove the volume for capacity, as RecordAssign would.
	if !vl.RecordAssign(1, 9000) {
		t.Fatalf("RecordAssign should report the volume reached capacity")
	}
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected 0 writable after RecordAssign, got %d", w)
	}

	dn := vl.vid2location[1].list[0]
	vi, err := dn.GetVolumesById(1)
	if err != nil {
		t.Fatalf("GetVolumesById: %v", err)
	}

	// Heartbeat reports the volume shrank back under the limit: the oversized
	// mark clears and effectiveSize decays, but the volume is still within
	// capacityRecoveryDelay.
	vi.Size = 4000
	vl.UpdateOversizedState(&vi, dn)
	vl.UpdateVolumeSize(1, vi.Size, 0)
	vl.EnsureCorrectWritables(&vi)
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected volume to stay unwritable during the cooldown, got %d writable", w)
	}

	// After the cooldown, a heartbeat with the shrunken size recovers it.
	advanceSizeTrackingClock(vl, 1, capacityRecoveryDelay+time.Second)
	if !vl.UpdateVolumeSize(1, vi.Size, 0) {
		t.Fatalf("expected volume to recover to writable after the cooldown")
	}
	vl.EnsureCorrectWritables(&vi)
	if w, _ := vl.GetWritableVolumeCount(); w != 1 {
		t.Fatalf("expected volume writable after the cooldown, got %d", w)
	}
}

// After the cooldown elapses, a volume whose effective size is still at the
// limit must not be restored by ensureCorrectWritables: the next assign would
// remove it again.
func TestEnsureCorrectWritablesDoesNotRestoreVolumeStillAtLimit(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":8000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// Remove the volume for capacity, as RecordAssign would. effectiveSize
	// lands at 13000, past the limit (10000); after the decay against a
	// reported size of 8000 it stays at 10500, still past the limit.
	if !vl.RecordAssign(1, 5000) {
		t.Fatalf("RecordAssign should report the volume reached capacity")
	}
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected 0 writable after RecordAssign, got %d", w)
	}

	dn := vl.vid2location[1].list[0]
	vi, err := dn.GetVolumesById(1)
	if err != nil {
		t.Fatalf("GetVolumesById: %v", err)
	}

	// Heartbeat reports a size below the limit: the oversized mark clears,
	// but the decay only halves the pending estimate, so effectiveSize stays
	// past the limit and UpdateVolumeSize refuses recovery.
	vi.Size = 8000
	vl.UpdateOversizedState(&vi, dn)
	vl.UpdateVolumeSize(1, vi.Size, 0)
	if vl.vid2location[1].AnyOversized() {
		t.Fatalf("expected oversized mark cleared after the shrink report")
	}

	// After the cooldown, the volume is still at the limit:
	// ensureCorrectWritables must not override UpdateVolumeSize's refusal.
	advanceSizeTrackingClock(vl, 1, capacityRecoveryDelay+time.Second)
	vl.EnsureCorrectWritables(&vi)
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected volume at the limit to stay unwritable, got %d writable", w)
	}
}

// A crowded volume is not a full one: it is above the growth threshold but
// still has room. One that drops out of writables while a replica is away must
// come back when the replica returns -- nothing writes to a volume that is not
// writable, so its size can never fall on its own and the lockout would be
// permanent.
func TestEnsureCorrectWritablesRestoresCrowdedVolumeAfterReplicaReturns(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "ip":"10.0.0.1",
        "volumes":[
          {"id":1, "size":9500, "replication":"001"}
        ],
        "limit":10
      },
      "server2":{
        "ip":"10.0.0.2",
        "volumes":[
          {"id":1, "size":9500, "replication":"001"}
        ],
        "limit":10
      }
    }
  }
}
`
	topo := setupWithLimit(t, layout, 10000)
	rp, _ := super_block.NewReplicaPlacementFromString("001")
	vl := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)

	// 9500 is past the growth threshold (9000) but under the limit (10000).
	vl.UpdateVolumeSize(1, 9500, 0)
	if _, crowded := vl.crowded[1]; !crowded {
		t.Fatalf("expected the volume to be crowded")
	}
	if w, _ := vl.GetWritableVolumeCount(); w != 1 {
		t.Fatalf("a crowded volume is still writable, got %d writable", w)
	}

	dn := vl.vid2location[1].list[1]
	vi, err := dn.GetVolumesById(1)
	if err != nil {
		t.Fatalf("GetVolumesById: %v", err)
	}
	vl.UnRegisterVolume(&vi, dn)
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected 0 writable while a replica is missing, got %d", w)
	}

	// The replica comes back on the next heartbeat.
	topo.RegisterVolumeLayout(vi, dn)
	advanceSizeTrackingClock(vl, 1, 5*time.Second)
	vl.UpdateOversizedState(&vi, dn)
	vl.UpdateVolumeSize(1, vi.Size, 0)
	vl.EnsureCorrectWritables(&vi)
	if w, _ := vl.GetWritableVolumeCount(); w != 1 {
		t.Fatalf("expected the volume writable again, got %d", w)
	}
}

// An incremental heartbeat announces an arrival with a short message that
// carries no size. Registering from it must not clear the oversized mark the
// full heartbeat set, or the volume is handed back to the writable list until
// the next full report.
func TestIncrementalRegistrationKeepsOversizedMark(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 25})

	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)

	// A full heartbeat reports the volume past the 32 KB limit.
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{{
		Id:               1,
		Size:             uint64(64 * 1024),
		ReplicaPlacement: uint32(0),
		Version:          uint32(needle.GetCurrentVersion()),
	}}, dn)
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected the oversized volume out of writables, got %d", w)
	}

	// The same volume is announced again as an arrival.
	topo.IncrementalSyncDataNodeRegistration([]*master_pb.VolumeShortInformationMessage{{
		Id:               1,
		ReplicaPlacement: uint32(0),
		Version:          uint32(needle.GetCurrentVersion()),
	}}, nil, dn)

	if !vl.vid2location[1].AnyOversized() {
		t.Fatalf("expected the oversized mark to survive the arrival announcement")
	}
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected the oversized volume to stay out of writables, got %d", w)
	}
}
