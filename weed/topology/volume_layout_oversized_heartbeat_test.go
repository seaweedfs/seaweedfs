package topology

import (
	"testing"
	"time"
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
