package topology

import (
	"testing"
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

	// A heartbeat that reports the now-oversized volume must refresh the mark,
	// otherwise the next ensureCorrectWritables would re-add it to writables.
	dn := vl.vid2location[1].list[0]
	vi, err := dn.GetVolumesById(1)
	if err != nil {
		t.Fatalf("GetVolumesById: %v", err)
	}
	oversized := vi
	oversized.Size = 12000
	vl.UpdateOversizedState(&oversized, dn)
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
