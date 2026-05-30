package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A volume removed by the heartbeat capacity path (not RecordAssign) must
// still recover once it shrinks — only works if SetVolumeCapacityFull stamps
// fullSince.
func TestSetVolumeCapacityFullStampsFullSinceAndRecovers(t *testing.T) {
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
	topo, vl := setupPickTest(t, layout, 10000)

	initialActive := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount
	initialWritables, _ := vl.GetWritableVolumeCount()
	if initialWritables != 1 {
		t.Fatalf("expected 1 writable volume initially, got %d", initialWritables)
	}

	if !vl.SetVolumeCapacityFull(1) {
		t.Fatalf("SetVolumeCapacityFull should report the volume was writable")
	}
	vl.AdjustActiveVolumeCountForFull(1)
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected 0 writable after capacity full, got %d", w)
	}
	if vl.sizeTracking[1].fullSince.IsZero() {
		t.Fatalf("expected SetVolumeCapacityFull to stamp fullSince")
	}

	// No recovery before capacityRecoveryDelay.
	advanceSizeTrackingClock(vl, 1, 3*time.Second)
	if vl.UpdateVolumeSize(1, 4000, 0) {
		t.Fatalf("recovery should not fire before capacityRecoveryDelay")
	}

	// After the delay, a smaller size restores it.
	advanceSizeTrackingClock(vl, 1, capacityRecoveryDelay)
	if !vl.UpdateVolumeSize(1, 4000, 0) {
		t.Fatalf("expected volume to recover to writable after shrinking")
	}
	vl.AdjustActiveVolumeCountAfterRecovery(1)

	if w, _ := vl.GetWritableVolumeCount(); w != initialWritables {
		t.Fatalf("expected %d writable after recovery, got %d", initialWritables, w)
	}
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Fatalf("expected activeVolumeCount restored to %d, got %d", initialActive, got)
	}
	if !vl.sizeTracking[1].fullSince.IsZero() {
		t.Fatalf("expected fullSince cleared after recovery")
	}
}
