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
	if vl.UpdateVolumeSize(1, 4000, 0, true) {
		t.Fatalf("recovery should not fire before capacityRecoveryDelay")
	}

	// After the delay, a smaller size restores it.
	advanceSizeTrackingClock(vl, 1, capacityRecoveryDelay)
	if !vl.UpdateVolumeSize(1, 4000, 0, true) {
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

func TestSetVolumeAvailableRestoresActiveCountForCapacityFullVolume(t *testing.T) {
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
	if !vl.SetVolumeCapacityFull(1) {
		t.Fatalf("SetVolumeCapacityFull should report the volume was writable")
	}
	vl.AdjustActiveVolumeCountForFull(1)
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive-1 {
		t.Fatalf("expected activeVolumeCount decremented to %d, got %d", initialActive-1, got)
	}

	vl.accessLock.RLock()
	dn := vl.vid2location[1].list[0]
	vl.accessLock.RUnlock()

	if !vl.SetVolumeAvailable(dn, 1, false, false) {
		t.Fatalf("SetVolumeAvailable should report the volume became writable")
	}
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Fatalf("expected SetVolumeAvailable to restore activeVolumeCount to %d, got %d", initialActive, got)
	}
	if !vl.sizeTracking[1].fullSince.IsZero() {
		t.Fatalf("expected SetVolumeAvailable to clear fullSince after restoring activeVolumeCount")
	}

	advanceSizeTrackingClock(vl, 1, capacityRecoveryDelay+time.Second)
	if vl.UpdateVolumeSize(1, 4000, 0, true) {
		t.Fatalf("heartbeat recovery should not fire after SetVolumeAvailable already restored the volume")
	}
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Fatalf("expected activeVolumeCount to remain %d, got %d", initialActive, got)
	}
}

// A volume marked full by pending estimates takes no writes, so no heartbeat
// reports it again and only the periodic decay can bring it back.
func TestDecayQuietVolumeSizesRecoversPhantomFullVolume(t *testing.T) {
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
	VolumeGrowStrategy.Threshold = 0.9

	initialActive := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount
	if !vl.RecordAssign(1, 20000) {
		t.Fatalf("expected RecordAssign to remove the volume from writable")
	}
	vl.AdjustActiveVolumeCountForFull(1)
	if w, _ := vl.GetWritableVolumeCount(); w != 0 {
		t.Fatalf("expected 0 writable after the pending estimate filled the volume, got %d", w)
	}

	recovered := false
	for i := 0; i < 10 && !recovered; i++ {
		advanceSizeTrackingClock(vl, 1, capacityRecoveryDelay+time.Second)
		topo.DecayQuietVolumeSizes()
		w, _ := vl.GetWritableVolumeCount()
		recovered = w == 1
	}
	if !recovered {
		t.Fatalf("the periodic decay never returned the volume to the writable list")
	}
	if got := topo.diskUsages.usages[types.HardDriveType].activeVolumeCount; got != initialActive {
		t.Fatalf("expected activeVolumeCount restored to %d, got %d", initialActive, got)
	}
	if !vl.sizeTracking[1].fullSince.IsZero() {
		t.Fatalf("expected fullSince cleared after recovery")
	}
}

// The decay must not consume the replica-dedup window: a real report landing
// right behind it carries a size no volume server will send again, since only
// a volume whose content changed is reported at all.
func TestDecayQuietVolumeSizesKeepsTheNextHeartbeat(t *testing.T) {
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

	vl.RecordAssign(1, 2000)
	advanceSizeTrackingClock(vl, 1, 30*time.Second)
	topo.DecayQuietVolumeSizes()

	vl.UpdateVolumeSize(1, 6000, 7, true)

	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	if got := vl.sizeTracking[1].reportedSize; got != 6000 {
		t.Errorf("the heartbeat after a decay left reportedSize at %d, want the reported 6000", got)
	}
	if got := vl.sizeTracking[1].compactRevision; got != 7 {
		t.Errorf("the heartbeat after a decay left compactRevision at %d, want the reported 7", got)
	}
}

// The decay picks its volumes under a read lock and replays them under a write
// one. A compaction heartbeat landing in that gap must survive, so the replay
// takes the record as it stands rather than the size the pass set out with —
// the arguments below stand in for that older snapshot.
func TestDecayQuietVolumeSizesDoesNotRollBackAHeartbeat(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":9000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	vl.RecordAssign(1, 500)
	// The heartbeat that wins the race: a compaction shrank the volume.
	vl.UpdateVolumeSize(1, 2000, 3, true)
	// Far enough behind that the dedup window no longer covers for the replay,
	// which is the only case where reading the record under the lock is what
	// saves the report.
	advanceSizeTrackingClock(vl, 1, 3*time.Second)

	vl.UpdateVolumeSize(1, 9000, 0, false)

	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	if got := vl.sizeTracking[1].reportedSize; got != 2000 {
		t.Errorf("the decay rolled reportedSize back to %d, want the compacted 2000", got)
	}
	if got := vl.sizeTracking[1].compactRevision; got != 3 {
		t.Errorf("the decay rolled compactRevision back to %d, want the reported 3", got)
	}
	if got := vl.sizeTracking[1].effectiveSize; got > 2000 {
		t.Errorf("effectiveSize %d outgrew the compacted size the heartbeat reported", got)
	}
}

// The decay stands in for a report that never came, so a heartbeat arriving
// after the pass chose the volume takes its place rather than adding to it:
// halving twice in one cycle forgets pending bytes the volume has yet to write.
func TestDecayQuietVolumeSizesYieldsToAHeartbeatItRaced(t *testing.T) {
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

	vl.RecordAssign(1, 4000)
	advanceSizeTrackingClock(vl, 1, 30*time.Second)

	// The heartbeat wins the race and does the one decay this cycle owes.
	vl.UpdateVolumeSize(1, 4000, 0, true)
	vl.accessLock.RLock()
	afterHeartbeat := vl.sizeTracking[1].effectiveSize
	vl.accessLock.RUnlock()
	if afterHeartbeat != 6000 {
		t.Fatalf("the heartbeat left effectiveSize at %d, want 6000", afterHeartbeat)
	}

	vl.UpdateVolumeSize(1, 0, 0, false)

	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	if got := vl.sizeTracking[1].effectiveSize; got != afterHeartbeat {
		t.Errorf("the decay halved again to %d, want the heartbeat's %d left alone", got, afterHeartbeat)
	}
}

// A volume the disk really did fill can never recover through the decay, so it
// must leave the candidate set instead of costing a write lock every pulse for
// the rest of its life.
func TestDecayQuietVolumeSizesSkipsAGenuinelyFullVolume(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":10000, "replication":"000"},
          {"id":2, "size":4000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// Volume 1 is full on disk; volume 2 only looks full to the estimate.
	vl.SetVolumeCapacityFull(1)
	vl.RecordAssign(2, 6000)
	advanceSizeTrackingClock(vl, 1, 30*time.Second)
	advanceSizeTrackingClock(vl, 2, 30*time.Second)

	candidates := vl.quietDecayCandidates(10 * time.Second)
	for _, vid := range candidates {
		if vid == 1 {
			t.Errorf("the decay keeps taking the write lock for volume 1, which the disk really did fill")
		}
	}
	if len(candidates) != 1 || candidates[0] != 2 {
		t.Errorf("candidates %v, want only the phantom-full volume 2", candidates)
	}

	vl.DecayQuietVolumeSizes(10 * time.Second)

	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	if got := vl.sizeTracking[2].effectiveSize; got != 7000 {
		t.Errorf("the phantom-full volume was left at %d, want it decayed to 7000", got)
	}
}
