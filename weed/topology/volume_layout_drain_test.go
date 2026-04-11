package topology

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestGetPendingSize(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":1000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// Initially no pending
	if p := vl.GetPendingSize(1); p != 0 {
		t.Fatalf("expected 0 pending, got %d", p)
	}

	// RecordAssign increases pending
	vl.RecordAssign(1, 5000)
	if p := vl.GetPendingSize(1); p != 5000 {
		t.Fatalf("expected 5000 pending, got %d", p)
	}

	// UpdateVolumeSize (heartbeat) decays pending
	vl.UpdateVolumeSize(1, 3000, 0)
	// effective was 6000, reported 3000 → decay to 3000 + (6000-3000)/2 = 4500
	// pending = 4500 - 3000 = 1500
	if p := vl.GetPendingSize(1); p != 1500 {
		t.Fatalf("expected 1500 pending after decay, got %d", p)
	}
}

func TestGetPendingSize_CompactionResets(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":5000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// Add large pending
	vl.RecordAssign(1, 4000)
	if p := vl.GetPendingSize(1); p != 4000 {
		t.Fatalf("expected 4000 pending, got %d", p)
	}

	// Compaction happens — size drops from 5000 to 2000, revision changes.
	// Without compaction awareness, decay would give: 2000 + (9000-2000)/2 = 5500.
	// With compaction awareness, vid2size resets to 2000 (the real size).
	vl.UpdateVolumeSize(1, 2000, 1) // revision 0 → 1

	if p := vl.GetPendingSize(1); p != 0 {
		t.Errorf("expected 0 pending after compaction reset, got %d", p)
	}

	// Verify vid2size is the reported size, not a decayed value
	vl.accessLock.RLock()
	if vl.sizeTracking[1].effectiveSize != 2000 {
		t.Errorf("expected vid2size=2000 after compaction, got %d", vl.sizeTracking[1].effectiveSize)
	}
	vl.accessLock.RUnlock()
}

func TestDrainAndRemoveFromWritable_NoPending(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":1000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// No pending — drain should return immediately
	start := time.Now()
	vl.DrainAndRemoveFromWritable(1)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("drain with no pending took %v, expected near-instant", elapsed)
	}

	// Verify volume is no longer writable
	writable, _ := vl.GetWritableVolumeCount()
	if writable != 0 {
		t.Errorf("expected 0 writable after drain, got %d", writable)
	}
}

func TestDrainAndRemoveFromWritable_WithPending(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":1000, "replication":"000"},
          {"id":2, "size":1000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// Add pending below threshold
	vl.RecordAssign(1, int64(pendingSizeThreshold-1))

	start := time.Now()
	vl.DrainAndRemoveFromWritable(1)
	elapsed := time.Since(start)

	// Should return quickly since pending is below threshold
	if elapsed > 100*time.Millisecond {
		t.Errorf("drain with pending below threshold took %v", elapsed)
	}

	// Volume removed from writable
	writables := vl.CloneWritableVolumes()
	for _, vid := range writables {
		if vid == 1 {
			t.Error("volume 1 should not be writable after drain")
		}
	}
	// Volume 2 still writable
	found := false
	for _, vid := range writables {
		if vid == 2 {
			found = true
		}
	}
	if !found {
		t.Error("volume 2 should still be writable")
	}
}

func TestDrainAndRemoveFromWritable_DecaysViaConcurrentHeartbeat(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":1000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	// Add large pending (well above threshold)
	vl.RecordAssign(1, 100*1024*1024) // 100 MB

	// Simulate heartbeats in background that will decay the pending
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			time.Sleep(500 * time.Millisecond)
			// Each heartbeat reports actual size, decaying pending by half
			vl.UpdateVolumeSize(1, 1000+uint64(i+1)*1000, 0)
		}
	}()

	start := time.Now()
	vl.DrainAndRemoveFromWritable(1)
	elapsed := time.Since(start)

	// Should have drained within a few seconds (heartbeats every 500ms).
	// Use generous margin for slow CI.
	if elapsed > 15*time.Second {
		t.Errorf("drain took %v, expected faster with concurrent heartbeats", elapsed)
	}

	<-done
}

func TestDrainAndSetVolumeReadOnly(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":1000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	topo := setupWithLimit(t, layout, 10000)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)
	dn := vl.Lookup(1)[0]

	start := time.Now()
	result := vl.DrainAndSetVolumeReadOnly(context.Background(), dn, 1)
	elapsed := time.Since(start)

	if !result {
		t.Error("expected SetVolumeReadOnly to return true")
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("drain with no pending took %v", elapsed)
	}

	// Verify readonly
	writable, _ := vl.GetWritableVolumeCount()
	if writable != 0 {
		t.Errorf("expected 0 writable after readonly, got %d", writable)
	}
}
