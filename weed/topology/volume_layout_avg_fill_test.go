package topology

import (
	"math"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestWritableAvgFill(t *testing.T) {
	// 3 writable volumes at 20%, 50%, 80% fill (sizes 2000, 5000, 8000 of
	// limit 10000): average fill is 50%.
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":2000, "replication":"000"},
          {"id":2, "size":5000, "replication":"000"},
          {"id":3, "size":8000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	avgFill, writableCount := vl.WritableAvgFill(nil)
	if writableCount != 3 {
		t.Fatalf("expected 3 writable volumes, got %d", writableCount)
	}
	if math.Abs(avgFill-0.5) > 1e-9 {
		t.Errorf("expected avg fill 0.5, got %v", avgFill)
	}
}

func TestWritableAvgFillNoWritables(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":2000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	vl.accessLock.Lock()
	vl.removeFromWritable(1)
	vl.accessLock.Unlock()

	avgFill, writableCount := vl.WritableAvgFill(nil)
	if writableCount != 0 || avgFill != 0 {
		t.Errorf("expected (0, 0) with no writables, got (%v, %d)", avgFill, writableCount)
	}
}

// A writable volume without a size-tracking entry yet — freshly created, no
// heartbeat size report so far — counts as empty, not as an error.
func TestWritableAvgFillUntrackedVolumeCountsAsEmpty(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":5000, "replication":"000"},
          {"id":2, "size":5000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	vl.accessLock.Lock()
	delete(vl.sizeTracking, 2)
	vl.accessLock.Unlock()

	avgFill, writableCount := vl.WritableAvgFill(nil)
	if writableCount != 2 {
		t.Fatalf("expected 2 writable volumes, got %d", writableCount)
	}
	if math.Abs(avgFill-0.25) > 1e-9 {
		t.Errorf("expected avg fill 0.25 (one of two volumes empty), got %v", avgFill)
	}
}

func TestWritableAvgFillScopedToTarget(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":9000, "replication":"000"},
          {"id":2, "size":9000, "replication":"000"}
        ],
        "limit":10
      }
    }
  },
  "dc2":{
    "rack2":{
      "server2":{
        "volumes":[
          {"id":3, "size":1000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout, 10000)

	avgFill, writableCount := vl.WritableAvgFill(&VolumeGrowOption{DataCenter: "dc2"})
	if writableCount != 1 || math.Abs(avgFill-0.1) > 1e-9 {
		t.Fatalf("expected dc2 fill (0.1, 1), got (%v, %d)", avgFill, writableCount)
	}

	// A target with no writable volumes reports none, so growth is not
	// blocked by fill elsewhere.
	avgFill, writableCount = vl.WritableAvgFill(&VolumeGrowOption{DataCenter: "dc2", DataNode: "missing"})
	if writableCount != 0 || avgFill != 0 {
		t.Fatalf("expected (0, 0) for a target with no writables, got (%v, %d)", avgFill, writableCount)
	}
}

func TestFindVolumeLayout(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":2000, "collection":"colA", "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	topo := setupWithLimit(t, layout, 10000)
	rp, _ := super_block.NewReplicaPlacementFromString("000")

	// Existing layout is found.
	if vl := topo.FindVolumeLayout("colA", rp, needle.EMPTY_TTL, types.HardDriveType); vl == nil {
		t.Error("expected layout for colA to be found")
	}

	// A missing collection resolves to nil and must not be created.
	if vl := topo.FindVolumeLayout("noSuchCollection", rp, needle.EMPTY_TTL, types.HardDriveType); vl != nil {
		t.Error("expected nil layout for unknown collection")
	}
	if _, ok := topo.collectionMap.Find("noSuchCollection"); ok {
		t.Error("FindVolumeLayout must not create the collection")
	}

	// A missing layout within an existing collection also resolves to nil.
	rpOther, _ := super_block.NewReplicaPlacementFromString("001")
	if vl := topo.FindVolumeLayout("colA", rpOther, needle.EMPTY_TTL, types.HardDriveType); vl != nil {
		t.Error("expected nil layout for unused replication")
	}
}
