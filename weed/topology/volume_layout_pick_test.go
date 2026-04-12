package topology

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// setupWithLimit is like setup() but allows specifying the volumeSizeLimit
// so that VolumeLayouts are created with the correct limit from the start.
func setupWithLimit(t testing.TB, topologyLayout string, volumeSizeLimit uint64) *Topology {
	t.Helper()
	var data interface{}
	if err := json.Unmarshal([]byte(topologyLayout), &data); err != nil {
		t.Fatalf("setupWithLimit: json.Unmarshal: %v", err)
	}
	mTopology, ok := data.(map[string]interface{})
	if !ok {
		t.Fatalf("setupWithLimit: expected map[string]interface{}, got %T", data)
	}

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), volumeSizeLimit, 5, false)
	for dcKey, dcValue := range mTopology {
		dc := NewDataCenter(dcKey)
		dcMap := dcValue.(map[string]interface{})
		topo.LinkChildNode(dc)
		for rackKey, rackValue := range dcMap {
			dcRack := NewRack(rackKey)
			rackMap := rackValue.(map[string]interface{})
			dc.LinkChildNode(dcRack)
			for serverKey, serverValue := range rackMap {
				server := NewDataNode(serverKey)
				serverMap := serverValue.(map[string]interface{})
				if ip, ok := serverMap["ip"]; ok {
					server.Ip = ip.(string)
				}
				dcRack.LinkChildNode(server)
				for _, v := range serverMap["volumes"].([]interface{}) {
					m := v.(map[string]interface{})
					vi := storage.VolumeInfo{
						Id:      needle.VolumeId(int64(m["id"].(float64))),
						Size:    uint64(m["size"].(float64)),
						Version: needle.GetCurrentVersion(),
					}
					if mVal, ok := m["collection"]; ok {
						vi.Collection = mVal.(string)
					}
					if mVal, ok := m["replication"]; ok {
						rp, _ := super_block.NewReplicaPlacementFromString(mVal.(string))
						vi.ReplicaPlacement = rp
					}
					if vi.ReplicaPlacement != nil {
						vl := topo.GetVolumeLayout(vi.Collection, vi.ReplicaPlacement, needle.EMPTY_TTL, types.HardDriveType)
						vl.RegisterVolume(&vi, server)
						vl.setVolumeWritable(vi.Id)
					}
					server.AddOrUpdateVolume(vi)
				}
				disk := server.getOrCreateDisk("")
				disk.UpAdjustDiskUsageDelta("", &DiskUsageCounts{
					maxVolumeCount: int64(serverMap["limit"].(float64)),
				})
			}
		}
	}
	return topo
}

func setupPickTest(t testing.TB, layout string, volumeSizeLimit uint64) (*Topology, *VolumeLayout) {
	t.Helper()
	topo := setupWithLimit(t, layout, volumeSizeLimit)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)
	return topo, vl
}

func TestPickForWriteWeightedDistribution(t *testing.T) {
	// 3 volumes at 20%, 50%, 80% full (sizes 2000, 5000, 8000 of limit 10000)
	// remaining: 8000, 5000, 2000 => ratios ~53%, 33%, 13%
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
	_, vl := setupPickTest(t, layout,10000)

	counts := make(map[needle.VolumeId]int)
	option := &VolumeGrowOption{}
	n := 60000

	for i := 0; i < n; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, option)
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		counts[vid]++
	}

	// vid 1 (remaining 8000) > vid 2 (remaining 5000) > vid 3 (remaining 2000)
	if counts[1] <= counts[3] {
		t.Errorf("expected vid 1 picked more than vid 3: vid1=%d, vid3=%d", counts[1], counts[3])
	}
	if counts[2] <= counts[3] {
		t.Errorf("expected vid 2 picked more than vid 3: vid2=%d, vid3=%d", counts[2], counts[3])
	}

	// Check proportions: expected 8000/5000/2000 out of 15000
	expected := map[needle.VolumeId]float64{
		1: 8000.0 / 15000.0,
		2: 5000.0 / 15000.0,
		3: 2000.0 / 15000.0,
	}
	for vid, expectedPct := range expected {
		actualPct := float64(counts[vid]) / float64(n)
		if math.Abs(actualPct-expectedPct) > 0.03 {
			t.Errorf("vid %d: expected ~%.1f%%, got %.1f%%", vid, expectedPct*100, actualPct*100)
		}
	}
}

func TestPickForWriteWithPendingSize(t *testing.T) {
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
	_, vl := setupPickTest(t, layout,10000)

	// Add large pending to vid 1, making it effectively 9000/10000
	vl.RecordAssign(1, 8000)

	counts := make(map[needle.VolumeId]int)
	option := &VolumeGrowOption{}
	n := 10000

	for i := 0; i < n; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, option)
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		counts[vid]++
	}

	// vid 2 (remaining ~9000) should be picked much more than vid 1 (remaining ~1000)
	ratio := float64(counts[2]) / float64(counts[1])
	if ratio < 3.0 {
		t.Errorf("vid2/vid1 ratio %.2f expected >= 3.0 (vid1=%d, vid2=%d)", ratio, counts[1], counts[2])
	}
}

func TestPickForWriteSingleWritable(t *testing.T) {
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
	_, vl := setupPickTest(t, layout,10000)

	option := &VolumeGrowOption{}
	for i := 0; i < 100; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, option)
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		if vid != 1 {
			t.Fatalf("expected vid 1, got %d", vid)
		}
	}
}

func TestPickForWriteAllNearFull(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":9999, "replication":"000"},
          {"id":2, "size":9999, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout,10000)

	option := &VolumeGrowOption{}
	for i := 0; i < 100; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, option)
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		if vid != 1 && vid != 2 {
			t.Fatalf("expected vid 1 or 2, got %d", vid)
		}
	}
}

func TestPickForWriteConstrainedWeighted(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "ip":"10.0.0.1",
        "volumes":[
          {"id":1, "size":2000, "replication":"000"},
          {"id":2, "size":8000, "replication":"000"}
        ],
        "limit":10
      }
    },
    "rack2":{
      "server2":{
        "ip":"10.0.0.2",
        "volumes":[
          {"id":3, "size":5000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout,10000)

	counts := make(map[needle.VolumeId]int)
	option := &VolumeGrowOption{DataCenter: "dc1"}
	n := 20000

	for i := 0; i < n; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, option)
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		counts[vid]++
	}

	// vid 1 (remaining 8000) should be picked most, vid 2 (remaining 2000) least
	if counts[1] <= counts[2] {
		t.Errorf("expected vid 1 picked more than vid 2: vid1=%d, vid2=%d", counts[1], counts[2])
	}
}

func TestRecordAssignMarksCrowded(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":8500, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout,10000)

	// Volume at 85% — not crowded yet (threshold is 90%)
	_, crowded := vl.GetWritableVolumeCount()
	if crowded != 0 {
		t.Fatalf("expected 0 crowded, got %d", crowded)
	}

	// Add pending that pushes past 90%
	vl.RecordAssign(1, 1000)

	_, crowded = vl.GetWritableVolumeCount()
	if crowded != 1 {
		t.Errorf("expected 1 crowded after pending push past 90%%, got %d", crowded)
	}
}

func TestHeartbeatDecaysPendingSize(t *testing.T) {
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
	_, vl := setupPickTest(t, layout,10000)

	// vid2size starts at 1000 (reported). Add 8000 pending → 9000.
	vl.RecordAssign(1, 8000)

	vl.accessLock.RLock()
	if vl.sizeTracking[1].effectiveSize != 9000 {
		t.Fatalf("expected vid2size=9000 after RecordAssign, got %d", vl.sizeTracking[1].effectiveSize)
	}
	vl.accessLock.RUnlock()

	// Helper to simulate a new heartbeat cycle (advance past dedup window)
	advanceCycle := func() {
		vl.accessLock.Lock()
		vl.sizeTracking[1].lastUpdateTime = time.Now().Add(-3 * time.Second)
		vl.accessLock.Unlock()
	}

	// Heartbeat: volume server reports size=3000 (some writes landed).
	// Old effective=9000, new reported=3000 → excess=6000 → decayed to 3000.
	// So vid2size should become 3000 + 6000/2 = 6000, not just 3000.
	vl.UpdateVolumeSize(1, 3000, 0)

	vl.accessLock.RLock()
	if vl.sizeTracking[1].effectiveSize != 6000 {
		t.Errorf("expected vid2size=6000 after decay (3000 + 6000/2), got %d", vl.sizeTracking[1].effectiveSize)
	}
	vl.accessLock.RUnlock()

	// Second heartbeat: size=5000. Old effective=6000 → excess=1000 → decay to 500.
	// vid2size should become 5000 + 1000/2 = 5500.
	advanceCycle()
	vl.UpdateVolumeSize(1, 5000, 0)

	vl.accessLock.RLock()
	if vl.sizeTracking[1].effectiveSize != 5500 {
		t.Errorf("expected vid2size=5500 after second decay (5000 + 1000/2), got %d", vl.sizeTracking[1].effectiveSize)
	}
	vl.accessLock.RUnlock()

	// Third heartbeat: size=5500. Old effective=5500 → no excess.
	// vid2size should be exactly 5500.
	advanceCycle()
	vl.UpdateVolumeSize(1, 5500, 0)

	vl.accessLock.RLock()
	if vl.sizeTracking[1].effectiveSize != 5500 {
		t.Errorf("expected vid2size=5500 (no excess), got %d", vl.sizeTracking[1].effectiveSize)
	}
	vl.accessLock.RUnlock()

	// vid 2 (remaining 9000) should be picked more than vid 1 (remaining 4500)
	counts := make(map[needle.VolumeId]int)
	option := &VolumeGrowOption{}
	for i := 0; i < 10000; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, option)
		if err != nil {
			t.Fatalf("PickForWrite: %v", err)
		}
		counts[vid]++
	}
	if counts[2] <= counts[1] {
		t.Errorf("vid 2 (remaining 9000) should be picked more than vid 1 (remaining 4500): vid1=%d, vid2=%d", counts[1], counts[2])
	}
}

func TestHeartbeatDecayDedupReplicas(t *testing.T) {
	// Volume 1 replicated on server1 and server2.
	// Both servers report size=3000 in the same heartbeat cycle.
	// Decay should run only once, not once per replica.
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "ip":"10.0.0.1",
        "volumes":[
          {"id":1, "size":1000, "replication":"001"}
        ],
        "limit":10
      },
      "server2":{
        "ip":"10.0.0.2",
        "volumes":[
          {"id":1, "size":1000, "replication":"001"}
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

	// Add pending: effective = 1000 + 8000 = 9000
	vl.RecordAssign(1, 8000)

	vl.accessLock.RLock()
	if vl.sizeTracking[1].effectiveSize != 9000 {
		t.Fatalf("expected vid2size=9000, got %d", vl.sizeTracking[1].effectiveSize)
	}
	vl.accessLock.RUnlock()

	// Both replicas report size=3000. Decay should happen once: 3000 + (9000-3000)/2 = 6000.
	// Calling UpdateVolumeSize twice simulates two replicas reporting in the same cycle.
	vl.UpdateVolumeSize(1, 3000, 0)
	vl.UpdateVolumeSize(1, 3000, 0) // second replica, same size — should be a no-op

	vl.accessLock.RLock()
	got := vl.sizeTracking[1].effectiveSize
	vl.accessLock.RUnlock()
	// Without dedup: would be 3000 + (6000-3000)/2 = 4500 (double decay).
	// With dedup: should be 6000 (single decay).
	if got != 6000 {
		t.Errorf("expected vid2size=6000 (single decay), got %d (double decay would give 4500)", got)
	}
}

func TestUpdateVolumeSize_DecaysEvenWhenReportedSizeUnchanged(t *testing.T) {
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

	// Add pending: effective = 1000 + 8000 = 9000
	vl.RecordAssign(1, 8000)
	if p := vl.GetPendingSize(1); p != 8000 {
		t.Fatalf("expected 8000 pending, got %d", p)
	}

	// First heartbeat: reported size unchanged at 1000 (writes haven't landed).
	// Decay should still run: 1000 + (9000-1000)/2 = 5000.
	vl.UpdateVolumeSize(1, 1000, 0)
	if p := vl.GetPendingSize(1); p != 4000 {
		t.Errorf("expected 4000 pending after first decay, got %d", p)
	}

	// Simulate next heartbeat cycle (>2s later) with same reported size.
	// Need to advance lastUpdateTime — manipulate directly under lock.
	vl.accessLock.Lock()
	vl.sizeTracking[1].lastUpdateTime = time.Now().Add(-3 * time.Second)
	vl.accessLock.Unlock()

	// Second heartbeat: still 1000. Decay again: 1000 + (5000-1000)/2 = 3000.
	vl.UpdateVolumeSize(1, 1000, 0)
	if p := vl.GetPendingSize(1); p != 2000 {
		t.Errorf("expected 2000 pending after second decay, got %d", p)
	}
}

func TestShouldGrowVolumesByDcAndRack_WithPendingSize(t *testing.T) {
	layout := `
{
  "dc1":{
    "rack1":{
      "server1":{
        "ip":"10.0.0.1",
        "volumes":[
          {"id":1, "size":8500, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`
	_, vl := setupPickTest(t, layout,10000)

	writables := vl.CloneWritableVolumes()
	if vl.ShouldGrowVolumesByDcAndRack(&writables, "dc1", "rack1") {
		t.Error("should not grow before pending makes volume crowded")
	}

	// Add pending that pushes effective size past 9000 threshold
	vl.RecordAssign(1, 600)

	if !vl.ShouldGrowVolumesByDcAndRack(&writables, "dc1", "rack1") {
		t.Error("should grow after pending pushes volume past crowded threshold")
	}
}

