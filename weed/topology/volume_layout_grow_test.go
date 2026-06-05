package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// Reproduces https://github.com/seaweedfs/seaweedfs/issues/8986
//
// Topology: 1 DC with 3 racks, replication "010" (different-rack).
// Each volume has replicas on 2 of the 3 racks.
// The third rack has no replica, so ShouldGrowVolumesByDcAndRack
// incorrectly returns true for that rack, causing endless volume creation
// even though the DC already has plenty of non-crowded writable volumes.
var topologyLayout3Racks = `
{
  "dc_edge":{
    "rack1":{
      "server1":{
        "ip":"10.0.0.1",
        "volumes":[
          {"id":1, "size":12312, "replication":"010"},
          {"id":2, "size":12312, "replication":"010"}
        ],
        "limit":30
      }
    },
    "rack2":{
      "server2":{
        "ip":"10.0.0.2",
        "volumes":[
          {"id":1, "size":12312, "replication":"010"},
          {"id":2, "size":12312, "replication":"010"}
        ],
        "limit":30
      }
    },
    "rack3":{
      "server3":{
        "ip":"10.0.0.3",
        "volumes":[],
        "limit":30
      }
    }
  }
}
`

func TestShouldGrowVolumesByDcAndRack_Issue8986(t *testing.T) {
	topo := setup(topologyLayout3Racks)

	rp, _ := super_block.NewReplicaPlacementFromString("010")
	vl := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)

	writables := vl.CloneWritableVolumes()
	if len(writables) != 2 {
		t.Fatalf("expected 2 writable volumes, got %d", len(writables))
	}

	// rack1 and rack2 each have replicas of the writable volumes.
	// ShouldGrowVolumesByDcAndRack should return false for them.
	if vl.ShouldGrowVolumesByDcAndRack(&writables, "dc_edge", "rack1") {
		t.Error("rack1 should NOT need growth — it has writable volumes")
	}
	if vl.ShouldGrowVolumesByDcAndRack(&writables, "dc_edge", "rack2") {
		t.Error("rack2 should NOT need growth — it has writable volumes")
	}

	// rack3 has no replicas. With the old logic, this returns true (should grow),
	// but since the DC already has non-crowded writable volumes with "010"
	// replication, growing a new volume is unnecessary — the existing volumes
	// can serve any write request that doesn't pin to a specific rack.
	if vl.ShouldGrowVolumesByDcAndRack(&writables, "dc_edge", "rack3") {
		t.Error("rack3 should NOT need growth — the DC already has non-crowded writable volumes that can serve writes")
	}
}

// Topology from https://github.com/seaweedfs/seaweedfs/issues/9832
// 1 DC, 2 racks, 1 server each, replication 010, one crowded volume (28.65 GB)
// near the 30 GB size limit.
var topologyLayout9832 = `
{
  "datacenter1":{
    "rack1":{
      "node-a":{
        "ip":"10.0.0.1",
        "volumes":[ {"id":12, "size":28650, "replication":"010", "collection":"bucket-nexus"} ],
        "limit":30
      }
    },
    "rack2":{
      "node-b":{
        "ip":"10.0.0.2",
        "volumes":[ {"id":12, "size":28650, "replication":"010", "collection":"bucket-nexus"} ],
        "limit":30
      }
    }
  }
}
`

// Reproduces https://github.com/seaweedfs/seaweedfs/issues/9832
//
// A crowded "010" volume made the periodic rack-aware scan return true for
// every rack in the DC, so it grew once per rack with a hardcoded step of 2 —
// 2 racks × 2 = 4 logical volumes (8 physical) — and ignored a lowered
// master.volume_growth.copy_2. PlanRackAwareGrowth now plans a single DC-wide
// grow capped at copy_N.
func TestPlanRackAwareGrowth_Issue9832(t *testing.T) {
	defer restoreCopyCounts(VolumeGrowStrategy.Copy1Count, VolumeGrowStrategy.Copy2Count)
	VolumeGrowStrategy.Copy2Count = 1 // user's master.volume_growth.copy_2

	topo := setupWithLimit(t, topologyLayout9832, 30000)
	rp, _ := super_block.NewReplicaPlacementFromString("010")
	vl := topo.GetVolumeLayout("bucket-nexus", rp, needle.EMPTY_TTL, types.HardDriveType)

	if writables := vl.CloneWritableVolumes(); len(writables) != 1 {
		t.Fatalf("expected 1 writable volume, got %d", len(writables))
	}

	// The crowded volume makes both racks report "should grow" — the source of
	// the old per-rack multiplication.
	writables := vl.CloneWritableVolumes()
	if !vl.ShouldGrowVolumesByDcAndRack(&writables, "datacenter1", "rack1") ||
		!vl.ShouldGrowVolumesByDcAndRack(&writables, "datacenter1", "rack2") {
		t.Fatal("expected both racks to report should-grow for the crowded volume")
	}

	plans := vl.PlanRackAwareGrowth(topo.ListDCAndRacks(), 0, 2)
	if len(plans) != 1 {
		t.Fatalf("expected 1 DC-wide grow, got %d: %+v", len(plans), plans)
	}
	if plans[0].Rack != "" {
		t.Errorf("expected DC-wide grow (empty rack), got rack %q", plans[0].Rack)
	}
	if plans[0].WritableVolumeCount != 1 {
		t.Errorf("expected copy_2=1 logical volume, got %d", plans[0].WritableVolumeCount)
	}
}

// With the default copy_2 the per-event step is preserved (not increased): the
// fix only removes the per-rack multiplication.
func TestPlanRackAwareGrowth_DefaultStepNotMultiplied(t *testing.T) {
	defer restoreCopyCounts(VolumeGrowStrategy.Copy1Count, VolumeGrowStrategy.Copy2Count)
	VolumeGrowStrategy.Copy2Count = 6 // default

	topo := setupWithLimit(t, topologyLayout9832, 30000)
	rp, _ := super_block.NewReplicaPlacementFromString("010")
	vl := topo.GetVolumeLayout("bucket-nexus", rp, needle.EMPTY_TTL, types.HardDriveType)

	plans := vl.PlanRackAwareGrowth(topo.ListDCAndRacks(), 0, 2)
	if len(plans) != 1 {
		t.Fatalf("expected 1 DC-wide grow, got %d: %+v", len(plans), plans)
	}
	if plans[0].WritableVolumeCount != 2 {
		t.Errorf("expected step count 2 (min of step 2 and copy_2 6), got %d", plans[0].WritableVolumeCount)
	}
}

// A non-crowded "010" volume needs no growth at all.
func TestPlanRackAwareGrowth_NotCrowdedNoGrowth(t *testing.T) {
	layout := `
{
  "datacenter1":{
    "rack1":{ "node-a":{ "ip":"10.0.0.1", "volumes":[ {"id":12, "size":1000, "replication":"010", "collection":"c"} ], "limit":30 } },
    "rack2":{ "node-b":{ "ip":"10.0.0.2", "volumes":[ {"id":12, "size":1000, "replication":"010", "collection":"c"} ], "limit":30 } }
  }
}
`
	topo := setupWithLimit(t, layout, 30000)
	rp, _ := super_block.NewReplicaPlacementFromString("010")
	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)

	if plans := vl.PlanRackAwareGrowth(topo.ListDCAndRacks(), 0, 2); len(plans) != 0 {
		t.Fatalf("expected no growth for non-crowded volume, got %+v", plans)
	}
}

// Non-rack-spanning replication ("000") still grows per rack: a rack without a
// writable volume gets its own grow.
func TestPlanRackAwareGrowth_PerRackForNonRackSpanning(t *testing.T) {
	layout := `
{
  "datacenter1":{
    "rack1":{ "node-a":{ "ip":"10.0.0.1", "volumes":[ {"id":1, "size":1000, "replication":"000", "collection":"c"} ], "limit":30 } },
    "rack2":{ "node-b":{ "ip":"10.0.0.2", "volumes":[], "limit":30 } }
  }
}
`
	topo := setupWithLimit(t, layout, 30000)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)

	plans := vl.PlanRackAwareGrowth(topo.ListDCAndRacks(), 0, 2)
	if len(plans) != 1 {
		t.Fatalf("expected 1 per-rack grow, got %d: %+v", len(plans), plans)
	}
	if plans[0].Rack != "rack2" {
		t.Errorf("expected grow pinned to empty rack2, got %q", plans[0].Rack)
	}
}

func restoreCopyCounts(copy1, copy2 uint32) {
	VolumeGrowStrategy.Copy1Count = copy1
	VolumeGrowStrategy.Copy2Count = copy2
}
