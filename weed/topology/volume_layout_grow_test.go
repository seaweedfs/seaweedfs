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
