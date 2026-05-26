package ecbalancer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// TestFromActiveTopology verifies the encode/repair-side snapshot constructor maps
// nodes, per-disk EC shard counts, per-volume shard bits, and free slots from an
// ActiveTopology. Shard accounting is asserted exactly; free slots are asserted to
// be positive (their exact value depends on effective-capacity internals).
func TestFromActiveTopology(t *testing.T) {
	const vid uint32 = 7
	at := topology.NewActiveTopology(10)

	// Node A holds one EC shard (id 3) of volume 7 on disk 0; node B is empty.
	nodeA := &master_pb.DataNodeInfo{
		Id: "10.0.0.1:8080",
		DiskInfos: map[string]*master_pb.DiskInfo{
			"hdd": {
				DiskId:         0,
				MaxVolumeCount: 100,
				VolumeCount:    1,
				EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
					Id:          vid,
					Collection:  "c1",
					EcIndexBits: uint32(1) << 3,
					DiskId:      0,
				}},
			},
		},
	}
	nodeB := &master_pb.DataNodeInfo{
		Id: "10.0.0.2:8080",
		DiskInfos: map[string]*master_pb.DiskInfo{
			"hdd": {DiskId: 0, MaxVolumeCount: 100, VolumeCount: 0},
		},
	}
	if err := at.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id:            "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{nodeA, nodeB},
			}},
		}},
	}); err != nil {
		t.Fatalf("UpdateTopology: %v", err)
	}

	topo := FromActiveTopology(at, 0)

	if got := len(topo.nodes); got != 2 {
		t.Fatalf("node count = %d, want 2", got)
	}

	a := topo.nodes["10.0.0.1:8080"]
	if a == nil {
		t.Fatal("node A missing from snapshot")
	}
	if a.rack != "dc1:rack1" {
		t.Errorf("node A rack = %q, want dc1:rack1", a.rack)
	}
	diskA := a.disks[0]
	if diskA == nil {
		t.Fatal("node A disk 0 missing")
	}
	if diskA.shardCount != 1 {
		t.Errorf("node A disk 0 shardCount = %d, want 1", diskA.shardCount)
	}
	vs := a.shards[volKey{collection: "c1", vid: vid}]
	if vs == nil {
		t.Fatal("volume 7 shards not recorded on node A")
	}
	if !vs.shardBits.Has(erasure_coding.ShardId(3)) {
		t.Errorf("node A volume 7 shardBits %b missing shard 3", vs.shardBits)
	}
	if vs.shardBits.Count() != 1 {
		t.Errorf("node A volume 7 shard count = %d, want 1", vs.shardBits.Count())
	}

	b := topo.nodes["10.0.0.2:8080"]
	if b == nil {
		t.Fatal("node B missing from snapshot")
	}
	if b.rack != "dc1:rack1" {
		t.Errorf("node B rack = %q, want dc1:rack1", b.rack)
	}
	if diskB := b.disks[0]; diskB == nil || diskB.shardCount != 0 {
		t.Errorf("node B disk 0 shardCount = %v, want 0", diskB)
	}
	if len(b.shards) != 0 {
		t.Errorf("node B should hold no volume shards, got %d", len(b.shards))
	}

	// Free slots should be positive on both near-empty disks.
	if a.freeSlots <= 0 || b.freeSlots <= 0 {
		t.Errorf("free slots not positive: A=%d B=%d", a.freeSlots, b.freeSlots)
	}
}

// TestEcShardSlotsOnDiskRoundsUp covers the mixed-ratio (targetDataShards <
// existingDataShards) conversion: an existing shard's fractional footprint must
// round up so it is never floored to zero, which would overstate free capacity.
// OSS always uses the standard ratio at runtime, but ecShardSlotsOnDisk takes the
// target data-shard count as a parameter, so the fractional path is exercised
// directly here; the enterprise build reaches it with real per-volume ratios.
func TestEcShardSlotsOnDiskRoundsUp(t *testing.T) {
	// A single shard (id 3) of a standard 10-data-shard volume on disk 0.
	disk := &topology.DiskInfo{
		DiskID: 0,
		DiskInfo: &master_pb.DiskInfo{
			DiskId: 0,
			EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{{
				Id:          7,
				Collection:  "c1",
				EcIndexBits: uint32(1) << 3,
				DiskId:      0,
			}},
		},
	}

	// Against a 2-data-shard target the shard occupies 2/10 of a slot, which must
	// round up to 1 rather than floor to 0.
	if got := ecShardSlotsOnDisk(disk, 2); got != 1 {
		t.Errorf("ecShardSlotsOnDisk(target=2) = %d, want 1 (rounded up from 0.2)", got)
	}

	// Identity case: target equals the existing data-shard count, so the shard
	// consumes exactly its whole-number footprint.
	if got := ecShardSlotsOnDisk(disk, erasure_coding.DataShardsCount); got != 1 {
		t.Errorf("ecShardSlotsOnDisk(target=%d) = %d, want 1", erasure_coding.DataShardsCount, got)
	}
}
