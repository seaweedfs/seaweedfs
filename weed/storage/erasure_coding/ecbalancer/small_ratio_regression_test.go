package ecbalancer

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// Test every initial assignment of the three unique shards to three healthy DCs.
// This includes all shards on the encoder and the already-balanced-but-unsafe
// state with data+parity in one DC, data in another, and the third empty.
func TestSmallRatioAcrossThreeDCs(t *testing.T) {
	for _, placement := range []string{"000", "200", "211", "011"} {
		for a := 0; a < 3; a++ {
			for b := 0; b < 3; b++ {
				for p := 0; p < 3; p++ {
					t.Run(fmt.Sprintf("%s/%d%d%d", placement, a, b, p), func(t *testing.T) {
						topo := NewTopology()
						for i := 0; i < 3; i++ {
							id := fmt.Sprintf("node%d", i)
							dc := fmt.Sprintf("dc%d", i)
							n := topo.AddNode(id, dc, dc+":rack1", 100)
							n.AddDisk(0, "ssd", 100, 0)
						}
						for shard, node := range []int{a, b, p} {
							topo.nodes[fmt.Sprintf("node%d", node)].AddShards(7, "test", 0, bits(shard))
						}
						for _, node := range topo.nodes {
							occupied := volumeShardCount(node, volKey{collection: "test", vid: 7})
							node.freeSlots -= occupied
							node.disks[0].freeSlots -= occupied
							node.disks[0].shardCount = occupied
						}
						rp, err := super_block.NewReplicaPlacementFromString(placement)
						if err != nil {
							t.Fatal(err)
						}
						// Use the volume-specific ratio reported by Enterprise, even
						// when the collection fallback has the OSS default of 10+4.
						opts := Options{DiskType: "ssd", ReplicaPlacement: rp, Ratio: ratio(10, 4),
							VolumeRatio: func(string, uint32) (int, int) { return 2, 1 }}
						moves := Plan(topo, opts)
						vk := volKey{collection: "test", vid: 7}
						for id, node := range topo.nodes {
							if got := volumeShardCount(node, vk); got != 1 {
								t.Errorf("%s holds %d shards, want 1; moves=%+v", id, got, moves)
							}
							if node.freeSlots != 99 || node.disks[0].freeSlots != 99 || node.disks[0].shardCount != 1 {
								t.Errorf("%s: inconsistent capacity after placement: node=%d disk=%+v", id, node.freeSlots, node.disks[0])
							}
						}
						for shard := 0; shard < 3; shard++ {
							copies := 0
							for _, node := range topo.nodes {
								if node.shards[vk] != nil && node.shards[vk].shardBits&bits(shard) != 0 {
									copies++
								}
							}
							if copies != 1 {
								t.Errorf("shard %d: got %d copies, want 1", shard, copies)
							}
						}
						if next := Plan(topo, opts); len(next) != 0 {
							t.Errorf("placement did not converge: %+v", next)
						}
					})
				}
			}
		}
	}
}

func TestSmallRatioDoesNotMoveParityToFullRack(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a", "a", "a:r", 100)
	a.AddDisk(0, "ssd", 100, 2)
	a.AddShards(1, "test", 0, bits(0, 2))
	b := topo.AddNode("b", "b", "b:r", 100)
	b.AddDisk(0, "ssd", 100, 1)
	b.AddShards(1, "test", 0, bits(1))
	c := topo.AddNode("c", "c", "c:r", 0)
	c.AddDisk(0, "ssd", 0, 0)
	if moves := Plan(topo, Options{DiskType: "ssd", Ratio: ratio(2, 1)}); len(moves) != 0 {
		t.Fatalf("no eligible third rack: must not churn or move into a full destination: %+v", moves)
	}
}
