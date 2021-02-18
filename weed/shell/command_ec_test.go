package shell

import (
	"fmt"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func TestCommandEcDistribution(t *testing.T) {

	allEcNodes := []*EcNode{
		newEcNode("dc1", "rack1", "dn1", 100),
		newEcNode("dc1", "rack2", "dn2", 100),
	}

	allocated := balancedEcDistribution(allEcNodes)

	fmt.Printf("allocated: %+v", allocated)
}

func TestCommandEcBalanceSmall(t *testing.T) {

	allEcNodes := []*EcNode{
		newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
		newEcNode("dc1", "rack2", "dn2", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
	}

	racks := collectRacks(allEcNodes)
	balanceEcVolumes(nil, "c1", allEcNodes, racks, false)
}

func TestCommandEcBalanceNothingToMove(t *testing.T) {

	allEcNodes := []*EcNode{
		newEcNode("dc1", "rack1", "dn1", 100).
			addEcVolumeAndShardsForTest(1, "c1", []uint32{0, 1, 2, 3, 4, 5, 6}).
			addEcVolumeAndShardsForTest(2, "c1", []uint32{7, 8, 9, 10, 11, 12, 13}),
		newEcNode("dc1", "rack1", "dn2", 100).
			addEcVolumeAndShardsForTest(1, "c1", []uint32{7, 8, 9, 10, 11, 12, 13}).
			addEcVolumeAndShardsForTest(2, "c1", []uint32{0, 1, 2, 3, 4, 5, 6}),
	}

	racks := collectRacks(allEcNodes)
	balanceEcVolumes(nil, "c1", allEcNodes, racks, false)
}

func TestCommandEcBalanceAddNewServers(t *testing.T) {

	allEcNodes := []*EcNode{
		newEcNode("dc1", "rack1", "dn1", 100).
			addEcVolumeAndShardsForTest(1, "c1", []uint32{0, 1, 2, 3, 4, 5, 6}).
			addEcVolumeAndShardsForTest(2, "c1", []uint32{7, 8, 9, 10, 11, 12, 13}),
		newEcNode("dc1", "rack1", "dn2", 100).
			addEcVolumeAndShardsForTest(1, "c1", []uint32{7, 8, 9, 10, 11, 12, 13}).
			addEcVolumeAndShardsForTest(2, "c1", []uint32{0, 1, 2, 3, 4, 5, 6}),
		newEcNode("dc1", "rack1", "dn3", 100),
		newEcNode("dc1", "rack1", "dn4", 100),
	}

	racks := collectRacks(allEcNodes)
	balanceEcVolumes(nil, "c1", allEcNodes, racks, false)
}

func TestCommandEcBalanceAddNewRacks(t *testing.T) {

	allEcNodes := []*EcNode{
		newEcNode("dc1", "rack1", "dn1", 100).
			addEcVolumeAndShardsForTest(1, "c1", []uint32{0, 1, 2, 3, 4, 5, 6}).
			addEcVolumeAndShardsForTest(2, "c1", []uint32{7, 8, 9, 10, 11, 12, 13}),
		newEcNode("dc1", "rack1", "dn2", 100).
			addEcVolumeAndShardsForTest(1, "c1", []uint32{7, 8, 9, 10, 11, 12, 13}).
			addEcVolumeAndShardsForTest(2, "c1", []uint32{0, 1, 2, 3, 4, 5, 6}),
		newEcNode("dc1", "rack2", "dn3", 100),
		newEcNode("dc1", "rack2", "dn4", 100),
	}

	racks := collectRacks(allEcNodes)
	balanceEcVolumes(nil, "c1", allEcNodes, racks, false)
}

func TestCommandEcBalanceVolumeEvenButRackUneven(t *testing.T) {

	allEcNodes := []*EcNode{
		newEcNode("dc1", "rack1", "dn_shared", 100).
			addEcVolumeAndShardsForTest(1, "c1", []uint32{0}).
			addEcVolumeAndShardsForTest(2, "c1", []uint32{0}),

		newEcNode("dc1", "rack1", "dn_a1", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{1}),
		newEcNode("dc1", "rack1", "dn_a2", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{2}),
		newEcNode("dc1", "rack1", "dn_a3", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{3}),
		newEcNode("dc1", "rack1", "dn_a4", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{4}),
		newEcNode("dc1", "rack1", "dn_a5", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{5}),
		newEcNode("dc1", "rack1", "dn_a6", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{6}),
		newEcNode("dc1", "rack1", "dn_a7", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{7}),
		newEcNode("dc1", "rack1", "dn_a8", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{8}),
		newEcNode("dc1", "rack1", "dn_a9", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{9}),
		newEcNode("dc1", "rack1", "dn_a10", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{10}),
		newEcNode("dc1", "rack1", "dn_a11", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{11}),
		newEcNode("dc1", "rack1", "dn_a12", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{12}),
		newEcNode("dc1", "rack1", "dn_a13", 100).addEcVolumeAndShardsForTest(1, "c1", []uint32{13}),

		newEcNode("dc1", "rack1", "dn_b1", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{1}),
		newEcNode("dc1", "rack1", "dn_b2", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{2}),
		newEcNode("dc1", "rack1", "dn_b3", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{3}),
		newEcNode("dc1", "rack1", "dn_b4", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{4}),
		newEcNode("dc1", "rack1", "dn_b5", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{5}),
		newEcNode("dc1", "rack1", "dn_b6", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{6}),
		newEcNode("dc1", "rack1", "dn_b7", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{7}),
		newEcNode("dc1", "rack1", "dn_b8", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{8}),
		newEcNode("dc1", "rack1", "dn_b9", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{9}),
		newEcNode("dc1", "rack1", "dn_b10", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{10}),
		newEcNode("dc1", "rack1", "dn_b11", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{11}),
		newEcNode("dc1", "rack1", "dn_b12", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{12}),
		newEcNode("dc1", "rack1", "dn_b13", 100).addEcVolumeAndShardsForTest(2, "c1", []uint32{13}),

		newEcNode("dc1", "rack1", "dn3", 100),
	}

	racks := collectRacks(allEcNodes)
	balanceEcVolumes(nil, "c1", allEcNodes, racks, false)
	balanceEcRacks(nil, racks, false)
}

func newEcNode(dc string, rack string, dataNodeId string, freeEcSlot int) *EcNode {
	return &EcNode{
		info: &master_pb.DataNodeInfo{
			Id:        dataNodeId,
			DiskInfos: make(map[string]*master_pb.DiskInfo),
		},
		dc:         dc,
		rack:       RackId(rack),
		freeEcSlot: freeEcSlot,
	}
}

func (ecNode *EcNode) addEcVolumeAndShardsForTest(vid uint32, collection string, shardIds []uint32) *EcNode {
	return ecNode.addEcVolumeShards(needle.VolumeId(vid), collection, shardIds)
}
