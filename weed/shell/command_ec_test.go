package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestCommandEcBalanceSmall(t *testing.T) {
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
			newEcNode("dc1", "rack2", "dn2", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}

	ecb.balanceEcVolumes("c1")
}

func TestCommandEcBalanceNothingToMove(t *testing.T) {
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).
				addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6}).
				addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{7, 8, 9, 10, 11, 12, 13}),
			newEcNode("dc1", "rack1", "dn2", 100).
				addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{7, 8, 9, 10, 11, 12, 13}).
				addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6}),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}

	ecb.balanceEcVolumes("c1")
}

func TestCommandEcBalanceAddNewServers(t *testing.T) {
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).
				addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6}).
				addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{7, 8, 9, 10, 11, 12, 13}),
			newEcNode("dc1", "rack1", "dn2", 100).
				addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{7, 8, 9, 10, 11, 12, 13}).
				addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6}),
			newEcNode("dc1", "rack1", "dn3", 100),
			newEcNode("dc1", "rack1", "dn4", 100),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}

	ecb.balanceEcVolumes("c1")
}

func TestCommandEcBalanceAddNewRacks(t *testing.T) {
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn1", 100).
				addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6}).
				addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{7, 8, 9, 10, 11, 12, 13}),
			newEcNode("dc1", "rack1", "dn2", 100).
				addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{7, 8, 9, 10, 11, 12, 13}).
				addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6}),
			newEcNode("dc1", "rack2", "dn3", 100),
			newEcNode("dc1", "rack2", "dn4", 100),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}

	ecb.balanceEcVolumes("c1")
}

func TestCommandEcBalanceVolumeEvenButRackUneven(t *testing.T) {
	ecb := ecBalancer{
		ecNodes: []*EcNode{
			newEcNode("dc1", "rack1", "dn_shared", 100).
				addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0}).
				addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{0}),

			newEcNode("dc1", "rack1", "dn_a1", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{1}),
			newEcNode("dc1", "rack1", "dn_a2", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{2}),
			newEcNode("dc1", "rack1", "dn_a3", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{3}),
			newEcNode("dc1", "rack1", "dn_a4", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{4}),
			newEcNode("dc1", "rack1", "dn_a5", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{5}),
			newEcNode("dc1", "rack1", "dn_a6", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{6}),
			newEcNode("dc1", "rack1", "dn_a7", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{7}),
			newEcNode("dc1", "rack1", "dn_a8", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{8}),
			newEcNode("dc1", "rack1", "dn_a9", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{9}),
			newEcNode("dc1", "rack1", "dn_a10", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{10}),
			newEcNode("dc1", "rack1", "dn_a11", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{11}),
			newEcNode("dc1", "rack1", "dn_a12", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{12}),
			newEcNode("dc1", "rack1", "dn_a13", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{13}),

			newEcNode("dc1", "rack1", "dn_b1", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{1}),
			newEcNode("dc1", "rack1", "dn_b2", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{2}),
			newEcNode("dc1", "rack1", "dn_b3", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{3}),
			newEcNode("dc1", "rack1", "dn_b4", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{4}),
			newEcNode("dc1", "rack1", "dn_b5", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{5}),
			newEcNode("dc1", "rack1", "dn_b6", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{6}),
			newEcNode("dc1", "rack1", "dn_b7", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{7}),
			newEcNode("dc1", "rack1", "dn_b8", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{8}),
			newEcNode("dc1", "rack1", "dn_b9", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{9}),
			newEcNode("dc1", "rack1", "dn_b10", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{10}),
			newEcNode("dc1", "rack1", "dn_b11", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{11}),
			newEcNode("dc1", "rack1", "dn_b12", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{12}),
			newEcNode("dc1", "rack1", "dn_b13", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{13}),

			newEcNode("dc1", "rack1", "dn3", 100),
		},
		applyBalancing: false,
		diskType:       types.HardDriveType,
	}

	ecb.balanceEcVolumes("c1")
	ecb.balanceEcRacks()
}

func newEcNode(dc string, rack string, dataNodeId string, freeEcSlot int) *EcNode {
	return &EcNode{
		info: &master_pb.DataNodeInfo{
			Id:        dataNodeId,
			DiskInfos: make(map[string]*master_pb.DiskInfo),
		},
		dc:         DataCenterId(dc),
		rack:       RackId(rack),
		freeEcSlot: freeEcSlot,
	}
}

func (ecNode *EcNode) addEcVolumeAndShardsForTest(vid uint32, collection string, shardIds []erasure_coding.ShardId) *EcNode {
	return ecNode.addEcVolumeShards(needle.VolumeId(vid), collection, shardIds, types.HardDriveType)
}

// TestCommandEcBalanceEvenDataAndParityDistribution verifies that after balancing:
// 1. Data shards (0-9) are evenly distributed across racks (max 2 per rack for 6 racks)
// 2. Parity shards (10-13) are evenly distributed across racks (max 1 per rack for 6 racks)
func TestCommandEcBalanceEvenDataAndParityDistribution(t *testing.T) {
	// Setup: All 14 shards start on rack1 (simulating fresh EC encode)
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			// All shards initially on rack1/dn1
			newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
			// Empty nodes on other racks
			newEcNode("dc1", "rack2", "dn2", 100),
			newEcNode("dc1", "rack3", "dn3", 100),
			newEcNode("dc1", "rack4", "dn4", 100),
			newEcNode("dc1", "rack5", "dn5", 100),
			newEcNode("dc1", "rack6", "dn6", 100),
		},
		applyBalancing: false, // Dry-run mode (simulates moves by updating internal state)
		diskType:       types.HardDriveType,
	}

	ecb.balanceEcVolumes("c1")

	// After balancing (dry-run), verify the PLANNED distribution by checking what moves were proposed
	// The ecb.ecNodes state is updated during dry-run to track planned moves
	vid := needle.VolumeId(1)
	dataShardCount := erasure_coding.DataShardsCount     // 10
	parityShardCount := erasure_coding.ParityShardsCount // 4

	// Count data and parity shards per rack based on current (updated) state
	dataPerRack, parityPerRack := countDataAndParityShardsPerRack(ecb.ecNodes, vid, dataShardCount)

	// With 6 racks:
	// - Data shards (10): max 2 per rack (ceil(10/6) = 2)
	// - Parity shards (4): max 1 per rack (ceil(4/6) = 1)
	maxDataPerRack := ceilDivide(dataShardCount, 6)     // 2
	maxParityPerRack := ceilDivide(parityShardCount, 6) // 1

	// Verify no rack has more than max data shards
	for rackId, count := range dataPerRack {
		if count > maxDataPerRack {
			t.Errorf("rack %s has %d data shards, expected max %d", rackId, count, maxDataPerRack)
		}
	}

	// Verify no rack has more than max parity shards
	for rackId, count := range parityPerRack {
		if count > maxParityPerRack {
			t.Errorf("rack %s has %d parity shards, expected max %d", rackId, count, maxParityPerRack)
		}
	}

	// Verify all shards are distributed (total counts)
	totalData := 0
	totalParity := 0
	for _, count := range dataPerRack {
		totalData += count
	}
	for _, count := range parityPerRack {
		totalParity += count
	}
	if totalData != dataShardCount {
		t.Errorf("total data shards = %d, expected %d", totalData, dataShardCount)
	}
	if totalParity != parityShardCount {
		t.Errorf("total parity shards = %d, expected %d", totalParity, parityShardCount)
	}

	// Verify data shards are spread across at least 5 racks (10 shards / 2 max per rack)
	racksWithData := len(dataPerRack)
	minRacksForData := dataShardCount / maxDataPerRack // At least 5 racks needed for 10 data shards
	if racksWithData < minRacksForData {
		t.Errorf("data shards spread across only %d racks, expected at least %d", racksWithData, minRacksForData)
	}

	// Verify parity shards are spread across at least 4 racks (4 shards / 1 max per rack)
	racksWithParity := len(parityPerRack)
	if racksWithParity < parityShardCount {
		t.Errorf("parity shards spread across only %d racks, expected at least %d", racksWithParity, parityShardCount)
	}

	t.Logf("Distribution after balancing:")
	t.Logf("  Data shards per rack: %v (max allowed: %d)", dataPerRack, maxDataPerRack)
	t.Logf("  Parity shards per rack: %v (max allowed: %d)", parityPerRack, maxParityPerRack)
}

// countDataAndParityShardsPerRack counts data and parity shards per rack
func countDataAndParityShardsPerRack(ecNodes []*EcNode, vid needle.VolumeId, dataShardCount int) (dataPerRack, parityPerRack map[string]int) {
	dataPerRack = make(map[string]int)
	parityPerRack = make(map[string]int)

	for _, ecNode := range ecNodes {
		si := findEcVolumeShardsInfo(ecNode, vid, types.HardDriveType)
		for _, shardId := range si.Ids() {
			rackId := string(ecNode.rack)
			if int(shardId) < dataShardCount {
				dataPerRack[rackId]++
			} else {
				parityPerRack[rackId]++
			}
		}
	}
	return
}

// TestCommandEcBalanceMultipleVolumesEvenDistribution tests that multiple volumes
// each get their data and parity shards evenly distributed
func TestCommandEcBalanceMultipleVolumesEvenDistribution(t *testing.T) {
	// Setup: Two volumes, each with all 14 shards on different starting racks
	ecb := &ecBalancer{
		ecNodes: []*EcNode{
			// Volume 1: all shards on rack1
			newEcNode("dc1", "rack1", "dn1", 100).addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
			// Volume 2: all shards on rack2
			newEcNode("dc1", "rack2", "dn2", 100).addEcVolumeAndShardsForTest(2, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}),
			// Empty nodes on other racks
			newEcNode("dc1", "rack3", "dn3", 100),
			newEcNode("dc1", "rack4", "dn4", 100),
			newEcNode("dc1", "rack5", "dn5", 100),
			newEcNode("dc1", "rack6", "dn6", 100),
		},
		applyBalancing: false, // Dry-run mode
		diskType:       types.HardDriveType,
	}

	ecb.balanceEcVolumes("c1")

	// Check both volumes
	for _, vid := range []needle.VolumeId{1, 2} {
		dataPerRack, parityPerRack := countDataAndParityShardsPerRack(ecb.ecNodes, vid, erasure_coding.DataShardsCount)

		maxDataPerRack := ceilDivide(erasure_coding.DataShardsCount, 6)
		maxParityPerRack := ceilDivide(erasure_coding.ParityShardsCount, 6)

		for rackId, count := range dataPerRack {
			if count > maxDataPerRack {
				t.Errorf("volume %d: rack %s has %d data shards, expected max %d", vid, rackId, count, maxDataPerRack)
			}
		}
		for rackId, count := range parityPerRack {
			if count > maxParityPerRack {
				t.Errorf("volume %d: rack %s has %d parity shards, expected max %d", vid, rackId, count, maxParityPerRack)
			}
		}

		t.Logf("Volume %d - Data: %v, Parity: %v", vid, dataPerRack, parityPerRack)
	}
}
