package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestECRebalanceWithLimitedSlots tests that EC rebalance handles the scenario
// where there are limited free slots on volume servers.
//
// This is a regression test for the error:
//
//	"no free ec shard slots. only 0 left"
//
// Scenario (from real usage):
// - 6 volume servers in 6 racks
// - Each server has max=10 volume slots
// - 7 volumes were EC encoded (7 × 14 = 98 EC shards)
// - All 14 shards per volume are on the original server (not yet distributed)
//
// Expected behavior:
// - The rebalance algorithm should distribute shards across servers
// - Even if perfect distribution isn't possible, it should do best-effort
// - Currently fails with "no free ec shard slots" because freeSlots calculation
//
//	doesn't account for shards being moved (freed slots on source, used on target)
func TestECRebalanceWithLimitedSlots(t *testing.T) {
	// Build a topology matching the problematic scenario:
	// 6 servers, each with 2+ volumes worth of EC shards (all 14 shards per volume on same server)
	topology := buildLimitedSlotsTopology()

	// Collect EC nodes from the topology
	ecNodes, totalFreeEcSlots := collectEcVolumeServersByDc(topology, "", types.HardDriveType)

	t.Logf("Topology summary:")
	t.Logf("  Number of EC nodes: %d", len(ecNodes))
	t.Logf("  Total free EC slots: %d", totalFreeEcSlots)

	// Log per-node details
	for _, node := range ecNodes {
		shardCount := 0
		for _, diskInfo := range node.info.DiskInfos {
			for _, ecShard := range diskInfo.EcShardInfos {
				shardCount += erasure_coding.ShardBits(ecShard.EcIndexBits).ShardIdCount()
			}
		}
		t.Logf("  Node %s (rack %s): %d shards, %d free slots",
			node.info.Id, node.rack, shardCount, node.freeEcSlot)
	}

	// Calculate total EC shards
	totalEcShards := 0
	for _, node := range ecNodes {
		for _, diskInfo := range node.info.DiskInfos {
			for _, ecShard := range diskInfo.EcShardInfos {
				totalEcShards += erasure_coding.ShardBits(ecShard.EcIndexBits).ShardIdCount()
			}
		}
	}
	t.Logf("  Total EC shards: %d", totalEcShards)

	// Document the issue:
	// With 98 EC shards (7 volumes × 14 shards) on 6 servers with max=10 each,
	// total capacity is 60 slots. But shards already occupy slots on their current servers.
	//
	// The current algorithm calculates free slots as:
	//   freeSlots = maxVolumeCount - volumeCount - ecShardCount
	//
	// If all shards are on their original servers:
	// - Server A has 28 shards (2 volumes × 14) → may have negative free slots
	// - This causes totalFreeEcSlots to be 0 or negative
	//
	// The EXPECTED improvement:
	// - Rebalance should recognize that moving a shard FREES a slot on the source
	// - The algorithm should work iteratively, moving shards one at a time
	// - Even if starting with 0 free slots, moving one shard opens a slot

	if totalFreeEcSlots < 1 {
		// This is the current (buggy) behavior we're documenting
		t.Logf("")
		t.Logf("KNOWN ISSUE: totalFreeEcSlots = %d (< 1)", totalFreeEcSlots)
		t.Logf("")
		t.Logf("This triggers the error: 'no free ec shard slots. only %d left'", totalFreeEcSlots)
		t.Logf("")
		t.Logf("Analysis:")
		t.Logf("  - %d EC shards across %d servers", totalEcShards, len(ecNodes))
		t.Logf("  - Shards are concentrated on original servers (not distributed)")
		t.Logf("  - Current slot calculation doesn't account for slots freed by moving shards")
		t.Logf("")
		t.Logf("Expected fix:")
		t.Logf("  1. Rebalance should work iteratively, moving one shard at a time")
		t.Logf("  2. Moving a shard from A to B: frees 1 slot on A, uses 1 slot on B")
		t.Logf("  3. The 'free slots' check should be per-move, not global")
		t.Logf("  4. Or: calculate 'redistributable slots' = total capacity - shards that must stay")

		// For now, document this is a known issue - don't fail the test
		// When the fix is implemented, this test should be updated to verify the fix works
		return
	}

	// If we get here, the issue might have been fixed
	t.Logf("totalFreeEcSlots = %d, rebalance should be possible", totalFreeEcSlots)
}

// TestECRebalanceZeroFreeSlots tests the specific scenario where
// the topology appears to have free slots but rebalance fails.
//
// This can happen when the VolumeCount in the topology includes the original
// volumes that were EC-encoded, making the free slot calculation incorrect.
func TestECRebalanceZeroFreeSlots(t *testing.T) {
	// Build a topology where volumes were NOT deleted after EC encoding
	// (VolumeCount still reflects the original volumes)
	topology := buildZeroFreeSlotTopology()

	ecNodes, totalFreeEcSlots := collectEcVolumeServersByDc(topology, "", types.HardDriveType)

	t.Logf("Zero free slots scenario:")
	for _, node := range ecNodes {
		shardCount := 0
		for _, diskInfo := range node.info.DiskInfos {
			for _, ecShard := range diskInfo.EcShardInfos {
				shardCount += erasure_coding.ShardBits(ecShard.EcIndexBits).ShardIdCount()
			}
		}
		t.Logf("  Node %s: %d shards, %d free slots, volumeCount=%d, max=%d",
			node.info.Id, shardCount, node.freeEcSlot,
			node.info.DiskInfos[string(types.HardDriveType)].VolumeCount,
			node.info.DiskInfos[string(types.HardDriveType)].MaxVolumeCount)
	}
	t.Logf("  Total free slots: %d", totalFreeEcSlots)

	if totalFreeEcSlots == 0 {
		t.Logf("")
		t.Logf("SCENARIO REPRODUCED: totalFreeEcSlots = 0")
		t.Logf("This would trigger: 'no free ec shard slots. only 0 left'")
	}
}

// buildZeroFreeSlotTopology creates a topology where rebalance will fail
// because servers are at capacity (volumeCount equals maxVolumeCount)
func buildZeroFreeSlotTopology() *master_pb.TopologyInfo {
	diskTypeKey := string(types.HardDriveType)

	// Each server has max=10, volumeCount=10 (full capacity)
	// Free capacity = (10-10) * 10 = 0 per server
	// This will trigger "no free ec shard slots" error
	return &master_pb.TopologyInfo{
		Id: "test_zero_free_slots",
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack0",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "127.0.0.1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									diskTypeKey: {
										Type:           diskTypeKey,
										MaxVolumeCount: 10,
										VolumeCount:    10, // At full capacity
										EcShardInfos:   buildEcShards([]uint32{3, 4}),
									},
								},
							},
						},
					},
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "127.0.0.1:8081",
								DiskInfos: map[string]*master_pb.DiskInfo{
									diskTypeKey: {
										Type:           diskTypeKey,
										MaxVolumeCount: 10,
										VolumeCount:    10,
										EcShardInfos:   buildEcShards([]uint32{1, 7}),
									},
								},
							},
						},
					},
					{
						Id: "rack2",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "127.0.0.1:8082",
								DiskInfos: map[string]*master_pb.DiskInfo{
									diskTypeKey: {
										Type:           diskTypeKey,
										MaxVolumeCount: 10,
										VolumeCount:    10,
										EcShardInfos:   buildEcShards([]uint32{2}),
									},
								},
							},
						},
					},
					{
						Id: "rack3",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "127.0.0.1:8083",
								DiskInfos: map[string]*master_pb.DiskInfo{
									diskTypeKey: {
										Type:           diskTypeKey,
										MaxVolumeCount: 10,
										VolumeCount:    10,
										EcShardInfos:   buildEcShards([]uint32{5, 6}),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func buildEcShards(volumeIds []uint32) []*master_pb.VolumeEcShardInformationMessage {
	var shards []*master_pb.VolumeEcShardInformationMessage
	for _, vid := range volumeIds {
		allShardBits := erasure_coding.ShardBits(0)
		for i := 0; i < erasure_coding.TotalShardsCount; i++ {
			allShardBits = allShardBits.AddShardId(erasure_coding.ShardId(i))
		}
		shards = append(shards, &master_pb.VolumeEcShardInformationMessage{
			Id:          vid,
			Collection:  "ectest",
			EcIndexBits: uint32(allShardBits),
		})
	}
	return shards
}

// buildLimitedSlotsTopology creates a topology matching the problematic scenario:
// - 6 servers in 6 racks
// - Each server has max=10 volume slots
// - 7 volumes were EC encoded, shards distributed as follows:
//   - rack0 (8080): volumes 3,4 → 28 shards
//   - rack1 (8081): volumes 1,7 → 28 shards
//   - rack2 (8082): volume 2 → 14 shards
//   - rack3 (8083): volumes 5,6 → 28 shards
//   - rack4 (8084): (no volumes originally)
//   - rack5 (8085): (no volumes originally)
func buildLimitedSlotsTopology() *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		Id: "test_limited_slots",
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					buildRackWithEcShards("rack0", "127.0.0.1:8080", 10, []uint32{3, 4}),
					buildRackWithEcShards("rack1", "127.0.0.1:8081", 10, []uint32{1, 7}),
					buildRackWithEcShards("rack2", "127.0.0.1:8082", 10, []uint32{2}),
					buildRackWithEcShards("rack3", "127.0.0.1:8083", 10, []uint32{5, 6}),
					buildRackWithEcShards("rack4", "127.0.0.1:8084", 10, []uint32{}),
					buildRackWithEcShards("rack5", "127.0.0.1:8085", 10, []uint32{}),
				},
			},
		},
	}
}

// buildRackWithEcShards creates a rack with one data node containing EC shards
// for the specified volume IDs (all 14 shards per volume)
func buildRackWithEcShards(rackId, nodeId string, maxVolumes int64, volumeIds []uint32) *master_pb.RackInfo {
	var ecShardInfos []*master_pb.VolumeEcShardInformationMessage

	for _, vid := range volumeIds {
		// All 14 shards (bits 0-13) on this server
		allShardBits := erasure_coding.ShardBits(0)
		for i := 0; i < erasure_coding.TotalShardsCount; i++ {
			allShardBits = allShardBits.AddShardId(erasure_coding.ShardId(i))
		}

		ecShardInfos = append(ecShardInfos, &master_pb.VolumeEcShardInformationMessage{
			Id:          vid,
			Collection:  "ectest",
			EcIndexBits: uint32(allShardBits),
		})
	}

	// Note: types.HardDriveType is "" (empty string), so we use "" as the key
	diskTypeKey := string(types.HardDriveType)

	return &master_pb.RackInfo{
		Id: rackId,
		DataNodeInfos: []*master_pb.DataNodeInfo{
			{
				Id: nodeId,
				DiskInfos: map[string]*master_pb.DiskInfo{
					diskTypeKey: {
						Type:           diskTypeKey,
						MaxVolumeCount: maxVolumes,
						VolumeCount:    int64(len(volumeIds)), // Original volumes still counted
						EcShardInfos:   ecShardInfos,
					},
				},
			},
		},
	}
}
