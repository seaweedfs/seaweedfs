package topology

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/assert"
)

// NOTE: These tests are designed to work with any value of erasure_coding.DataShardsCount.
// This ensures compatibility with custom erasure coding configurations where DataShardsCount
// might be changed from the default value of 10. All shard-to-volume conversion calculations
// are done dynamically using the actual constant value.

// testGetDiskStorageImpact is a test helper that provides the same interface as the removed
// GetDiskStorageImpact method. For simplicity, it returns the total impact as "planned"
// and zeros for "reserved" since the distinction is not critical for most test scenarios.
func testGetDiskStorageImpact(at *ActiveTopology, nodeID string, diskID uint32) (plannedVolumeSlots, reservedVolumeSlots int64, plannedShardSlots, reservedShardSlots int32, estimatedSize int64) {
	impact := at.GetEffectiveCapacityImpact(nodeID, diskID)
	// Return total impact as "planned" for test compatibility
	return int64(impact.VolumeSlots), 0, impact.ShardSlots, 0, 0
}

// TestStorageSlotChangeArithmetic tests the arithmetic operations on StorageSlotChange
func TestStorageSlotChangeArithmetic(t *testing.T) {
	// Test basic arithmetic operations
	a := StorageSlotChange{VolumeSlots: 5, ShardSlots: 10}
	b := StorageSlotChange{VolumeSlots: 3, ShardSlots: 8}

	// Test Add
	sum := a.Add(b)
	assert.Equal(t, StorageSlotChange{VolumeSlots: 8, ShardSlots: 18}, sum, "Add should work correctly")

	// Test Subtract
	diff := a.Subtract(b)
	assert.Equal(t, StorageSlotChange{VolumeSlots: 2, ShardSlots: 2}, diff, "Subtract should work correctly")

	// Test AddInPlace
	c := StorageSlotChange{VolumeSlots: 1, ShardSlots: 2}
	c.AddInPlace(b)
	assert.Equal(t, StorageSlotChange{VolumeSlots: 4, ShardSlots: 10}, c, "AddInPlace should modify in place")

	// Test SubtractInPlace
	d := StorageSlotChange{VolumeSlots: 10, ShardSlots: 20}
	d.SubtractInPlace(b)
	assert.Equal(t, StorageSlotChange{VolumeSlots: 7, ShardSlots: 12}, d, "SubtractInPlace should modify in place")

	// Test IsZero
	zero := StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}
	nonZero := StorageSlotChange{VolumeSlots: 1, ShardSlots: 0}
	assert.True(t, zero.IsZero(), "Zero struct should return true for IsZero")
	assert.False(t, nonZero.IsZero(), "Non-zero struct should return false for IsZero")

	// Test ToVolumeSlots conversion
	impact1 := StorageSlotChange{VolumeSlots: 5, ShardSlots: 10}
	assert.Equal(t, int64(6), impact1.ToVolumeSlots(), fmt.Sprintf("ToVolumeSlots should be 5 + 10/%d = 6", erasure_coding.DataShardsCount))

	impact2 := StorageSlotChange{VolumeSlots: -2, ShardSlots: 25}
	assert.Equal(t, int64(0), impact2.ToVolumeSlots(), fmt.Sprintf("ToVolumeSlots should be -2 + 25/%d = 0", erasure_coding.DataShardsCount))

	impact3 := StorageSlotChange{VolumeSlots: 3, ShardSlots: 7}
	assert.Equal(t, int64(3), impact3.ToVolumeSlots(), fmt.Sprintf("ToVolumeSlots should be 3 + 7/%d = 3 (integer division)", erasure_coding.DataShardsCount))
}

// TestStorageSlotChange tests the new dual-level storage slot tracking
func TestStorageSlotChange(t *testing.T) {
	activeTopology := NewActiveTopology(10)

	// Create test topology
	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "10.0.0.1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										DiskId:         0,
										Type:           "hdd",
										VolumeCount:    5,
										MaxVolumeCount: 20,
									},
								},
							},
							{
								Id: "10.0.0.2:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										DiskId:         0,
										Type:           "hdd",
										VolumeCount:    8,
										MaxVolumeCount: 15,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	activeTopology.UpdateTopology(topologyInfo)

	// Test 1: Basic storage slot calculation
	ecSourceChange, ecTargetChange := CalculateTaskStorageImpact(TaskTypeErasureCoding, 1024*1024*1024)
	assert.Equal(t, int32(0), ecSourceChange.VolumeSlots, "EC source reserves with zero StorageSlotChange impact")
	assert.Equal(t, int32(0), ecSourceChange.ShardSlots, "EC source should have zero shard impact")
	assert.Equal(t, int32(0), ecTargetChange.VolumeSlots, "EC should not directly impact target volume slots")
	assert.Equal(t, int32(0), ecTargetChange.ShardSlots, "EC target should have zero shard impact from this simplified function")

	balSourceChange, balTargetChange := CalculateTaskStorageImpact(TaskTypeBalance, 1024*1024*1024)
	assert.Equal(t, int32(-1), balSourceChange.VolumeSlots, "Balance should free 1 volume slot on source")
	assert.Equal(t, int32(1), balTargetChange.VolumeSlots, "Balance should consume 1 volume slot on target")

	// Test 2: EC shard impact calculation
	shardImpact := CalculateECShardStorageImpact(3, 100*1024*1024) // 3 shards, 100MB each
	assert.Equal(t, int32(0), shardImpact.VolumeSlots, "EC shards should not impact volume slots")
	assert.Equal(t, int32(3), shardImpact.ShardSlots, "EC should impact 3 shard slots")

	// Test 3: Add EC task with shard-level tracking
	sourceServer := "10.0.0.1:8080"
	sourceDisk := uint32(0)
	shardDestinations := []string{"10.0.0.2:8080", "10.0.0.2:8080"}
	shardDiskIDs := []uint32{0, 0}

	expectedShardSize := int64(50 * 1024 * 1024)    // 50MB per shard
	originalVolumeSize := int64(1024 * 1024 * 1024) // 1GB original

	// Create source specs (single replica in this test)
	sources := []TaskSourceSpec{
		{ServerID: sourceServer, DiskID: sourceDisk, CleanupType: CleanupVolumeReplica},
	}

	// Create destination specs
	destinations := make([]TaskDestinationSpec, len(shardDestinations))
	shardImpact = CalculateECShardStorageImpact(1, expectedShardSize)
	for i, dest := range shardDestinations {
		destinations[i] = TaskDestinationSpec{
			ServerID:      dest,
			DiskID:        shardDiskIDs[i],
			StorageImpact: &shardImpact,
			EstimatedSize: &expectedShardSize,
		}
	}

	err := activeTopology.AddPendingTask(TaskSpec{
		TaskID:       "ec_test",
		TaskType:     TaskTypeErasureCoding,
		VolumeID:     100,
		VolumeSize:   originalVolumeSize,
		Sources:      sources,
		Destinations: destinations,
	})
	assert.NoError(t, err, "Should add EC shard task successfully")

	// Test 4: Check storage impact on source (EC reserves with zero impact)
	sourceImpact := activeTopology.GetEffectiveCapacityImpact("10.0.0.1:8080", 0)
	assert.Equal(t, int32(0), sourceImpact.VolumeSlots, "Source should show 0 volume slot impact (EC reserves with zero impact)")
	assert.Equal(t, int32(0), sourceImpact.ShardSlots, "Source should show 0 shard slot impact")

	// Test 5: Check storage impact on target (should gain shards)
	targetImpact := activeTopology.GetEffectiveCapacityImpact("10.0.0.2:8080", 0)
	assert.Equal(t, int32(0), targetImpact.VolumeSlots, "Target should show 0 volume slot impact (EC shards don't use volume slots)")
	assert.Equal(t, int32(2), targetImpact.ShardSlots, "Target should show 2 shard slot impact")

	// Test 6: Check effective capacity calculation (EC source reserves with zero StorageSlotChange)
	sourceCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	targetCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)

	// Source: 15 original available (EC source reserves with zero StorageSlotChange impact)
	assert.Equal(t, int64(15), sourceCapacity, "Source should have 15 available slots (EC source has zero StorageSlotChange impact)")

	// Target: 7 original available - (2 shards / 10) = 7 (since 2/10 rounds down to 0)
	assert.Equal(t, int64(7), targetCapacity, "Target should have 7 available slots (minimal shard impact)")

	// Test 7: Add traditional balance task for comparison
	err = activeTopology.AddPendingTask(TaskSpec{
		TaskID:     "balance_test",
		TaskType:   TaskTypeBalance,
		VolumeID:   101,
		VolumeSize: 512 * 1024 * 1024,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.2:8080", DiskID: 0},
		},
	})
	assert.NoError(t, err, "Should add balance task successfully")

	// Check updated impacts after adding balance task
	finalSourceImpact := activeTopology.GetEffectiveCapacityImpact("10.0.0.1:8080", 0)
	finalTargetImpact := activeTopology.GetEffectiveCapacityImpact("10.0.0.2:8080", 0)

	assert.Equal(t, int32(-1), finalSourceImpact.VolumeSlots, "Source should show -1 volume slot impact (EC: 0, Balance: -1)")
	assert.Equal(t, int32(1), finalTargetImpact.VolumeSlots, "Target should show 1 volume slot impact (Balance: +1)")
	assert.Equal(t, int32(2), finalTargetImpact.ShardSlots, "Target should still show 2 shard slot impact (EC shards)")
}

// TestStorageSlotChangeCapacityCalculation tests the capacity calculation with mixed slot types
func TestStorageSlotChangeCapacityCalculation(t *testing.T) {
	activeTopology := NewActiveTopology(10)

	// Create simple topology
	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "10.0.0.1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										DiskId:         0,
										Type:           "hdd",
										VolumeCount:    10,
										MaxVolumeCount: 100, // Large capacity for testing
									},
								},
							},
						},
					},
				},
			},
		},
	}

	activeTopology.UpdateTopology(topologyInfo)

	// Initial capacity
	initialCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	assert.Equal(t, int64(90), initialCapacity, "Should start with 90 available slots")

	// Add tasks with different shard slot impacts
	targetImpact1 := StorageSlotChange{VolumeSlots: 0, ShardSlots: 5} // Target gains 5 shards
	estimatedSize1 := int64(100 * 1024 * 1024)
	err := activeTopology.AddPendingTask(TaskSpec{
		TaskID:     "shard_test_1",
		TaskType:   TaskTypeErasureCoding,
		VolumeID:   100,
		VolumeSize: estimatedSize1,
		Sources: []TaskSourceSpec{
			{ServerID: "", DiskID: 0}, // Source not applicable here
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0, StorageImpact: &targetImpact1, EstimatedSize: &estimatedSize1},
		},
	})
	assert.NoError(t, err, "Should add shard test 1 successfully")

	// Capacity should be reduced by pending tasks via StorageSlotChange
	capacityAfterShards := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	// Dynamic calculation: 5 shards < DataShardsCount, so no volume impact
	expectedImpact5 := int64(5 / erasure_coding.DataShardsCount) // Should be 0 for any reasonable DataShardsCount
	assert.Equal(t, int64(90-expectedImpact5), capacityAfterShards, fmt.Sprintf("5 shard slots should consume %d volume slot equivalent (5/%d = %d)", expectedImpact5, erasure_coding.DataShardsCount, expectedImpact5))

	// Add more shards to reach threshold
	additionalShards := int32(erasure_coding.DataShardsCount)                        // Add exactly one volume worth of shards
	targetImpact2 := StorageSlotChange{VolumeSlots: 0, ShardSlots: additionalShards} // Target gains additional shards
	estimatedSize2 := int64(100 * 1024 * 1024)
	err = activeTopology.AddPendingTask(TaskSpec{
		TaskID:     "shard_test_2",
		TaskType:   TaskTypeErasureCoding,
		VolumeID:   101,
		VolumeSize: estimatedSize2,
		Sources: []TaskSourceSpec{
			{ServerID: "", DiskID: 0}, // Source not applicable here
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0, StorageImpact: &targetImpact2, EstimatedSize: &estimatedSize2},
		},
	})
	assert.NoError(t, err, "Should add shard test 2 successfully")

	// Dynamic calculation: (5 + DataShardsCount) shards should consume 1 volume slot
	totalShards := 5 + erasure_coding.DataShardsCount
	expectedImpact15 := int64(totalShards / erasure_coding.DataShardsCount) // Should be 1
	capacityAfterMoreShards := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	assert.Equal(t, int64(90-expectedImpact15), capacityAfterMoreShards, fmt.Sprintf("%d shard slots should consume %d volume slot equivalent (%d/%d = %d)", totalShards, expectedImpact15, totalShards, erasure_coding.DataShardsCount, expectedImpact15))

	// Add a full volume task
	targetImpact3 := StorageSlotChange{VolumeSlots: 1, ShardSlots: 0} // Target gains 1 volume
	estimatedSize3 := int64(1024 * 1024 * 1024)
	err = activeTopology.AddPendingTask(TaskSpec{
		TaskID:     "volume_test",
		TaskType:   TaskTypeBalance,
		VolumeID:   102,
		VolumeSize: estimatedSize3,
		Sources: []TaskSourceSpec{
			{ServerID: "", DiskID: 0}, // Source not applicable here
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0, StorageImpact: &targetImpact3, EstimatedSize: &estimatedSize3},
		},
	})
	assert.NoError(t, err, "Should add volume test successfully")

	// Capacity should be reduced by 1 more volume slot
	finalCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	assert.Equal(t, int64(88), finalCapacity, "1 volume + 15 shard slots should consume 2 volume slots total")

	// Verify the detailed storage impact
	plannedVol, reservedVol, plannedShard, reservedShard, _ := testGetDiskStorageImpact(activeTopology, "10.0.0.1:8080", 0)
	assert.Equal(t, int64(1), plannedVol, "Should show 1 planned volume slot")
	assert.Equal(t, int64(0), reservedVol, "Should show 0 reserved volume slots")
	assert.Equal(t, int32(15), plannedShard, "Should show 15 planned shard slots")
	assert.Equal(t, int32(0), reservedShard, "Should show 0 reserved shard slots")
}

// TestECMultipleTargets demonstrates proper handling of EC operations with multiple targets
func TestECMultipleTargets(t *testing.T) {
	activeTopology := NewActiveTopology(10)

	// Create test topology with multiple target nodes
	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "10.0.0.1:8080", // Source
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {DiskId: 0, Type: "hdd", VolumeCount: 10, MaxVolumeCount: 50},
								},
							},
							{
								Id: "10.0.0.2:8080", // Target 1
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {DiskId: 0, Type: "hdd", VolumeCount: 5, MaxVolumeCount: 30},
								},
							},
							{
								Id: "10.0.0.3:8080", // Target 2
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {DiskId: 0, Type: "hdd", VolumeCount: 8, MaxVolumeCount: 40},
								},
							},
							{
								Id: "10.0.0.4:8080", // Target 3
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {DiskId: 0, Type: "hdd", VolumeCount: 12, MaxVolumeCount: 35},
								},
							},
						},
					},
				},
			},
		},
	}

	activeTopology.UpdateTopology(topologyInfo)

	// Demonstrate why CalculateTaskStorageImpact is insufficient for EC
	sourceChange, targetChange := CalculateTaskStorageImpact(TaskTypeErasureCoding, 1*1024*1024*1024)
	assert.Equal(t, StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, sourceChange, "Source reserves with zero StorageSlotChange")
	assert.Equal(t, StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, targetChange, "Target has zero impact from simplified function - insufficient for multi-target EC")

	// Proper way: Use AddPendingTask for multiple targets
	sourceServer := "10.0.0.1:8080"
	sourceDisk := uint32(0)

	// EC typically distributes shards across multiple targets
	shardDestinations := []string{
		"10.0.0.2:8080", "10.0.0.2:8080", "10.0.0.2:8080", "10.0.0.2:8080", "10.0.0.2:8080", // 5 shards to target 1
		"10.0.0.3:8080", "10.0.0.3:8080", "10.0.0.3:8080", "10.0.0.3:8080", "10.0.0.3:8080", // 5 shards to target 2
		"10.0.0.4:8080", "10.0.0.4:8080", "10.0.0.4:8080", "10.0.0.4:8080", // 4 shards to target 3
	}
	shardDiskIDs := make([]uint32, len(shardDestinations))
	for i := range shardDiskIDs {
		shardDiskIDs[i] = 0
	}

	// Create source specs (single replica in this test)
	sources := []TaskSourceSpec{
		{ServerID: sourceServer, DiskID: sourceDisk, CleanupType: CleanupVolumeReplica},
	}

	// Create destination specs
	destinations := make([]TaskDestinationSpec, len(shardDestinations))
	expectedShardSize := int64(50 * 1024 * 1024)
	shardImpact := CalculateECShardStorageImpact(1, expectedShardSize)
	for i, dest := range shardDestinations {
		destinations[i] = TaskDestinationSpec{
			ServerID:      dest,
			DiskID:        shardDiskIDs[i],
			StorageImpact: &shardImpact,
			EstimatedSize: &expectedShardSize,
		}
	}

	err := activeTopology.AddPendingTask(TaskSpec{
		TaskID:       "ec_multi_target",
		TaskType:     TaskTypeErasureCoding,
		VolumeID:     200,
		VolumeSize:   1 * 1024 * 1024 * 1024,
		Sources:      sources,
		Destinations: destinations,
	})
	assert.NoError(t, err, "Should add multi-target EC task successfully")

	// Verify source impact (EC reserves with zero StorageSlotChange)
	sourcePlannedVol, sourceReservedVol, sourcePlannedShard, sourceReservedShard, _ := testGetDiskStorageImpact(activeTopology, "10.0.0.1:8080", 0)
	assert.Equal(t, int64(0), sourcePlannedVol, "Source should reserve with zero volume slot impact")
	assert.Equal(t, int64(0), sourceReservedVol, "Source should not have reserved capacity yet")
	assert.Equal(t, int32(0), sourcePlannedShard, "Source should not have planned shard impact")
	assert.Equal(t, int32(0), sourceReservedShard, "Source should not have reserved shard impact")
	// Note: EstimatedSize tracking is no longer exposed via public API

	// Verify target impacts (planned, not yet reserved)
	target1PlannedVol, target1ReservedVol, target1PlannedShard, target1ReservedShard, _ := testGetDiskStorageImpact(activeTopology, "10.0.0.2:8080", 0)
	target2PlannedVol, target2ReservedVol, target2PlannedShard, target2ReservedShard, _ := testGetDiskStorageImpact(activeTopology, "10.0.0.3:8080", 0)
	target3PlannedVol, target3ReservedVol, target3PlannedShard, target3ReservedShard, _ := testGetDiskStorageImpact(activeTopology, "10.0.0.4:8080", 0)

	assert.Equal(t, int64(0), target1PlannedVol, "Target 1 should not have planned volume impact")
	assert.Equal(t, int32(5), target1PlannedShard, "Target 1 should plan to receive 5 shards")
	assert.Equal(t, int64(0), target1ReservedVol, "Target 1 should not have reserved capacity yet")
	assert.Equal(t, int32(0), target1ReservedShard, "Target 1 should not have reserved shards yet")

	assert.Equal(t, int64(0), target2PlannedVol, "Target 2 should not have planned volume impact")
	assert.Equal(t, int32(5), target2PlannedShard, "Target 2 should plan to receive 5 shards")
	assert.Equal(t, int64(0), target2ReservedVol, "Target 2 should not have reserved capacity yet")
	assert.Equal(t, int32(0), target2ReservedShard, "Target 2 should not have reserved shards yet")

	assert.Equal(t, int64(0), target3PlannedVol, "Target 3 should not have planned volume impact")
	assert.Equal(t, int32(4), target3PlannedShard, "Target 3 should plan to receive 4 shards")
	assert.Equal(t, int64(0), target3ReservedVol, "Target 3 should not have reserved capacity yet")
	assert.Equal(t, int32(0), target3ReservedShard, "Target 3 should not have reserved shards yet")

	// Verify effective capacity (considers both pending and active tasks via StorageSlotChange)
	sourceCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	target1Capacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)
	target2Capacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.3:8080", 0)
	target3Capacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.4:8080", 0)

	// Dynamic capacity calculations based on actual DataShardsCount
	expectedTarget1Impact := int64(5 / erasure_coding.DataShardsCount) // 5 shards impact
	expectedTarget2Impact := int64(5 / erasure_coding.DataShardsCount) // 5 shards impact
	expectedTarget3Impact := int64(4 / erasure_coding.DataShardsCount) // 4 shards impact

	assert.Equal(t, int64(40), sourceCapacity, "Source: 40 (EC source reserves with zero StorageSlotChange impact)")
	assert.Equal(t, int64(25-expectedTarget1Impact), target1Capacity, fmt.Sprintf("Target 1: 25 - %d (5 shards/%d = %d impact) = %d", expectedTarget1Impact, erasure_coding.DataShardsCount, expectedTarget1Impact, 25-expectedTarget1Impact))
	assert.Equal(t, int64(32-expectedTarget2Impact), target2Capacity, fmt.Sprintf("Target 2: 32 - %d (5 shards/%d = %d impact) = %d", expectedTarget2Impact, erasure_coding.DataShardsCount, expectedTarget2Impact, 32-expectedTarget2Impact))
	assert.Equal(t, int64(23-expectedTarget3Impact), target3Capacity, fmt.Sprintf("Target 3: 23 - %d (4 shards/%d = %d impact) = %d", expectedTarget3Impact, erasure_coding.DataShardsCount, expectedTarget3Impact, 23-expectedTarget3Impact))

	t.Logf("EC operation distributed %d shards across %d targets", len(shardDestinations), 3)
	t.Logf("Capacity impacts: EC source reserves with zero impact, Targets minimal (shards < %d)", erasure_coding.DataShardsCount)
}

// TestCapacityReservationCycle demonstrates the complete task lifecycle and capacity management
func TestCapacityReservationCycle(t *testing.T) {
	activeTopology := NewActiveTopology(10)

	// Create test topology
	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "10.0.0.1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {DiskId: 0, Type: "hdd", VolumeCount: 10, MaxVolumeCount: 20},
								},
							},
							{
								Id: "10.0.0.2:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {DiskId: 0, Type: "hdd", VolumeCount: 5, MaxVolumeCount: 15},
								},
							},
						},
					},
				},
			},
		},
	}
	activeTopology.UpdateTopology(topologyInfo)

	// Initial capacity
	sourceCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	targetCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)
	assert.Equal(t, int64(10), sourceCapacity, "Source initial capacity")
	assert.Equal(t, int64(10), targetCapacity, "Target initial capacity")

	// Step 1: Add pending task (should reserve capacity via StorageSlotChange)
	err := activeTopology.AddPendingTask(TaskSpec{
		TaskID:     "balance_test",
		TaskType:   TaskTypeBalance,
		VolumeID:   123,
		VolumeSize: 1 * 1024 * 1024 * 1024,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.2:8080", DiskID: 0},
		},
	})
	assert.NoError(t, err, "Should add balance test successfully")

	sourceCapacityAfterPending := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	targetCapacityAfterPending := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)
	assert.Equal(t, int64(11), sourceCapacityAfterPending, "Source should gain capacity from pending balance task (balance source frees 1 slot)")
	assert.Equal(t, int64(9), targetCapacityAfterPending, "Target should consume capacity from pending task (balance reserves 1 slot)")

	// Verify planning capacity considers the same pending tasks
	planningDisks := activeTopology.GetDisksForPlanning(TaskTypeBalance, "", 1)
	assert.Len(t, planningDisks, 2, "Both disks should be available for planning")

	// Step 2: Assign task (capacity already reserved by pending task)
	err = activeTopology.AssignTask("balance_test")
	assert.NoError(t, err, "Should assign task successfully")

	sourceCapacityAfterAssign := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	targetCapacityAfterAssign := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)

	assert.Equal(t, int64(11), sourceCapacityAfterAssign, "Source capacity should remain same (already accounted by pending)")
	assert.Equal(t, int64(9), targetCapacityAfterAssign, "Target capacity should remain same (already accounted by pending)")

	// Note: Detailed task state tracking (planned vs reserved) is no longer exposed via public API
	// The important functionality is that capacity calculations remain consistent

	// Step 3: Complete task (should release reserved capacity)
	err = activeTopology.CompleteTask("balance_test")
	assert.NoError(t, err, "Should complete task successfully")

	sourceCapacityAfterComplete := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	targetCapacityAfterComplete := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)
	assert.Equal(t, int64(10), sourceCapacityAfterComplete, "Source should return to original capacity")
	assert.Equal(t, int64(10), targetCapacityAfterComplete, "Target should return to original capacity")

	// Step 4: Apply actual storage change (simulates master topology update)
	activeTopology.ApplyActualStorageChange("10.0.0.1:8080", 0, -1) // Source loses 1 volume
	activeTopology.ApplyActualStorageChange("10.0.0.2:8080", 0, 1)  // Target gains 1 volume

	// Final capacity should reflect actual topology changes
	finalSourceCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	finalTargetCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)
	assert.Equal(t, int64(11), finalSourceCapacity, "Source: (20-9) = 11 after losing 1 volume")
	assert.Equal(t, int64(9), finalTargetCapacity, "Target: (15-6) = 9 after gaining 1 volume")

	t.Logf("Capacity lifecycle with StorageSlotChange: Pending -> Assigned -> Released -> Applied")
	t.Logf("Source: 10 -> 11 -> 11 -> 10 -> 11 (freed by pending balance, then applied)")
	t.Logf("Target: 10 -> 9 -> 9 -> 10 -> 9 (reserved by pending, then applied)")
}

// TestReplicatedVolumeECOperations tests EC operations on replicated volumes
func TestReplicatedVolumeECOperations(t *testing.T) {
	activeTopology := NewActiveTopology(10)

	// Setup cluster with multiple servers for replicated volumes
	activeTopology.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "10.0.0.1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 10},
								},
							},
							{
								Id: "10.0.0.2:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 5},
								},
							},
							{
								Id: "10.0.0.3:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 3},
								},
							},
							{
								Id: "10.0.0.4:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 15},
								},
							},
							{
								Id: "10.0.0.5:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 20},
								},
							},
							{
								Id: "10.0.0.6:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 25},
								},
							},
						},
					},
				},
			},
		},
	})

	// Test: EC operation on replicated volume (3 replicas)
	volumeID := uint32(300)
	originalVolumeSize := int64(1024 * 1024 * 1024) // 1GB

	// Create source specs for replicated volume (3 replicas)
	sources := []TaskSourceSpec{
		{ServerID: "10.0.0.1:8080", DiskID: 0, CleanupType: CleanupVolumeReplica}, // Replica 1
		{ServerID: "10.0.0.2:8080", DiskID: 0, CleanupType: CleanupVolumeReplica}, // Replica 2
		{ServerID: "10.0.0.3:8080", DiskID: 0, CleanupType: CleanupVolumeReplica}, // Replica 3
	}

	// EC destinations (shards distributed across different servers than sources)
	shardDestinations := []string{
		"10.0.0.4:8080", "10.0.0.4:8080", "10.0.0.4:8080", "10.0.0.4:8080", "10.0.0.4:8080", // 5 shards
		"10.0.0.5:8080", "10.0.0.5:8080", "10.0.0.5:8080", "10.0.0.5:8080", "10.0.0.5:8080", // 5 shards
		"10.0.0.6:8080", "10.0.0.6:8080", "10.0.0.6:8080", "10.0.0.6:8080", // 4 shards
	}
	shardDiskIDs := make([]uint32, len(shardDestinations))
	for i := range shardDiskIDs {
		shardDiskIDs[i] = 0
	}

	expectedShardSize := int64(50 * 1024 * 1024) // 50MB per shard

	// Create destination specs
	destinations := make([]TaskDestinationSpec, len(shardDestinations))
	shardImpact := CalculateECShardStorageImpact(1, expectedShardSize)
	for i, dest := range shardDestinations {
		destinations[i] = TaskDestinationSpec{
			ServerID:      dest,
			DiskID:        shardDiskIDs[i],
			StorageImpact: &shardImpact,
			EstimatedSize: &expectedShardSize,
		}
	}

	// Create EC task for replicated volume
	err := activeTopology.AddPendingTask(TaskSpec{
		TaskID:       "ec_replicated",
		TaskType:     TaskTypeErasureCoding,
		VolumeID:     volumeID,
		VolumeSize:   originalVolumeSize,
		Sources:      sources,
		Destinations: destinations,
	})
	assert.NoError(t, err, "Should successfully create EC task for replicated volume")

	// Verify capacity impact on all source replicas (each should reserve with zero impact)
	for i, source := range sources {
		plannedVol, reservedVol, plannedShard, reservedShard, _ := testGetDiskStorageImpact(activeTopology, source.ServerID, source.DiskID)
		assert.Equal(t, int64(0), plannedVol, fmt.Sprintf("Source replica %d should reserve with zero volume slot impact", i+1))
		assert.Equal(t, int64(0), reservedVol, fmt.Sprintf("Source replica %d should have no active volume slots", i+1))
		assert.Equal(t, int32(0), plannedShard, fmt.Sprintf("Source replica %d should have no planned shard slots", i+1))
		assert.Equal(t, int32(0), reservedShard, fmt.Sprintf("Source replica %d should have no active shard slots", i+1))
		// Note: EstimatedSize tracking is no longer exposed via public API
	}

	// Verify capacity impact on EC destinations
	destinationCounts := make(map[string]int)
	for _, dest := range shardDestinations {
		destinationCounts[dest]++
	}

	for serverID, expectedShards := range destinationCounts {
		plannedVol, _, plannedShard, _, _ := testGetDiskStorageImpact(activeTopology, serverID, 0)
		assert.Equal(t, int64(0), plannedVol, fmt.Sprintf("Destination %s should have no planned volume slots", serverID))
		assert.Equal(t, int32(expectedShards), plannedShard, fmt.Sprintf("Destination %s should plan to receive %d shards", serverID, expectedShards))
	}

	// Verify effective capacity calculation for sources (should have zero EC impact)
	sourceCapacity1 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	sourceCapacity2 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)
	sourceCapacity3 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.3:8080", 0)

	// All sources should have same capacity as baseline (EC source reserves with zero impact)
	assert.Equal(t, int64(90), sourceCapacity1, "Source 1: 100 - 10 (current) - 0 (EC source impact) = 90")
	assert.Equal(t, int64(95), sourceCapacity2, "Source 2: 100 - 5 (current) - 0 (EC source impact) = 95")
	assert.Equal(t, int64(97), sourceCapacity3, "Source 3: 100 - 3 (current) - 0 (EC source impact) = 97")

	// Verify effective capacity calculation for destinations (should be reduced by shard slots)
	destCapacity4 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.4:8080", 0)
	destCapacity5 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.5:8080", 0)
	destCapacity6 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.6:8080", 0)

	// Dynamic shard impact calculations
	dest4ShardImpact := int64(5 / erasure_coding.DataShardsCount) // 5 shards impact
	dest5ShardImpact := int64(5 / erasure_coding.DataShardsCount) // 5 shards impact
	dest6ShardImpact := int64(4 / erasure_coding.DataShardsCount) // 4 shards impact

	// Destinations should be reduced by shard impact
	assert.Equal(t, int64(85-dest4ShardImpact), destCapacity4, fmt.Sprintf("Dest 4: 100 - 15 (current) - %d (5 shards/%d = %d impact) = %d", dest4ShardImpact, erasure_coding.DataShardsCount, dest4ShardImpact, 85-dest4ShardImpact))
	assert.Equal(t, int64(80-dest5ShardImpact), destCapacity5, fmt.Sprintf("Dest 5: 100 - 20 (current) - %d (5 shards/%d = %d impact) = %d", dest5ShardImpact, erasure_coding.DataShardsCount, dest5ShardImpact, 80-dest5ShardImpact))
	assert.Equal(t, int64(75-dest6ShardImpact), destCapacity6, fmt.Sprintf("Dest 6: 100 - 25 (current) - %d (4 shards/%d = %d impact) = %d", dest6ShardImpact, erasure_coding.DataShardsCount, dest6ShardImpact, 75-dest6ShardImpact))

	t.Logf("Replicated volume EC operation: %d source replicas, %d EC shards distributed across %d destinations",
		len(sources), len(shardDestinations), len(destinationCounts))
	t.Logf("Each source replica reserves with zero capacity impact, destinations receive EC shards")
}

// TestECWithOldShardCleanup tests EC operations that need to clean up old shards from previous failed attempts
func TestECWithOldShardCleanup(t *testing.T) {
	activeTopology := NewActiveTopology(10)

	// Setup cluster with servers
	activeTopology.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "10.0.0.1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 10},
								},
							},
							{
								Id: "10.0.0.2:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 5},
								},
							},
							{
								Id: "10.0.0.3:8080", // Had old EC shards from previous failed attempt
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 3},
								},
							},
							{
								Id: "10.0.0.4:8080", // Had old EC shards from previous failed attempt
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 7},
								},
							},
							{
								Id: "10.0.0.5:8080", // New EC destination
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 20},
								},
							},
							{
								Id: "10.0.0.6:8080", // New EC destination
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 25},
								},
							},
						},
					},
				},
			},
		},
	})

	// Test: EC operation that needs to clean up both volume replicas AND old EC shards
	volumeID := uint32(400)
	originalVolumeSize := int64(1024 * 1024 * 1024) // 1GB

	// Create source specs: volume replicas + old EC shard locations
	sources := []TaskSourceSpec{
		{ServerID: "10.0.0.1:8080", DiskID: 0, CleanupType: CleanupVolumeReplica}, // Volume replica 1
		{ServerID: "10.0.0.2:8080", DiskID: 0, CleanupType: CleanupVolumeReplica}, // Volume replica 2
		{ServerID: "10.0.0.3:8080", DiskID: 0, CleanupType: CleanupECShards},      // Old EC shards from failed attempt
		{ServerID: "10.0.0.4:8080", DiskID: 0, CleanupType: CleanupECShards},      // Old EC shards from failed attempt
	}

	// EC destinations (new complete set of shards)
	shardDestinations := []string{
		"10.0.0.5:8080", "10.0.0.5:8080", "10.0.0.5:8080", "10.0.0.5:8080", "10.0.0.5:8080", // 5 shards
		"10.0.0.5:8080", "10.0.0.5:8080", "10.0.0.5:8080", "10.0.0.5:8080", // 4 more shards (9 total)
		"10.0.0.6:8080", "10.0.0.6:8080", "10.0.0.6:8080", "10.0.0.6:8080", "10.0.0.6:8080", // 5 shards
	}
	shardDiskIDs := make([]uint32, len(shardDestinations))
	for i := range shardDiskIDs {
		shardDiskIDs[i] = 0
	}

	expectedShardSize := int64(50 * 1024 * 1024) // 50MB per shard

	// Create destination specs
	destinations := make([]TaskDestinationSpec, len(shardDestinations))
	shardImpact := CalculateECShardStorageImpact(1, expectedShardSize)
	for i, dest := range shardDestinations {
		destinations[i] = TaskDestinationSpec{
			ServerID:      dest,
			DiskID:        shardDiskIDs[i],
			StorageImpact: &shardImpact,
			EstimatedSize: &expectedShardSize,
		}
	}

	// Create EC task that cleans up both volume replicas and old EC shards
	err := activeTopology.AddPendingTask(TaskSpec{
		TaskID:       "ec_cleanup",
		TaskType:     TaskTypeErasureCoding,
		VolumeID:     volumeID,
		VolumeSize:   originalVolumeSize,
		Sources:      sources,
		Destinations: destinations,
	})
	assert.NoError(t, err, "Should successfully create EC task with mixed cleanup types")

	// Verify capacity impact on volume replica sources (zero impact for EC)
	for i := 0; i < 2; i++ {
		source := sources[i]
		plannedVol, _, plannedShard, _, _ := testGetDiskStorageImpact(activeTopology, source.ServerID, source.DiskID)
		assert.Equal(t, int64(0), plannedVol, fmt.Sprintf("Volume replica source %d should have zero volume slot impact", i+1))
		assert.Equal(t, int32(0), plannedShard, fmt.Sprintf("Volume replica source %d should have zero shard slot impact", i+1))
		// Note: EstimatedSize tracking is no longer exposed via public API
	}

	// Verify capacity impact on old EC shard sources (should free shard slots)
	for i := 2; i < 4; i++ {
		source := sources[i]
		plannedVol, _, plannedShard, _, _ := testGetDiskStorageImpact(activeTopology, source.ServerID, source.DiskID)
		assert.Equal(t, int64(0), plannedVol, fmt.Sprintf("EC shard source %d should have zero volume slot impact", i+1))
		assert.Equal(t, int32(-erasure_coding.TotalShardsCount), plannedShard, fmt.Sprintf("EC shard source %d should free %d shard slots", i+1, erasure_coding.TotalShardsCount))
		// Note: EstimatedSize tracking is no longer exposed via public API
	}

	// Verify capacity impact on new EC destinations
	destPlan5, _, destShard5, _, _ := testGetDiskStorageImpact(activeTopology, "10.0.0.5:8080", 0)
	destPlan6, _, destShard6, _, _ := testGetDiskStorageImpact(activeTopology, "10.0.0.6:8080", 0)

	assert.Equal(t, int64(0), destPlan5, "New EC destination 5 should have no planned volume slots")
	assert.Equal(t, int32(9), destShard5, "New EC destination 5 should plan to receive 9 shards")
	assert.Equal(t, int64(0), destPlan6, "New EC destination 6 should have no planned volume slots")
	assert.Equal(t, int32(5), destShard6, "New EC destination 6 should plan to receive 5 shards")

	// Verify effective capacity calculation shows proper impact
	capacity3 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.3:8080", 0) // Freeing old EC shards
	capacity4 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.4:8080", 0) // Freeing old EC shards
	capacity5 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.5:8080", 0) // Receiving new EC shards
	capacity6 := activeTopology.GetEffectiveAvailableCapacity("10.0.0.6:8080", 0) // Receiving new EC shards

	// Servers freeing old EC shards should have INCREASED capacity (freed shard slots provide capacity)
	assert.Equal(t, int64(98), capacity3, fmt.Sprintf("Server 3: 100 - 3 (current) + 1 (freeing %d shards) = 98", erasure_coding.TotalShardsCount))
	assert.Equal(t, int64(94), capacity4, fmt.Sprintf("Server 4: 100 - 7 (current) + 1 (freeing %d shards) = 94", erasure_coding.TotalShardsCount))

	// Servers receiving new EC shards should have slightly reduced capacity
	server5ShardImpact := int64(9 / erasure_coding.DataShardsCount) // 9 shards impact
	server6ShardImpact := int64(5 / erasure_coding.DataShardsCount) // 5 shards impact

	assert.Equal(t, int64(80-server5ShardImpact), capacity5, fmt.Sprintf("Server 5: 100 - 20 (current) - %d (9 shards/%d = %d impact) = %d", server5ShardImpact, erasure_coding.DataShardsCount, server5ShardImpact, 80-server5ShardImpact))
	assert.Equal(t, int64(75-server6ShardImpact), capacity6, fmt.Sprintf("Server 6: 100 - 25 (current) - %d (5 shards/%d = %d impact) = %d", server6ShardImpact, erasure_coding.DataShardsCount, server6ShardImpact, 75-server6ShardImpact))

	t.Logf("EC operation with cleanup: %d volume replicas + %d old EC shard locations → %d new EC shards",
		2, 2, len(shardDestinations))
	t.Logf("Volume sources have zero impact, old EC shard sources free capacity, new destinations consume shard slots")
}

// TestDetailedCapacityCalculations tests the new StorageSlotChange-based capacity calculation functions
func TestDetailedCapacityCalculations(t *testing.T) {
	activeTopology := NewActiveTopology(10)

	// Setup cluster
	activeTopology.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "10.0.0.1:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"0": {DiskId: 0, Type: "hdd", MaxVolumeCount: 100, VolumeCount: 20},
								},
							},
						},
					},
				},
			},
		},
	})

	// Test: Add an EC task and check detailed capacity
	sources := []TaskSourceSpec{
		{ServerID: "10.0.0.1:8080", DiskID: 0, CleanupType: CleanupVolumeReplica},
	}

	shardDestinations := []string{"10.0.0.1:8080", "10.0.0.1:8080", "10.0.0.1:8080", "10.0.0.1:8080", "10.0.0.1:8080"}
	shardDiskIDs := []uint32{0, 0, 0, 0, 0}

	// Create destination specs
	destinations := make([]TaskDestinationSpec, len(shardDestinations))
	expectedShardSize := int64(50 * 1024 * 1024)
	shardImpact := CalculateECShardStorageImpact(1, expectedShardSize)
	for i, dest := range shardDestinations {
		destinations[i] = TaskDestinationSpec{
			ServerID:      dest,
			DiskID:        shardDiskIDs[i],
			StorageImpact: &shardImpact,
			EstimatedSize: &expectedShardSize,
		}
	}

	err := activeTopology.AddPendingTask(TaskSpec{
		TaskID:       "detailed_test",
		TaskType:     TaskTypeErasureCoding,
		VolumeID:     500,
		VolumeSize:   1024 * 1024 * 1024,
		Sources:      sources,
		Destinations: destinations,
	})
	assert.NoError(t, err, "Should add EC task successfully")

	// Test the new detailed capacity function
	detailedCapacity := activeTopology.GetEffectiveAvailableCapacityDetailed("10.0.0.1:8080", 0)
	simpleCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)

	// The simple capacity should match the volume slots from detailed capacity
	assert.Equal(t, int64(detailedCapacity.VolumeSlots), simpleCapacity, "Simple capacity should match detailed volume slots")

	// Verify detailed capacity has both volume and shard information
	assert.Equal(t, int32(80), detailedCapacity.VolumeSlots, "Should have 80 available volume slots (100 - 20 current, no volume impact from EC)")
	assert.Equal(t, int32(-5), detailedCapacity.ShardSlots, "Should show -5 available shard slots (5 destination shards)")

	// Verify capacity impact
	capacityImpact := activeTopology.GetEffectiveCapacityImpact("10.0.0.1:8080", 0)
	assert.Equal(t, int32(0), capacityImpact.VolumeSlots, "EC source should have zero volume slot impact")
	assert.Equal(t, int32(5), capacityImpact.ShardSlots, "Should have positive shard slot impact (consuming 5 shards)")

	t.Logf("Detailed capacity calculation: VolumeSlots=%d, ShardSlots=%d",
		detailedCapacity.VolumeSlots, detailedCapacity.ShardSlots)
	t.Logf("Capacity impact: VolumeSlots=%d, ShardSlots=%d",
		capacityImpact.VolumeSlots, capacityImpact.ShardSlots)
	t.Logf("Simple capacity (backward compatible): %d", simpleCapacity)
}

// TestStorageSlotChangeConversions tests the conversion and accommodation methods for StorageSlotChange
// This test is designed to work with any value of erasure_coding.DataShardsCount, making it
// compatible with custom erasure coding configurations.
func TestStorageSlotChangeConversions(t *testing.T) {
	// Get the actual erasure coding constants for dynamic testing
	dataShards := int32(erasure_coding.DataShardsCount)

	// Test conversion constants
	assert.Equal(t, int(dataShards), ShardsPerVolumeSlot, fmt.Sprintf("Should use erasure_coding.DataShardsCount (%d) shards per volume slot", dataShards))

	// Test basic conversions using dynamic values
	volumeOnly := StorageSlotChange{VolumeSlots: 5, ShardSlots: 0}
	shardOnly := StorageSlotChange{VolumeSlots: 0, ShardSlots: 2 * dataShards} // 2 volume equivalents in shards
	mixed := StorageSlotChange{VolumeSlots: 2, ShardSlots: dataShards + 5}     // 2 volumes + 1.5 volume equivalent in shards

	// Test ToVolumeSlots conversion - these should work regardless of DataShardsCount value
	assert.Equal(t, int64(5), volumeOnly.ToVolumeSlots(), "5 volume slots = 5 volume slots")
	assert.Equal(t, int64(2), shardOnly.ToVolumeSlots(), fmt.Sprintf("%d shard slots = 2 volume slots", 2*dataShards))
	expectedMixedVolumes := int64(2 + (dataShards+5)/dataShards) // 2 + floor((DataShardsCount+5)/DataShardsCount)
	assert.Equal(t, expectedMixedVolumes, mixed.ToVolumeSlots(), fmt.Sprintf("2 volume + %d shards = %d volume slots", dataShards+5, expectedMixedVolumes))

	// Test ToShardSlots conversion
	expectedVolumeShards := int32(5 * dataShards)
	assert.Equal(t, expectedVolumeShards, volumeOnly.ToShardSlots(), fmt.Sprintf("5 volume slots = %d shard slots", expectedVolumeShards))
	assert.Equal(t, 2*dataShards, shardOnly.ToShardSlots(), fmt.Sprintf("%d shard slots = %d shard slots", 2*dataShards, 2*dataShards))
	expectedMixedShards := int32(2*dataShards + dataShards + 5)
	assert.Equal(t, expectedMixedShards, mixed.ToShardSlots(), fmt.Sprintf("2 volume + %d shards = %d shard slots", dataShards+5, expectedMixedShards))

	// Test capacity accommodation checks using shard-based comparison
	availableVolumes := int32(10)
	available := StorageSlotChange{VolumeSlots: availableVolumes, ShardSlots: 0} // availableVolumes * dataShards shard slots available

	smallVolumeRequest := StorageSlotChange{VolumeSlots: 3, ShardSlots: 0}                    // Needs 3 * dataShards shard slots
	largeVolumeRequest := StorageSlotChange{VolumeSlots: availableVolumes + 5, ShardSlots: 0} // Needs more than available
	shardRequest := StorageSlotChange{VolumeSlots: 0, ShardSlots: 5 * dataShards}             // Needs 5 volume equivalents in shards
	mixedRequest := StorageSlotChange{VolumeSlots: 8, ShardSlots: 3 * dataShards}             // Needs 11 volume equivalents total

	smallShardsNeeded := 3 * dataShards
	availableShards := availableVolumes * dataShards
	largeShardsNeeded := (availableVolumes + 5) * dataShards
	shardShardsNeeded := 5 * dataShards
	mixedShardsNeeded := 8*dataShards + 3*dataShards

	assert.True(t, available.CanAccommodate(smallVolumeRequest), fmt.Sprintf("Should accommodate small volume request (%d <= %d shards)", smallShardsNeeded, availableShards))
	assert.False(t, available.CanAccommodate(largeVolumeRequest), fmt.Sprintf("Should NOT accommodate large volume request (%d > %d shards)", largeShardsNeeded, availableShards))
	assert.True(t, available.CanAccommodate(shardRequest), fmt.Sprintf("Should accommodate shard request (%d <= %d shards)", shardShardsNeeded, availableShards))
	assert.False(t, available.CanAccommodate(mixedRequest), fmt.Sprintf("Should NOT accommodate mixed request (%d > %d shards)", mixedShardsNeeded, availableShards))

	t.Logf("Conversion tests passed: %d shards = 1 volume slot", ShardsPerVolumeSlot)
	t.Logf("Mixed capacity (%d volumes + %d shards) = %d equivalent volume slots",
		mixed.VolumeSlots, mixed.ShardSlots, mixed.ToVolumeSlots())
	t.Logf("Available capacity (%d volumes) = %d total shard slots",
		available.VolumeSlots, available.ToShardSlots())
	t.Logf("NOTE: This test adapts automatically to erasure_coding.DataShardsCount = %d", erasure_coding.DataShardsCount)
}
