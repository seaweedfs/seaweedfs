package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/assert"
)

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

	// Test TotalImpact
	impact1 := StorageSlotChange{VolumeSlots: 5, ShardSlots: 10}
	assert.Equal(t, int64(6), impact1.TotalImpact(), "TotalImpact should be 5 + 10/10 = 6")

	impact2 := StorageSlotChange{VolumeSlots: -2, ShardSlots: 25}
	assert.Equal(t, int64(0), impact2.TotalImpact(), "TotalImpact should be -2 + 25/10 = 0")

	impact3 := StorageSlotChange{VolumeSlots: 3, ShardSlots: 7}
	assert.Equal(t, int64(3), impact3.TotalImpact(), "TotalImpact should be 3 + 7/10 = 3 (integer division)")
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
	assert.Equal(t, int32(1), ecTargetChange.ShardSlots, "EC target typically gains 1 shard slot")

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
	shardCount := int32(2)
	expectedShardSize := int64(50 * 1024 * 1024)    // 50MB per shard
	originalVolumeSize := int64(1024 * 1024 * 1024) // 1GB original

	err := activeTopology.AddPendingECShardTask("ec_test", 100, sourceServer, sourceDisk,
		shardDestinations, shardDiskIDs, shardCount, expectedShardSize, originalVolumeSize)
	assert.NoError(t, err, "Should add EC shard task successfully")

	// Test 4: Check storage impact on source (EC reserves with zero impact)
	pendingVol, activeVol, pendingShard, activeShard, estimatedSize := activeTopology.GetDiskStorageImpact("10.0.0.1:8080", 0)
	assert.Equal(t, int64(0), pendingVol, "Source should show 0 pending volume slot (EC reserves with zero impact)")
	assert.Equal(t, int64(0), activeVol, "Source should show 0 active volume slots")
	assert.Equal(t, int32(0), pendingShard, "Source should show 0 pending shard slots")
	assert.Equal(t, int32(0), activeShard, "Source should show 0 active shard slots")
	assert.Equal(t, originalVolumeSize, estimatedSize, "Should track original volume size")

	// Test 5: Check storage impact on target (should gain shards)
	targetPendingVol, _, targetPendingShard, _, _ := activeTopology.GetDiskStorageImpact("10.0.0.2:8080", 0)
	assert.Equal(t, int64(0), targetPendingVol, "Target should show 0 pending volume slots (EC shards don't use volume slots)")
	assert.Equal(t, int32(2), targetPendingShard, "Target should show 2 pending shard slots")

	// Test 6: Check effective capacity calculation (EC source reserves with zero StorageSlotChange)
	sourceCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	targetCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)

	// Source: 15 original available (EC source reserves with zero StorageSlotChange impact)
	assert.Equal(t, int64(15), sourceCapacity, "Source should have 15 available slots (EC source has zero StorageSlotChange impact)")

	// Target: 7 original available - (2 shards / 10) = 7 (since 2/10 rounds down to 0)
	assert.Equal(t, int64(7), targetCapacity, "Target should have 7 available slots (minimal shard impact)")

	// Test 7: Add traditional balance task for comparison
	balanceSourceChange, balanceTargetChange := CalculateTaskStorageImpact(TaskTypeBalance, 512*1024*1024)
	activeTopology.addPendingTaskWithStorageInfo("balance_test", TaskTypeBalance, 101,
		"10.0.0.1:8080", 0, "10.0.0.2:8080", 0, balanceSourceChange, balanceTargetChange, 512*1024*1024)

	// Check updated impacts
	finalSourcePendingVol, _, _, _, _ := activeTopology.GetDiskStorageImpact("10.0.0.1:8080", 0)
	finalTargetPendingVol, _, finalTargetPendingShard, _, _ := activeTopology.GetDiskStorageImpact("10.0.0.2:8080", 0)

	assert.Equal(t, int64(-1), finalSourcePendingVol, "Source should show -1 pending volume slot (EC: 0, Balance: -1)")
	assert.Equal(t, int64(1), finalTargetPendingVol, "Target should show 1 pending volume slot (Balance: +1)")
	assert.Equal(t, int32(2), finalTargetPendingShard, "Target should still show 2 pending shard slots (EC shards)")
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
	activeTopology.addPendingTaskWithStorageInfo("shard_test_1", TaskTypeErasureCoding, 100,
		"", 0, "10.0.0.1:8080", 0,
		StorageSlotChange{VolumeSlots: 0, ShardSlots: 0}, // Source change (not applicable here)
		StorageSlotChange{VolumeSlots: 0, ShardSlots: 5}, // Target gains 5 shards
		100*1024*1024)

	// Capacity should be reduced by pending tasks via StorageSlotChange
	capacityAfterShards := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	assert.Equal(t, int64(90), capacityAfterShards, "5 shard slots should not impact volume capacity significantly (5/10 = 0)")

	// Add more shards to reach threshold
	activeTopology.addPendingTaskWithStorageInfo("shard_test_2", TaskTypeErasureCoding, 101,
		"", 0, "10.0.0.1:8080", 0,
		StorageSlotChange{VolumeSlots: 0, ShardSlots: 0},
		StorageSlotChange{VolumeSlots: 0, ShardSlots: 10}, // Target gains 10 more shards (15 total)
		100*1024*1024)

	// Capacity should be reduced by 15/10 = 1 volume slot
	capacityAfterMoreShards := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	assert.Equal(t, int64(89), capacityAfterMoreShards, "15 shard slots should consume 1 volume slot equivalent (15/10 = 1)")

	// Add a full volume task
	activeTopology.addPendingTaskWithStorageInfo("volume_test", TaskTypeBalance, 102,
		"", 0, "10.0.0.1:8080", 0,
		StorageSlotChange{VolumeSlots: 0, ShardSlots: 0},
		StorageSlotChange{VolumeSlots: 1, ShardSlots: 0}, // Target gains 1 volume
		1024*1024*1024)

	// Capacity should be reduced by 1 more volume slot
	finalCapacity := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	assert.Equal(t, int64(88), finalCapacity, "1 volume + 15 shard slots should consume 2 volume slots total")

	// Verify the detailed storage impact
	plannedVol, reservedVol, plannedShard, reservedShard, _ := activeTopology.GetDiskStorageImpact("10.0.0.1:8080", 0)
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
	assert.Equal(t, StorageSlotChange{VolumeSlots: 0, ShardSlots: 1}, targetChange, "Target change only represents typical single shard")

	// Proper way: Use AddPendingECShardTask for multiple targets
	sourceServer := "10.0.0.1:8080"
	sourceDisk := uint32(0)

	// EC typically distributes 14 shards across multiple targets
	shardDestinations := []string{
		"10.0.0.2:8080", "10.0.0.2:8080", "10.0.0.2:8080", "10.0.0.2:8080", "10.0.0.2:8080", // 5 shards to target 1
		"10.0.0.3:8080", "10.0.0.3:8080", "10.0.0.3:8080", "10.0.0.3:8080", "10.0.0.3:8080", // 5 shards to target 2
		"10.0.0.4:8080", "10.0.0.4:8080", "10.0.0.4:8080", "10.0.0.4:8080", // 4 shards to target 3
	}
	shardDiskIDs := make([]uint32, len(shardDestinations))
	for i := range shardDiskIDs {
		shardDiskIDs[i] = 0
	}

	err := activeTopology.AddPendingECShardTask("ec_multi_target", 200, sourceServer, sourceDisk,
		shardDestinations, shardDiskIDs, int32(len(shardDestinations)), 50*1024*1024, 1*1024*1024*1024)
	assert.NoError(t, err, "Should add multi-target EC task successfully")

	// Verify source impact (EC reserves with zero StorageSlotChange)
	sourcePlannedVol, sourceReservedVol, sourcePlannedShard, sourceReservedShard, sourceEstimatedSize := activeTopology.GetDiskStorageImpact("10.0.0.1:8080", 0)
	assert.Equal(t, int64(0), sourcePlannedVol, "Source should reserve with zero volume slot impact")
	assert.Equal(t, int64(0), sourceReservedVol, "Source should not have reserved capacity yet")
	assert.Equal(t, int32(0), sourcePlannedShard, "Source should not have planned shard impact")
	assert.Equal(t, int32(0), sourceReservedShard, "Source should not have reserved shard impact")
	assert.Equal(t, int64(1*1024*1024*1024), sourceEstimatedSize, "Source should track original volume size")

	// Verify target impacts (planned, not yet reserved)
	target1PlannedVol, target1ReservedVol, target1PlannedShard, target1ReservedShard, _ := activeTopology.GetDiskStorageImpact("10.0.0.2:8080", 0)
	target2PlannedVol, target2ReservedVol, target2PlannedShard, target2ReservedShard, _ := activeTopology.GetDiskStorageImpact("10.0.0.3:8080", 0)
	target3PlannedVol, target3ReservedVol, target3PlannedShard, target3ReservedShard, _ := activeTopology.GetDiskStorageImpact("10.0.0.4:8080", 0)

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

	assert.Equal(t, int64(40), sourceCapacity, "Source: 40 (EC source reserves with zero StorageSlotChange impact)")
	assert.Equal(t, int64(25), target1Capacity, "Target 1: 25 - 0 (5 shards < 10) = 25")
	assert.Equal(t, int64(32), target2Capacity, "Target 2: 32 - 0 (5 shards < 10) = 32")
	assert.Equal(t, int64(23), target3Capacity, "Target 3: 23 - 0 (4 shards < 10) = 23")

	t.Logf("EC operation distributed %d shards across %d targets", len(shardDestinations), 3)
	t.Logf("Capacity impacts: EC source reserves with zero impact, Targets minimal (shards < 10)")
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
	balanceSourceChange, balanceTargetChange := CalculateTaskStorageImpact(TaskTypeBalance, 1*1024*1024*1024)
	activeTopology.addPendingTaskWithStorageInfo("balance_test", TaskTypeBalance, 123,
		"10.0.0.1:8080", 0, "10.0.0.2:8080", 0, balanceSourceChange, balanceTargetChange, 1*1024*1024*1024)

	sourceCapacityAfterPending := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	targetCapacityAfterPending := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)
	assert.Equal(t, int64(11), sourceCapacityAfterPending, "Source should gain capacity from pending balance task (balance source frees 1 slot)")
	assert.Equal(t, int64(9), targetCapacityAfterPending, "Target should consume capacity from pending task (balance reserves 1 slot)")

	// Verify planning capacity considers the same pending tasks
	planningDisks := activeTopology.GetDisksForPlanning(TaskTypeBalance, "", 1)
	assert.Len(t, planningDisks, 2, "Both disks should be available for planning")

	// Step 2: Assign task (capacity already reserved by pending task)
	err := activeTopology.AssignTask("balance_test")
	assert.NoError(t, err, "Should assign task successfully")

	sourceCapacityAfterAssign := activeTopology.GetEffectiveAvailableCapacity("10.0.0.1:8080", 0)
	targetCapacityAfterAssign := activeTopology.GetEffectiveAvailableCapacity("10.0.0.2:8080", 0)

	assert.Equal(t, int64(11), sourceCapacityAfterAssign, "Source capacity should remain same (already accounted by pending)")
	assert.Equal(t, int64(9), targetCapacityAfterAssign, "Target capacity should remain same (already accounted by pending)")

	// Verify storage impact during assignment (moved from planned to reserved)
	sourcePlanned, sourceReserved, _, _, _ := activeTopology.GetDiskStorageImpact("10.0.0.1:8080", 0)
	targetPlanned, targetReserved, _, _, _ := activeTopology.GetDiskStorageImpact("10.0.0.2:8080", 0)
	assert.Equal(t, int64(0), sourcePlanned, "Source should have no planned tasks (moved to reserved)")
	assert.Equal(t, int64(-1), sourceReserved, "Source should have reserved -1 volume impact")
	assert.Equal(t, int64(0), targetPlanned, "Target should have no planned tasks (moved to reserved)")
	assert.Equal(t, int64(1), targetReserved, "Target should have reserved 1 slot")

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
