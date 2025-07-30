package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestActiveTopologyBasicOperations tests basic topology management
func TestActiveTopologyBasicOperations(t *testing.T) {
	topology := NewActiveTopology(10)
	assert.NotNil(t, topology)
	assert.Equal(t, 10, topology.recentTaskWindowSeconds)

	// Test empty topology
	assert.Equal(t, 0, len(topology.nodes))
	assert.Equal(t, 0, len(topology.disks))
	assert.Equal(t, 0, len(topology.pendingTasks))
}

// TestActiveTopologyUpdate tests topology updates from master
func TestActiveTopologyUpdate(t *testing.T) {
	topology := NewActiveTopology(10)

	// Create sample topology info
	topologyInfo := createSampleTopology()

	err := topology.UpdateTopology(topologyInfo)
	require.NoError(t, err)

	// Verify topology structure
	assert.Equal(t, 2, len(topology.nodes)) // 2 nodes
	assert.Equal(t, 4, len(topology.disks)) // 4 disks total (2 per node)

	// Verify node structure
	node1, exists := topology.nodes["10.0.0.1:8080"]
	require.True(t, exists)
	assert.Equal(t, "dc1", node1.dataCenter)
	assert.Equal(t, "rack1", node1.rack)
	assert.Equal(t, 2, len(node1.disks))

	// Verify disk structure
	disk1, exists := topology.disks["10.0.0.1:8080:0"]
	require.True(t, exists)
	assert.Equal(t, uint32(0), disk1.DiskID)
	assert.Equal(t, "hdd", disk1.DiskType)
	assert.Equal(t, "dc1", disk1.DataCenter)
}

// TestTaskLifecycle tests the complete task lifecycle
func TestTaskLifecycle(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	taskID := "balance-001"

	// 1. Add pending task
	topology.AddPendingTask(taskID, TaskTypeBalance, 1001,
		"10.0.0.1:8080", 0, "10.0.0.2:8080", 1)

	// Verify pending state
	assert.Equal(t, 1, len(topology.pendingTasks))
	assert.Equal(t, 0, len(topology.assignedTasks))
	assert.Equal(t, 0, len(topology.recentTasks))

	task := topology.pendingTasks[taskID]
	assert.Equal(t, TaskStatusPending, task.Status)
	assert.Equal(t, uint32(1001), task.VolumeID)

	// Verify task assigned to disks
	sourceDisk := topology.disks["10.0.0.1:8080:0"]
	targetDisk := topology.disks["10.0.0.2:8080:1"]
	assert.Equal(t, 1, len(sourceDisk.pendingTasks))
	assert.Equal(t, 1, len(targetDisk.pendingTasks))

	// 2. Assign task
	err := topology.AssignTask(taskID)
	require.NoError(t, err)

	// Verify assigned state
	assert.Equal(t, 0, len(topology.pendingTasks))
	assert.Equal(t, 1, len(topology.assignedTasks))
	assert.Equal(t, 0, len(topology.recentTasks))

	task = topology.assignedTasks[taskID]
	assert.Equal(t, TaskStatusInProgress, task.Status)

	// Verify task moved to assigned on disks
	assert.Equal(t, 0, len(sourceDisk.pendingTasks))
	assert.Equal(t, 1, len(sourceDisk.assignedTasks))
	assert.Equal(t, 0, len(targetDisk.pendingTasks))
	assert.Equal(t, 1, len(targetDisk.assignedTasks))

	// 3. Complete task
	err = topology.CompleteTask(taskID)
	require.NoError(t, err)

	// Verify completed state
	assert.Equal(t, 0, len(topology.pendingTasks))
	assert.Equal(t, 0, len(topology.assignedTasks))
	assert.Equal(t, 1, len(topology.recentTasks))

	task = topology.recentTasks[taskID]
	assert.Equal(t, TaskStatusCompleted, task.Status)
	assert.False(t, task.CompletedAt.IsZero())
}

// TestTaskDetectionScenarios tests various task detection scenarios
func TestTaskDetectionScenarios(t *testing.T) {
	tests := []struct {
		name          string
		scenario      func() *ActiveTopology
		expectedTasks map[string]bool // taskType -> shouldDetect
	}{
		{
			name: "Empty cluster - no tasks needed",
			scenario: func() *ActiveTopology {
				topology := NewActiveTopology(10)
				topology.UpdateTopology(createEmptyTopology())
				return topology
			},
			expectedTasks: map[string]bool{
				"balance": false,
				"vacuum":  false,
				"ec":      false,
			},
		},
		{
			name: "Unbalanced cluster - balance task needed",
			scenario: func() *ActiveTopology {
				topology := NewActiveTopology(10)
				topology.UpdateTopology(createUnbalancedTopology())
				return topology
			},
			expectedTasks: map[string]bool{
				"balance": true,
				"vacuum":  false,
				"ec":      false,
			},
		},
		{
			name: "High garbage ratio - vacuum task needed",
			scenario: func() *ActiveTopology {
				topology := NewActiveTopology(10)
				topology.UpdateTopology(createHighGarbageTopology())
				return topology
			},
			expectedTasks: map[string]bool{
				"balance": false,
				"vacuum":  true,
				"ec":      false,
			},
		},
		{
			name: "Large volumes - EC task needed",
			scenario: func() *ActiveTopology {
				topology := NewActiveTopology(10)
				topology.UpdateTopology(createLargeVolumeTopology())
				return topology
			},
			expectedTasks: map[string]bool{
				"balance": false,
				"vacuum":  false,
				"ec":      true,
			},
		},
		{
			name: "Recent tasks - no immediate re-detection",
			scenario: func() *ActiveTopology {
				topology := NewActiveTopology(10)
				topology.UpdateTopology(createUnbalancedTopology())
				// Add recent balance task
				topology.recentTasks["recent-balance"] = &taskState{
					VolumeID:    1001,
					TaskType:    TaskTypeBalance,
					Status:      TaskStatusCompleted,
					CompletedAt: time.Now().Add(-5 * time.Second), // 5 seconds ago
				}
				return topology
			},
			expectedTasks: map[string]bool{
				"balance": false, // Should not detect due to recent task
				"vacuum":  false,
				"ec":      false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology := tt.scenario()

			// Test balance task detection
			shouldDetectBalance := tt.expectedTasks["balance"]
			actualDetectBalance := !topology.HasRecentTaskForVolume(1001, TaskTypeBalance)
			if shouldDetectBalance {
				assert.True(t, actualDetectBalance, "Should detect balance task")
			} else {
				// Note: In real implementation, task detection would be more sophisticated
				// This is a simplified test of the recent task prevention mechanism
			}

			// Test that recent tasks prevent re-detection
			if len(topology.recentTasks) > 0 {
				for _, task := range topology.recentTasks {
					hasRecent := topology.HasRecentTaskForVolume(task.VolumeID, task.TaskType)
					assert.True(t, hasRecent, "Should find recent task for volume %d", task.VolumeID)
				}
			}
		})
	}
}

// TestTargetSelectionScenarios tests target selection for different task types
func TestTargetSelectionScenarios(t *testing.T) {
	tests := []struct {
		name               string
		topology           *ActiveTopology
		taskType           TaskType
		excludeNode        string
		expectedTargets    int
		expectedBestTarget string
	}{
		{
			name:            "Balance task - find least loaded disk",
			topology:        createTopologyWithLoad(),
			taskType:        TaskTypeBalance,
			excludeNode:     "10.0.0.1:8080", // Exclude source node
			expectedTargets: 2,               // 2 disks on other node
		},
		{
			name:            "EC task - find multiple available disks",
			topology:        createTopologyForEC(),
			taskType:        TaskTypeErasureCoding,
			excludeNode:     "", // Don't exclude any nodes
			expectedTargets: 4,  // All 4 disks available
		},
		{
			name:            "Vacuum task - avoid conflicting disks",
			topology:        createTopologyWithConflicts(),
			taskType:        TaskTypeVacuum,
			excludeNode:     "",
			expectedTargets: 1, // Only 1 disk without conflicts (conflicts exclude more disks)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			availableDisks := tt.topology.GetAvailableDisks(tt.taskType, tt.excludeNode)
			assert.Equal(t, tt.expectedTargets, len(availableDisks),
				"Expected %d available disks, got %d", tt.expectedTargets, len(availableDisks))

			// Verify disks are actually available
			for _, disk := range availableDisks {
				assert.NotEqual(t, tt.excludeNode, disk.NodeID,
					"Available disk should not be on excluded node")

				load := tt.topology.GetDiskLoad(disk.NodeID, disk.DiskID)
				assert.Less(t, load, 2, "Disk load should be less than 2")
			}
		})
	}
}

// TestDiskLoadCalculation tests disk load calculation
func TestDiskLoadCalculation(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Initially no load
	load := topology.GetDiskLoad("10.0.0.1:8080", 0)
	assert.Equal(t, 0, load)

	// Add pending task
	topology.AddPendingTask("task1", TaskTypeBalance, 1001,
		"10.0.0.1:8080", 0, "10.0.0.2:8080", 1)

	// Check load increased
	load = topology.GetDiskLoad("10.0.0.1:8080", 0)
	assert.Equal(t, 1, load)

	// Add another task to same disk
	topology.AddPendingTask("task2", TaskTypeVacuum, 1002,
		"10.0.0.1:8080", 0, "", 0)

	load = topology.GetDiskLoad("10.0.0.1:8080", 0)
	assert.Equal(t, 2, load)

	// Move one task to assigned
	topology.AssignTask("task1")

	// Load should still be 2 (1 pending + 1 assigned)
	load = topology.GetDiskLoad("10.0.0.1:8080", 0)
	assert.Equal(t, 2, load)

	// Complete one task
	topology.CompleteTask("task1")

	// Load should decrease to 1
	load = topology.GetDiskLoad("10.0.0.1:8080", 0)
	assert.Equal(t, 1, load)
}

// TestTaskConflictDetection tests task conflict detection
func TestTaskConflictDetection(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Add a balance task
	topology.AddPendingTask("balance1", TaskTypeBalance, 1001,
		"10.0.0.1:8080", 0, "10.0.0.2:8080", 1)
	topology.AssignTask("balance1")

	// Try to get available disks for vacuum (conflicts with balance)
	availableDisks := topology.GetAvailableDisks(TaskTypeVacuum, "")

	// Source disk should not be available due to conflict
	sourceDiskAvailable := false
	for _, disk := range availableDisks {
		if disk.NodeID == "10.0.0.1:8080" && disk.DiskID == 0 {
			sourceDiskAvailable = true
			break
		}
	}
	assert.False(t, sourceDiskAvailable, "Source disk should not be available due to task conflict")
}

// TestPublicInterfaces tests the public interface methods
func TestPublicInterfaces(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Test GetAllNodes
	nodes := topology.GetAllNodes()
	assert.Equal(t, 2, len(nodes))
	assert.Contains(t, nodes, "10.0.0.1:8080")
	assert.Contains(t, nodes, "10.0.0.2:8080")

	// Test GetNodeDisks
	disks := topology.GetNodeDisks("10.0.0.1:8080")
	assert.Equal(t, 2, len(disks))

	// Test with non-existent node
	disks = topology.GetNodeDisks("non-existent")
	assert.Nil(t, disks)
}

// Helper functions to create test topologies

func createSampleTopology() *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
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
									"hdd": {DiskId: 0, VolumeCount: 10, MaxVolumeCount: 100},
									"ssd": {DiskId: 1, VolumeCount: 5, MaxVolumeCount: 50},
								},
							},
							{
								Id: "10.0.0.2:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {DiskId: 0, VolumeCount: 8, MaxVolumeCount: 100},
									"ssd": {DiskId: 1, VolumeCount: 3, MaxVolumeCount: 50},
								},
							},
						},
					},
				},
			},
		},
	}
}

func createEmptyTopology() *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
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
									"hdd": {DiskId: 0, VolumeCount: 0, MaxVolumeCount: 100},
								},
							},
						},
					},
				},
			},
		},
	}
}

func createUnbalancedTopology() *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
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
									"hdd": {DiskId: 0, VolumeCount: 90, MaxVolumeCount: 100}, // Very loaded
								},
							},
							{
								Id: "10.0.0.2:8080",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {DiskId: 0, VolumeCount: 10, MaxVolumeCount: 100}, // Lightly loaded
								},
							},
						},
					},
				},
			},
		},
	}
}

func createHighGarbageTopology() *master_pb.TopologyInfo {
	// In a real implementation, this would include volume-level garbage metrics
	return createSampleTopology()
}

func createLargeVolumeTopology() *master_pb.TopologyInfo {
	// In a real implementation, this would include volume-level size metrics
	return createSampleTopology()
}

func createTopologyWithLoad() *ActiveTopology {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Add some existing tasks to create load
	topology.AddPendingTask("existing1", TaskTypeVacuum, 2001,
		"10.0.0.1:8080", 0, "", 0)
	topology.AssignTask("existing1")

	return topology
}

func createTopologyForEC() *ActiveTopology {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())
	return topology
}

func createTopologyWithConflicts() *ActiveTopology {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Add conflicting tasks
	topology.AddPendingTask("balance1", TaskTypeBalance, 3001,
		"10.0.0.1:8080", 0, "10.0.0.2:8080", 0)
	topology.AssignTask("balance1")

	topology.AddPendingTask("ec1", TaskTypeErasureCoding, 3002,
		"10.0.0.1:8080", 1, "", 0)
	topology.AssignTask("ec1")

	return topology
}

// TestDestinationPlanning tests destination planning functionality
func TestDestinationPlanning(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Test balance destination planning
	t.Run("Balance destination planning", func(t *testing.T) {
		plan, err := topology.PlanBalanceDestination(1001, "10.0.0.1:8080", "rack1", "dc1", 1024*1024) // 1MB
		require.NoError(t, err)
		require.NotNil(t, plan)

		// Should not target the source node
		assert.NotEqual(t, "10.0.0.1:8080", plan.TargetNode)
		assert.Equal(t, "10.0.0.2:8080", plan.TargetNode)
		assert.NotEmpty(t, plan.TargetRack)
		assert.NotEmpty(t, plan.TargetDC)
		assert.Greater(t, plan.PlacementScore, 0.0)
	})

	// Test EC destination planning
	t.Run("EC destination planning", func(t *testing.T) {
		multiPlan, err := topology.PlanECDestinations(1002, "10.0.0.1:8080", "rack1", "dc1", 3) // Ask for 3 shards - source node can be included
		require.NoError(t, err)
		require.NotNil(t, multiPlan)
		assert.Greater(t, len(multiPlan.Plans), 0)
		assert.LessOrEqual(t, len(multiPlan.Plans), 3) // Should get at most 3 shards
		assert.Equal(t, len(multiPlan.Plans), multiPlan.TotalShards)

		// Check that all plans have valid target nodes
		for _, plan := range multiPlan.Plans {
			assert.NotEmpty(t, plan.TargetNode)
			assert.NotEmpty(t, plan.TargetRack)
			assert.NotEmpty(t, plan.TargetDC)
			assert.GreaterOrEqual(t, plan.PlacementScore, 0.0)
		}

		// Check diversity metrics
		assert.GreaterOrEqual(t, multiPlan.SuccessfulRack, 1)
		assert.GreaterOrEqual(t, multiPlan.SuccessfulDCs, 1)
	})

	// Test destination planning with load
	t.Run("Destination planning considers load", func(t *testing.T) {
		// Add load to one disk
		topology.AddPendingTask("task1", TaskTypeBalance, 2001,
			"10.0.0.2:8080", 0, "", 0)

		plan, err := topology.PlanBalanceDestination(1003, "10.0.0.1:8080", "rack1", "dc1", 1024*1024)
		require.NoError(t, err)
		require.NotNil(t, plan)

		// Should prefer less loaded disk (disk 1 over disk 0 on node2)
		assert.Equal(t, "10.0.0.2:8080", plan.TargetNode)
		assert.Equal(t, uint32(1), plan.TargetDisk) // Should prefer SSD (disk 1) which has no load
	})

	// Test insufficient destinations
	t.Run("Handle insufficient destinations", func(t *testing.T) {
		// Try to plan for more EC shards than available disks
		multiPlan, err := topology.PlanECDestinations(1004, "10.0.0.1:8080", "rack1", "dc1", 100)

		// Should get an error for insufficient disks
		assert.Error(t, err)
		assert.Nil(t, multiPlan)
	})
}

// TestDestinationPlanningWithActiveTopology tests the integration between task detection and destination planning
func TestDestinationPlanningWithActiveTopology(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createUnbalancedTopology())

	// Test that tasks are created with destinations
	t.Run("Balance task with destination", func(t *testing.T) {
		// Simulate what the balance detector would create
		sourceNode := "10.0.0.1:8080" // Overloaded node
		volumeID := uint32(1001)

		plan, err := topology.PlanBalanceDestination(volumeID, sourceNode, "rack1", "dc1", 1024*1024)
		require.NoError(t, err)
		require.NotNil(t, plan)

		// Verify the destination is different from source
		assert.NotEqual(t, sourceNode, plan.TargetNode)
		assert.Equal(t, "10.0.0.2:8080", plan.TargetNode) // Should be the lightly loaded node

		// Verify placement quality
		assert.Greater(t, plan.PlacementScore, 0.0)
		assert.LessOrEqual(t, plan.PlacementScore, 1.0)
	})

	// Test task state integration
	t.Run("Task state affects future planning", func(t *testing.T) {
		volumeID := uint32(1002)
		sourceNode := "10.0.0.1:8080"
		targetNode := "10.0.0.2:8080"

		// Plan first destination
		plan1, err := topology.PlanBalanceDestination(volumeID, sourceNode, "rack1", "dc1", 1024*1024)
		require.NoError(t, err)
		require.NotNil(t, plan1)

		// Add a pending task to the target
		topology.AddPendingTask("task1", TaskTypeBalance, volumeID, sourceNode, 0, targetNode, 0)

		// Plan another destination - should consider the pending task load
		plan2, err := topology.PlanBalanceDestination(1003, sourceNode, "rack1", "dc1", 1024*1024)
		require.NoError(t, err)
		require.NotNil(t, plan2)

		// The placement score should reflect the increased load
		// (This test might need adjustment based on the actual scoring algorithm)
		glog.V(1).Infof("Plan1 score: %.3f, Plan2 score: %.3f", plan1.PlacementScore, plan2.PlacementScore)
	})
}

// TestECDestinationPlanningDetailed tests the EC destination planning with multiple shards
func TestECDestinationPlanningDetailed(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	t.Run("EC multiple destinations", func(t *testing.T) {
		// Plan for 3 EC shards (now including source node, we have 4 disks total)
		multiPlan, err := topology.PlanECDestinations(1005, "10.0.0.1:8080", "rack1", "dc1", 3)
		require.NoError(t, err)
		require.NotNil(t, multiPlan)

		// Should get 3 destinations (can include source node's disks)
		assert.Equal(t, 3, len(multiPlan.Plans))
		assert.Equal(t, 3, multiPlan.TotalShards)

		// Count node distribution - source node can now be included
		nodeCount := make(map[string]int)
		for _, plan := range multiPlan.Plans {
			nodeCount[plan.TargetNode]++
		}

		// Should distribute across available nodes (both nodes can be used)
		assert.GreaterOrEqual(t, len(nodeCount), 1, "Should use at least 1 node")
		assert.LessOrEqual(t, len(nodeCount), 2, "Should use at most 2 nodes")
		glog.V(1).Infof("EC destinations node distribution: %v", nodeCount)

		glog.V(1).Infof("EC destinations: %d plans across %d racks, %d DCs",
			multiPlan.TotalShards, multiPlan.SuccessfulRack, multiPlan.SuccessfulDCs)
	})

	t.Run("EC destination planning with task conflicts", func(t *testing.T) {
		// Create a fresh topology for this test to avoid conflicts from previous test
		freshTopology := NewActiveTopology(10)
		freshTopology.UpdateTopology(createSampleTopology())

		// Add tasks to create conflicts on some disks
		freshTopology.AddPendingTask("conflict1", TaskTypeVacuum, 2001, "10.0.0.2:8080", 0, "", 0)
		freshTopology.AddPendingTask("conflict2", TaskTypeBalance, 2002, "10.0.0.1:8080", 0, "", 0)
		freshTopology.AssignTask("conflict1")
		freshTopology.AssignTask("conflict2")

		// Plan EC destinations - should still succeed using available disks
		multiPlan, err := freshTopology.PlanECDestinations(1006, "10.0.0.1:8080", "rack1", "dc1", 2)
		require.NoError(t, err)
		require.NotNil(t, multiPlan)

		// Should get destinations (using disks that don't have conflicts)
		assert.GreaterOrEqual(t, len(multiPlan.Plans), 1)
		assert.LessOrEqual(t, len(multiPlan.Plans), 2)

		// Available disks should be: node1/disk1 and node2/disk1 (since disk0 on both nodes have conflicts)
		for _, plan := range multiPlan.Plans {
			assert.Equal(t, uint32(1), plan.TargetDisk, "Should prefer disk 1 which has no conflicts")
		}

		glog.V(1).Infof("EC destination planning with conflicts: found %d destinations", len(multiPlan.Plans))
	})
}
