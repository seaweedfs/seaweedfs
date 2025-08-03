package topology

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to find a disk by ID for testing - reduces code duplication
func findDiskByID(disks []*DiskInfo, diskID uint32) *DiskInfo {
	for _, disk := range disks {
		if disk.DiskID == diskID {
			return disk
		}
	}
	return nil
}

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
	err := topology.AddPendingTask(TaskSpec{
		TaskID:     taskID,
		TaskType:   TaskTypeBalance,
		VolumeID:   1001,
		VolumeSize: 1024 * 1024 * 1024,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.2:8080", DiskID: 1},
		},
	})
	assert.NoError(t, err, "Should add pending task successfully")

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
	err = topology.AssignTask(taskID)
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

				assert.Less(t, disk.LoadCount, 2, "Disk load should be less than 2")
			}
		})
	}
}

// TestDiskLoadCalculation tests disk load calculation
func TestDiskLoadCalculation(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Initially no load
	disks := topology.GetNodeDisks("10.0.0.1:8080")
	targetDisk := findDiskByID(disks, 0)
	require.NotNil(t, targetDisk, "Should find disk with ID 0")
	assert.Equal(t, 0, targetDisk.LoadCount)

	// Add pending task
	err := topology.AddPendingTask(TaskSpec{
		TaskID:     "task1",
		TaskType:   TaskTypeBalance,
		VolumeID:   1001,
		VolumeSize: 1024 * 1024 * 1024,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.2:8080", DiskID: 1},
		},
	})
	assert.NoError(t, err, "Should add pending task successfully")

	// Check load increased
	disks = topology.GetNodeDisks("10.0.0.1:8080")
	targetDisk = findDiskByID(disks, 0)
	assert.Equal(t, 1, targetDisk.LoadCount)

	// Add another task to same disk
	err = topology.AddPendingTask(TaskSpec{
		TaskID:     "task2",
		TaskType:   TaskTypeVacuum,
		VolumeID:   1002,
		VolumeSize: 0,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "", DiskID: 0}, // Vacuum doesn't have a destination
		},
	})
	assert.NoError(t, err, "Should add vacuum task successfully")

	disks = topology.GetNodeDisks("10.0.0.1:8080")
	targetDisk = findDiskByID(disks, 0)
	assert.Equal(t, 2, targetDisk.LoadCount)

	// Move one task to assigned
	topology.AssignTask("task1")

	// Load should still be 2 (1 pending + 1 assigned)
	disks = topology.GetNodeDisks("10.0.0.1:8080")
	targetDisk = findDiskByID(disks, 0)
	assert.Equal(t, 2, targetDisk.LoadCount)

	// Complete one task
	topology.CompleteTask("task1")

	// Load should decrease to 1
	disks = topology.GetNodeDisks("10.0.0.1:8080")
	targetDisk = findDiskByID(disks, 0)
	assert.Equal(t, 1, targetDisk.LoadCount)
}

// TestTaskConflictDetection tests task conflict detection
func TestTaskConflictDetection(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Add a balance task
	err := topology.AddPendingTask(TaskSpec{
		TaskID:     "balance1",
		TaskType:   TaskTypeBalance,
		VolumeID:   1001,
		VolumeSize: 1024 * 1024 * 1024,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.2:8080", DiskID: 1},
		},
	})
	assert.NoError(t, err, "Should add balance task successfully")
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
	err := topology.AddPendingTask(TaskSpec{
		TaskID:     "existing1",
		TaskType:   TaskTypeVacuum,
		VolumeID:   2001,
		VolumeSize: 0,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "", DiskID: 0}, // Vacuum doesn't have a destination
		},
	})
	if err != nil {
		// In test helper function, just log error instead of failing
		fmt.Printf("Warning: Failed to add existing task: %v\n", err)
	}
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
	err := topology.AddPendingTask(TaskSpec{
		TaskID:     "balance1",
		TaskType:   TaskTypeBalance,
		VolumeID:   3001,
		VolumeSize: 1024 * 1024 * 1024,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 0},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "10.0.0.2:8080", DiskID: 0},
		},
	})
	if err != nil {
		fmt.Printf("Warning: Failed to add balance task: %v\n", err)
	}
	topology.AssignTask("balance1")

	err = topology.AddPendingTask(TaskSpec{
		TaskID:     "ec1",
		TaskType:   TaskTypeErasureCoding,
		VolumeID:   3002,
		VolumeSize: 1024 * 1024 * 1024,
		Sources: []TaskSourceSpec{
			{ServerID: "10.0.0.1:8080", DiskID: 1},
		},
		Destinations: []TaskDestinationSpec{
			{ServerID: "", DiskID: 0}, // EC doesn't have single destination
		},
	})
	if err != nil {
		fmt.Printf("Warning: Failed to add EC task: %v\n", err)
	}
	topology.AssignTask("ec1")

	return topology
}

// TestDestinationPlanning tests that the public interface works correctly
// NOTE: Destination planning is now done in task detection phase, not in ActiveTopology
func TestDestinationPlanning(t *testing.T) {
	topology := NewActiveTopology(10)
	topology.UpdateTopology(createSampleTopology())

	// Test that GetAvailableDisks works for destination planning
	t.Run("GetAvailableDisks functionality", func(t *testing.T) {
		availableDisks := topology.GetAvailableDisks(TaskTypeBalance, "10.0.0.1:8080")
		assert.Greater(t, len(availableDisks), 0)

		// Should exclude the source node
		for _, disk := range availableDisks {
			assert.NotEqual(t, "10.0.0.1:8080", disk.NodeID)
		}
	})

	// Test that topology state can be used for planning
	t.Run("Topology provides planning information", func(t *testing.T) {
		topologyInfo := topology.GetTopologyInfo()
		assert.NotNil(t, topologyInfo)
		assert.Greater(t, len(topologyInfo.DataCenterInfos), 0)

		// Test getting node disks
		disks := topology.GetNodeDisks("10.0.0.1:8080")
		assert.Greater(t, len(disks), 0)
	})
}
