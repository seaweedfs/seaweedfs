package task

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestVolumeStateManager_RegisterTaskImpact(t *testing.T) {
	vsm := NewVolumeStateManager(nil)

	// Create test volume state
	volumeID := uint32(1)
	volumeState := &VolumeState{
		VolumeID: volumeID,
		CurrentState: &VolumeInfo{
			ID:   volumeID,
			Size: 1024 * 1024 * 1024, // 1GB
		},
		InProgressTasks: []*TaskImpact{},
		PlannedChanges:  []*PlannedOperation{},
		Inconsistencies: []StateInconsistency{},
	}
	vsm.volumes[volumeID] = volumeState

	// Create task impact
	impact := &TaskImpact{
		TaskID:       "test_task_1",
		TaskType:     types.TaskTypeErasureCoding,
		VolumeID:     volumeID,
		WorkerID:     "worker_1",
		StartedAt:    time.Now(),
		EstimatedEnd: time.Now().Add(15 * time.Minute),
		VolumeChanges: &VolumeChanges{
			WillBecomeReadOnly: true,
		},
		ShardChanges:  make(map[int]*ShardChange),
		CapacityDelta: map[string]int64{"server1": 400 * 1024 * 1024}, // 400MB for shards
	}

	// Register impact
	vsm.RegisterTaskImpact(impact.TaskID, impact)

	// Verify impact was registered
	if len(vsm.inProgressTasks) != 1 {
		t.Errorf("Expected 1 in-progress task, got %d", len(vsm.inProgressTasks))
	}

	if len(volumeState.InProgressTasks) != 1 {
		t.Errorf("Expected 1 task in volume state, got %d", len(volumeState.InProgressTasks))
	}

	// Verify task can be retrieved
	retrievedImpact := vsm.inProgressTasks[impact.TaskID]
	if retrievedImpact == nil {
		t.Error("Task impact not found after registration")
	}

	if retrievedImpact.TaskType != types.TaskTypeErasureCoding {
		t.Errorf("Expected task type %v, got %v", types.TaskTypeErasureCoding, retrievedImpact.TaskType)
	}
}

func TestVolumeStateManager_UnregisterTaskImpact(t *testing.T) {
	vsm := NewVolumeStateManager(nil)

	// Setup test data
	volumeID := uint32(1)
	taskID := "test_task_1"

	volumeState := &VolumeState{
		VolumeID:        volumeID,
		CurrentState:    &VolumeInfo{ID: volumeID, Size: 1024 * 1024 * 1024},
		InProgressTasks: []*TaskImpact{},
	}
	vsm.volumes[volumeID] = volumeState

	impact := &TaskImpact{
		TaskID:        taskID,
		TaskType:      types.TaskTypeVacuum,
		VolumeID:      volumeID,
		CapacityDelta: map[string]int64{"server1": -100 * 1024 * 1024}, // 100MB savings
	}

	// Register then unregister
	vsm.RegisterTaskImpact(taskID, impact)
	vsm.UnregisterTaskImpact(taskID)

	// Verify impact was removed
	if len(vsm.inProgressTasks) != 0 {
		t.Errorf("Expected 0 in-progress tasks, got %d", len(vsm.inProgressTasks))
	}

	if len(volumeState.InProgressTasks) != 0 {
		t.Errorf("Expected 0 tasks in volume state, got %d", len(volumeState.InProgressTasks))
	}
}

func TestVolumeStateManager_CanAssignVolumeToServer(t *testing.T) {
	vsm := NewVolumeStateManager(nil)

	// Setup server capacity
	serverID := "test_server"
	capacity := &CapacityInfo{
		Server:           serverID,
		TotalCapacity:    10 * 1024 * 1024 * 1024, // 10GB
		UsedCapacity:     3 * 1024 * 1024 * 1024,  // 3GB used
		ReservedCapacity: 1 * 1024 * 1024 * 1024,  // 1GB reserved
		PredictedUsage:   4 * 1024 * 1024 * 1024,  // 4GB predicted total
	}
	vsm.capacityCache[serverID] = capacity

	tests := []struct {
		name       string
		volumeSize int64
		expected   bool
		desc       string
	}{
		{
			name:       "Small volume fits",
			volumeSize: 1 * 1024 * 1024 * 1024, // 1GB
			expected:   true,
			desc:       "1GB volume should fit in 6GB available space",
		},
		{
			name:       "Large volume fits exactly",
			volumeSize: 6 * 1024 * 1024 * 1024, // 6GB
			expected:   true,
			desc:       "6GB volume should fit exactly in available space",
		},
		{
			name:       "Volume too large",
			volumeSize: 7 * 1024 * 1024 * 1024, // 7GB
			expected:   false,
			desc:       "7GB volume should not fit in 6GB available space",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := vsm.CanAssignVolumeToServer(tt.volumeSize, serverID)
			if result != tt.expected {
				t.Errorf("CanAssignVolumeToServer() = %v, want %v. %s", result, tt.expected, tt.desc)
			}
		})
	}
}

func TestVolumeStateManager_GetPendingChange(t *testing.T) {
	vsm := NewVolumeStateManager(nil)

	volumeID := uint32(1)

	// Create volume with planned operation
	volumeState := &VolumeState{
		VolumeID: volumeID,
		CurrentState: &VolumeInfo{
			ID:   volumeID,
			Size: 2 * 1024 * 1024 * 1024, // 2GB
		},
		PlannedChanges: []*PlannedOperation{
			{
				OperationID: "op_1",
				Type:        OperationVacuum,
				VolumeID:    volumeID,
				Impact: &TaskImpact{
					TaskID: "task_1",
					VolumeChanges: &VolumeChanges{
						SizeChange: -500 * 1024 * 1024, // 500MB reduction
					},
				},
			},
		},
	}
	vsm.volumes[volumeID] = volumeState

	// Test getting pending change
	change := vsm.GetPendingChange(volumeID)

	if change == nil {
		t.Fatal("Expected pending change, got nil")
	}

	if change.VolumeID != volumeID {
		t.Errorf("Expected volume ID %d, got %d", volumeID, change.VolumeID)
	}

	expectedNewCapacity := int64(2*1024*1024*1024 - 500*1024*1024) // 2GB - 500MB
	if change.NewCapacity != expectedNewCapacity {
		t.Errorf("Expected new capacity %d, got %d", expectedNewCapacity, change.NewCapacity)
	}

	// Test no pending change
	change2 := vsm.GetPendingChange(999) // Non-existent volume
	if change2 != nil {
		t.Error("Expected nil for non-existent volume, got change")
	}
}

func TestVolumeStateManager_StateConsistency(t *testing.T) {
	// Test that demonstrates the core value: accurate state tracking
	vsm := NewVolumeStateManager(nil)

	volumeID := uint32(1)
	serverID := "test_server"

	// Setup initial state
	vsm.volumes[volumeID] = &VolumeState{
		VolumeID: volumeID,
		CurrentState: &VolumeInfo{
			ID:     volumeID,
			Size:   28 * 1024 * 1024 * 1024, // 28GB - ready for EC
			Server: serverID,
		},
		InProgressTasks: []*TaskImpact{},
		PlannedChanges:  []*PlannedOperation{},
	}

	vsm.capacityCache[serverID] = &CapacityInfo{
		Server:         serverID,
		TotalCapacity:  100 * 1024 * 1024 * 1024, // 100GB
		UsedCapacity:   50 * 1024 * 1024 * 1024,  // 50GB used
		PredictedUsage: 50 * 1024 * 1024 * 1024,  // Initially same as used
	}

	// Step 1: Register EC task impact
	ecImpact := &TaskImpact{
		TaskID:   "ec_task_1",
		TaskType: types.TaskTypeErasureCoding,
		VolumeID: volumeID,
		VolumeChanges: &VolumeChanges{
			WillBecomeReadOnly: true,
		},
		CapacityDelta: map[string]int64{
			serverID: 12 * 1024 * 1024 * 1024, // 12GB for EC shards (40% overhead)
		},
	}

	vsm.RegisterTaskImpact(ecImpact.TaskID, ecImpact)

	// Verify capacity is reserved
	capacity := vsm.GetAccurateCapacity(serverID)
	expectedPredicted := int64(50 * 1024 * 1024 * 1024) // 50GB initially
	if capacity.PredictedUsage != expectedPredicted {
		t.Errorf("Expected predicted usage %d, got %d", expectedPredicted, capacity.PredictedUsage)
	}

	// Verify reservation is tracked separately
	expectedReserved := int64(12 * 1024 * 1024 * 1024) // 12GB for EC shards
	if capacity.ReservedCapacity != expectedReserved {
		t.Errorf("Expected reserved capacity %d, got %d", expectedReserved, capacity.ReservedCapacity)
	}

	// Calculate available capacity correctly
	availableCapacity := capacity.TotalCapacity - capacity.UsedCapacity - capacity.ReservedCapacity
	// 100GB - 50GB - 12GB = 38GB available
	expectedAvailable := int64(38 * 1024 * 1024 * 1024)
	if availableCapacity != expectedAvailable {
		t.Errorf("Expected available capacity %d, got %d", expectedAvailable, availableCapacity)
	}

	// Step 2: Check assignment logic - should reject new large volume
	canAssign := vsm.CanAssignVolumeToServer(40*1024*1024*1024, serverID) // 40GB volume
	if canAssign {
		t.Error("Should not be able to assign 40GB volume when only 38GB available after reservations")
	}

	// Step 3: Complete EC task
	vsm.UnregisterTaskImpact(ecImpact.TaskID)

	// Verify capacity is updated correctly
	capacityAfter := vsm.GetAccurateCapacity(serverID)
	if capacityAfter.ReservedCapacity != 0 {
		t.Errorf("Expected 0 reserved capacity after task completion, got %d", capacityAfter.ReservedCapacity)
	}

	t.Logf("✅ State consistency test passed - accurate capacity tracking throughout task lifecycle")
}

func TestVolumeStateManager_ConcurrentTasks(t *testing.T) {
	// Test multiple concurrent tasks affecting capacity
	vsm := NewVolumeStateManager(nil)

	serverID := "test_server"
	vsm.capacityCache[serverID] = &CapacityInfo{
		Server:         serverID,
		TotalCapacity:  50 * 1024 * 1024 * 1024, // 50GB
		UsedCapacity:   10 * 1024 * 1024 * 1024, // 10GB used
		PredictedUsage: 10 * 1024 * 1024 * 1024, // Initially 10GB
	}

	// Register multiple tasks
	tasks := []struct {
		taskID        string
		volumeID      uint32
		capacityDelta int64
	}{
		{"ec_task_1", 1, 15 * 1024 * 1024 * 1024},     // 15GB for EC
		{"vacuum_task_1", 2, -5 * 1024 * 1024 * 1024}, // 5GB savings
		{"ec_task_2", 3, 20 * 1024 * 1024 * 1024},     // 20GB for EC
	}

	for _, task := range tasks {
		// Setup volume state
		vsm.volumes[task.volumeID] = &VolumeState{
			VolumeID:     task.volumeID,
			CurrentState: &VolumeInfo{ID: task.volumeID, Size: 25 * 1024 * 1024 * 1024},
		}

		impact := &TaskImpact{
			TaskID:        task.taskID,
			VolumeID:      task.volumeID,
			TaskType:      types.TaskTypeErasureCoding,
			CapacityDelta: map[string]int64{serverID: task.capacityDelta},
		}

		vsm.RegisterTaskImpact(task.taskID, impact)
	}

	// Check cumulative capacity impact
	capacity := vsm.GetAccurateCapacity(serverID)
	expectedPredicted := int64(10*1024*1024*1024 + 15*1024*1024*1024 - 5*1024*1024*1024 + 20*1024*1024*1024) // 40GB

	if capacity.PredictedUsage != expectedPredicted {
		t.Errorf("Expected predicted usage %d GB, got %d GB",
			expectedPredicted/(1024*1024*1024), capacity.PredictedUsage/(1024*1024*1024))
	}

	// Verify we can't assign more than available
	remainingCapacity := capacity.TotalCapacity - capacity.PredictedUsage
	canAssign := vsm.CanAssignVolumeToServer(remainingCapacity+1, serverID)
	if canAssign {
		t.Error("Should not be able to assign volume larger than remaining capacity")
	}

	t.Logf("✅ Concurrent tasks test passed - accurate cumulative capacity tracking")
}

func TestVolumeStateManager_ECShardTracking(t *testing.T) {
	vsm := NewVolumeStateManager(nil)

	volumeID := uint32(1)

	// Create EC shard state
	shardState := &ECShardState{
		VolumeID: volumeID,
		CurrentShards: map[int]*ShardInfo{
			0: {ShardID: 0, Server: "server1", Status: ShardStatusExists},
			1: {ShardID: 1, Server: "server1", Status: ShardStatusExists},
			2: {ShardID: 2, Server: "server2", Status: ShardStatusExists},
		},
		InProgressTasks: []*TaskImpact{},
		PlannedShards:   make(map[int]*PlannedShard),
		PredictedShards: make(map[int]*ShardInfo),
	}
	vsm.ecShards[volumeID] = shardState

	// Register task that will create more shards
	impact := &TaskImpact{
		TaskID:   "ec_expand_task",
		VolumeID: volumeID,
		TaskType: types.TaskTypeErasureCoding,
		ShardChanges: map[int]*ShardChange{
			3: {ShardID: 3, WillBeCreated: true, TargetServer: "server3"},
			4: {ShardID: 4, WillBeCreated: true, TargetServer: "server3"},
		},
	}

	vsm.RegisterTaskImpact(impact.TaskID, impact)

	// Verify shard state tracking
	retrievedState := vsm.GetECShardState(volumeID)
	if retrievedState == nil {
		t.Fatal("Expected EC shard state, got nil")
	}

	if len(retrievedState.InProgressTasks) != 1 {
		t.Errorf("Expected 1 in-progress task for shards, got %d", len(retrievedState.InProgressTasks))
	}

	// Verify current shards are still tracked
	if len(retrievedState.CurrentShards) != 3 {
		t.Errorf("Expected 3 current shards, got %d", len(retrievedState.CurrentShards))
	}

	t.Logf("✅ EC shard tracking test passed")
}

// Benchmark tests for performance
func BenchmarkVolumeStateManager_RegisterTaskImpact(b *testing.B) {
	vsm := NewVolumeStateManager(nil)

	// Setup test data
	for i := 0; i < 1000; i++ {
		volumeID := uint32(i + 1)
		vsm.volumes[volumeID] = &VolumeState{
			VolumeID:        volumeID,
			CurrentState:    &VolumeInfo{ID: volumeID},
			InProgressTasks: []*TaskImpact{},
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		impact := &TaskImpact{
			TaskID:        generateTaskID(),
			VolumeID:      uint32((i % 1000) + 1),
			TaskType:      types.TaskTypeVacuum,
			CapacityDelta: map[string]int64{"server1": 1024 * 1024},
		}

		vsm.RegisterTaskImpact(impact.TaskID, impact)
		vsm.UnregisterTaskImpact(impact.TaskID)
	}
}

func BenchmarkVolumeStateManager_CanAssignVolumeToServer(b *testing.B) {
	vsm := NewVolumeStateManager(nil)

	// Setup capacity data
	for i := 0; i < 100; i++ {
		serverID := fmt.Sprintf("server_%d", i)
		vsm.capacityCache[serverID] = &CapacityInfo{
			Server:         serverID,
			TotalCapacity:  100 * 1024 * 1024 * 1024,
			UsedCapacity:   50 * 1024 * 1024 * 1024,
			PredictedUsage: 50 * 1024 * 1024 * 1024,
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		serverID := fmt.Sprintf("server_%d", i%100)
		vsm.CanAssignVolumeToServer(1024*1024*1024, serverID)
	}
}
