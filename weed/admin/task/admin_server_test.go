package task

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestAdminServer_TaskAssignmentWithStateManagement(t *testing.T) {
	// Test the core functionality: accurate task assignment based on comprehensive state
	adminServer := NewAdminServer(DefaultAdminConfig(), nil)

	// Initialize components
	adminServer.workerRegistry = NewWorkerRegistry()
	adminServer.taskQueue = NewPriorityTaskQueue()
	adminServer.volumeStateManager = NewVolumeStateManager(nil)
	adminServer.taskScheduler = NewTaskScheduler(adminServer.workerRegistry, adminServer.taskQueue)
	adminServer.running = true // Mark as running for test

	// Setup test worker
	worker := &types.Worker{
		ID:            "test_worker_1",
		Address:       "server1:8080",
		Capabilities:  []types.TaskType{types.TaskTypeErasureCoding, types.TaskTypeVacuum},
		MaxConcurrent: 2,
		Status:        "active",
		CurrentLoad:   0,
	}
	adminServer.workerRegistry.RegisterWorker(worker)

	// Setup volume state
	volumeID := uint32(1)
	adminServer.volumeStateManager.volumes[volumeID] = &VolumeState{
		VolumeID: volumeID,
		CurrentState: &VolumeInfo{
			ID:     volumeID,
			Size:   28 * 1024 * 1024 * 1024, // 28GB - good for EC
			Server: "server1",
		},
		InProgressTasks: []*TaskImpact{},
		PlannedChanges:  []*PlannedOperation{},
	}

	// Setup server capacity
	adminServer.volumeStateManager.capacityCache["server1"] = &CapacityInfo{
		Server:         "server1",
		TotalCapacity:  100 * 1024 * 1024 * 1024, // 100GB
		UsedCapacity:   50 * 1024 * 1024 * 1024,  // 50GB used
		PredictedUsage: 50 * 1024 * 1024 * 1024,  // Initially same as used
	}

	// Create EC task
	task := &types.Task{
		ID:       "ec_task_1",
		Type:     types.TaskTypeErasureCoding,
		VolumeID: volumeID,
		Server:   "server1",
		Priority: types.TaskPriorityNormal,
	}

	// Test task assignment
	adminServer.taskQueue.Push(task)

	assignedTask, err := adminServer.RequestTask("test_worker_1", []types.TaskType{types.TaskTypeErasureCoding})
	if err != nil {
		t.Errorf("Task assignment failed: %v", err)
	}

	if assignedTask == nil {
		t.Fatal("Expected task to be assigned, got nil")
	}

	if assignedTask.ID != "ec_task_1" {
		t.Errorf("Expected task ec_task_1, got %s", assignedTask.ID)
	}

	// Verify state manager was updated
	if len(adminServer.volumeStateManager.inProgressTasks) != 1 {
		t.Errorf("Expected 1 in-progress task in state manager, got %d", len(adminServer.volumeStateManager.inProgressTasks))
	}

	// Verify capacity reservation
	capacity := adminServer.volumeStateManager.GetAccurateCapacity("server1")
	if capacity.ReservedCapacity <= 0 {
		t.Error("Expected capacity to be reserved for EC task")
	}

	t.Log("✅ Task assignment with state management test passed")
}

func TestAdminServer_CanAssignTask(t *testing.T) {
	adminServer := NewAdminServer(DefaultAdminConfig(), nil)
	adminServer.volumeStateManager = NewVolumeStateManager(nil)
	adminServer.inProgressTasks = make(map[string]*InProgressTask)

	// Setup volume state
	volumeID := uint32(1)
	adminServer.volumeStateManager.volumes[volumeID] = &VolumeState{
		VolumeID: volumeID,
		CurrentState: &VolumeInfo{
			ID:   volumeID,
			Size: 25 * 1024 * 1024 * 1024, // 25GB
		},
	}

	// Setup server capacity - limited space
	serverID := "server1"
	adminServer.volumeStateManager.capacityCache[serverID] = &CapacityInfo{
		Server:         serverID,
		TotalCapacity:  30 * 1024 * 1024 * 1024, // 30GB total
		UsedCapacity:   20 * 1024 * 1024 * 1024, // 20GB used
		PredictedUsage: 20 * 1024 * 1024 * 1024, // 10GB available
	}

	worker := &types.Worker{
		ID:      "worker1",
		Address: serverID,
	}

	tests := []struct {
		name     string
		taskType types.TaskType
		expected bool
		desc     string
	}{
		{
			name:     "EC task fits",
			taskType: types.TaskTypeErasureCoding,
			expected: false, // 25GB * 1.4 = 35GB needed, but only 10GB available
			desc:     "EC task should not fit due to insufficient capacity",
		},
		{
			name:     "Vacuum task fits",
			taskType: types.TaskTypeVacuum,
			expected: true,
			desc:     "Vacuum task should fit (no capacity increase)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &types.Task{
				ID:       "test_task",
				Type:     tt.taskType,
				VolumeID: volumeID,
				Server:   serverID,
			}

			result := adminServer.canAssignTask(task, worker)
			if result != tt.expected {
				t.Errorf("canAssignTask() = %v, want %v. %s", result, tt.expected, tt.desc)
			}
		})
	}
}

func TestAdminServer_CreateTaskImpact(t *testing.T) {
	adminServer := NewAdminServer(DefaultAdminConfig(), nil)
	adminServer.volumeStateManager = NewVolumeStateManager(nil)

	// Setup volume state for EC task
	volumeID := uint32(1)
	adminServer.volumeStateManager.volumes[volumeID] = &VolumeState{
		VolumeID: volumeID,
		CurrentState: &VolumeInfo{
			ID:   volumeID,
			Size: 25 * 1024 * 1024 * 1024, // 25GB
		},
	}

	task := &types.Task{
		ID:       "ec_task_1",
		Type:     types.TaskTypeErasureCoding,
		VolumeID: volumeID,
		Server:   "server1",
	}

	impact := adminServer.createTaskImpact(task, "worker1")

	// Verify impact structure
	if impact.TaskID != "ec_task_1" {
		t.Errorf("Expected task ID ec_task_1, got %s", impact.TaskID)
	}

	if impact.TaskType != types.TaskTypeErasureCoding {
		t.Errorf("Expected task type %v, got %v", types.TaskTypeErasureCoding, impact.TaskType)
	}

	// Verify volume changes for EC task
	if !impact.VolumeChanges.WillBecomeReadOnly {
		t.Error("Expected volume to become read-only after EC")
	}

	// Verify capacity delta (EC should require ~40% more space)
	expectedCapacity := int64(float64(25*1024*1024*1024) * 1.4) // ~35GB
	actualCapacity := impact.CapacityDelta["server1"]
	if actualCapacity != expectedCapacity {
		t.Errorf("Expected capacity delta %d, got %d", expectedCapacity, actualCapacity)
	}

	// Verify shard changes (should plan 14 shards)
	if len(impact.ShardChanges) != 14 {
		t.Errorf("Expected 14 shard changes, got %d", len(impact.ShardChanges))
	}

	for i := 0; i < 14; i++ {
		shardChange := impact.ShardChanges[i]
		if shardChange == nil {
			t.Errorf("Missing shard change for shard %d", i)
			continue
		}

		if !shardChange.WillBeCreated {
			t.Errorf("Shard %d should be marked for creation", i)
		}
	}

	t.Log("✅ Task impact creation test passed")
}

func TestAdminServer_TaskCompletionStateCleanup(t *testing.T) {
	adminServer := NewAdminServer(DefaultAdminConfig(), nil)
	adminServer.workerRegistry = NewWorkerRegistry()
	adminServer.volumeStateManager = NewVolumeStateManager(nil)
	adminServer.inProgressTasks = make(map[string]*InProgressTask)

	// Setup worker
	worker := &types.Worker{
		ID:          "worker1",
		CurrentLoad: 1, // Has 1 task assigned
	}
	adminServer.workerRegistry.RegisterWorker(worker)

	// Setup in-progress task
	task := &types.Task{
		ID:       "test_task_1",
		Type:     types.TaskTypeVacuum,
		VolumeID: 1,
	}

	inProgressTask := &InProgressTask{
		Task:           task,
		WorkerID:       "worker1",
		VolumeReserved: true,
	}
	adminServer.inProgressTasks["test_task_1"] = inProgressTask

	// Register impact in state manager
	impact := &TaskImpact{
		TaskID:        "test_task_1",
		VolumeID:      1,
		CapacityDelta: map[string]int64{"server1": -100 * 1024 * 1024}, // 100MB savings
	}
	adminServer.volumeStateManager.RegisterTaskImpact("test_task_1", impact)

	// Complete the task
	err := adminServer.CompleteTask("test_task_1", true, "")
	if err != nil {
		t.Errorf("Task completion failed: %v", err)
	}

	// Verify cleanup
	if len(adminServer.inProgressTasks) != 0 {
		t.Errorf("Expected 0 in-progress tasks after completion, got %d", len(adminServer.inProgressTasks))
	}

	// Verify worker load updated
	updatedWorker, _ := adminServer.workerRegistry.GetWorker("worker1")
	if updatedWorker.CurrentLoad != 0 {
		t.Errorf("Expected worker load 0 after task completion, got %d", updatedWorker.CurrentLoad)
	}

	// Verify state manager cleaned up
	if len(adminServer.volumeStateManager.inProgressTasks) != 0 {
		t.Errorf("Expected 0 tasks in state manager after completion, got %d", len(adminServer.volumeStateManager.inProgressTasks))
	}

	t.Log("✅ Task completion state cleanup test passed")
}

func TestAdminServer_PreventDuplicateTaskAssignment(t *testing.T) {
	adminServer := NewAdminServer(DefaultAdminConfig(), nil)
	adminServer.workerRegistry = NewWorkerRegistry()
	adminServer.taskQueue = NewPriorityTaskQueue()
	adminServer.volumeStateManager = NewVolumeStateManager(nil)
	adminServer.inProgressTasks = make(map[string]*InProgressTask)

	// Setup worker
	worker := &types.Worker{
		ID:            "worker1",
		Capabilities:  []types.TaskType{types.TaskTypeVacuum},
		MaxConcurrent: 2,
		Status:        "active",
		CurrentLoad:   0,
	}
	adminServer.workerRegistry.RegisterWorker(worker)

	// Setup volume state
	volumeID := uint32(1)
	adminServer.volumeStateManager.volumes[volumeID] = &VolumeState{
		VolumeID:     volumeID,
		CurrentState: &VolumeInfo{ID: volumeID, Size: 1024 * 1024 * 1024},
	}

	// Create first task and assign it
	task1 := &types.Task{
		ID:       "vacuum_task_1",
		Type:     types.TaskTypeVacuum,
		VolumeID: volumeID,
		Priority: types.TaskPriorityNormal,
	}

	adminServer.taskQueue.Push(task1)
	assignedTask1, err := adminServer.RequestTask("worker1", []types.TaskType{types.TaskTypeVacuum})
	if err != nil || assignedTask1 == nil {
		t.Fatal("First task assignment failed")
	}

	// Try to assign another vacuum task for the same volume
	task2 := &types.Task{
		ID:       "vacuum_task_2",
		Type:     types.TaskTypeVacuum,
		VolumeID: volumeID, // Same volume!
		Priority: types.TaskPriorityNormal,
	}

	adminServer.taskQueue.Push(task2)
	assignedTask2, err := adminServer.RequestTask("worker1", []types.TaskType{types.TaskTypeVacuum})

	// Should not assign duplicate task
	if assignedTask2 != nil {
		t.Error("Should not assign duplicate vacuum task for same volume")
	}

	t.Log("✅ Duplicate task prevention test passed")
}

func TestAdminServer_SystemStats(t *testing.T) {
	adminServer := NewAdminServer(DefaultAdminConfig(), nil)
	adminServer.workerRegistry = NewWorkerRegistry()
	adminServer.taskQueue = NewPriorityTaskQueue()
	adminServer.volumeStateManager = NewVolumeStateManager(nil)
	adminServer.inProgressTasks = make(map[string]*InProgressTask)
	adminServer.running = true

	// Add some test data
	worker := &types.Worker{ID: "worker1", Status: "active"}
	adminServer.workerRegistry.RegisterWorker(worker)

	task := &types.Task{ID: "task1", Type: types.TaskTypeErasureCoding}
	adminServer.taskQueue.Push(task)

	inProgressTask := &InProgressTask{
		Task: &types.Task{ID: "task2", Type: types.TaskTypeVacuum},
	}
	adminServer.inProgressTasks["task2"] = inProgressTask

	// Get system stats
	stats := adminServer.GetSystemStats()

	// Verify stats structure
	if !stats["running"].(bool) {
		t.Error("Expected running to be true")
	}

	if stats["in_progress_tasks"].(int) != 1 {
		t.Errorf("Expected 1 in-progress task, got %d", stats["in_progress_tasks"].(int))
	}

	if stats["queued_tasks"].(int) != 1 {
		t.Errorf("Expected 1 queued task, got %d", stats["queued_tasks"].(int))
	}

	// Check task breakdown
	tasksByType := stats["tasks_by_type"].(map[types.TaskType]int)
	if tasksByType[types.TaskTypeVacuum] != 1 {
		t.Errorf("Expected 1 vacuum task, got %d", tasksByType[types.TaskTypeVacuum])
	}

	t.Log("✅ System stats test passed")
}

func TestAdminServer_VolumeStateIntegration(t *testing.T) {
	// Integration test: Verify admin server correctly uses volume state for decisions
	adminServer := NewAdminServer(DefaultAdminConfig(), nil)
	adminServer.workerRegistry = NewWorkerRegistry()
	adminServer.taskQueue = NewPriorityTaskQueue()
	adminServer.volumeStateManager = NewVolumeStateManager(nil)
	adminServer.inProgressTasks = make(map[string]*InProgressTask)

	// Setup worker
	worker := &types.Worker{
		ID:            "worker1",
		Address:       "server1",
		Capabilities:  []types.TaskType{types.TaskTypeErasureCoding},
		MaxConcurrent: 1,
		Status:        "active",
		CurrentLoad:   0,
	}
	adminServer.workerRegistry.RegisterWorker(worker)

	// Setup volume and capacity that would normally allow EC
	volumeID := uint32(1)
	adminServer.volumeStateManager.volumes[volumeID] = &VolumeState{
		VolumeID: volumeID,
		CurrentState: &VolumeInfo{
			ID:     volumeID,
			Size:   25 * 1024 * 1024 * 1024, // 25GB
			Server: "server1",
		},
	}

	adminServer.volumeStateManager.capacityCache["server1"] = &CapacityInfo{
		Server:         "server1",
		TotalCapacity:  100 * 1024 * 1024 * 1024, // 100GB
		UsedCapacity:   20 * 1024 * 1024 * 1024,  // 20GB used
		PredictedUsage: 20 * 1024 * 1024 * 1024,  // 80GB available
	}

	// Create EC task
	task := &types.Task{
		ID:       "ec_task_1",
		Type:     types.TaskTypeErasureCoding,
		VolumeID: volumeID,
		Server:   "server1",
	}

	adminServer.taskQueue.Push(task)

	// First assignment should work
	assignedTask1, err := adminServer.RequestTask("worker1", []types.TaskType{types.TaskTypeErasureCoding})
	if err != nil || assignedTask1 == nil {
		t.Fatal("First EC task assignment should succeed")
	}

	// Verify capacity is now reserved
	capacity := adminServer.volumeStateManager.GetAccurateCapacity("server1")
	if capacity.ReservedCapacity <= 0 {
		t.Error("Expected capacity to be reserved for first EC task")
	}

	// Try to assign another large EC task - should fail due to capacity
	volumeID2 := uint32(2)
	adminServer.volumeStateManager.volumes[volumeID2] = &VolumeState{
		VolumeID: volumeID2,
		CurrentState: &VolumeInfo{
			ID:     volumeID2,
			Size:   30 * 1024 * 1024 * 1024, // 30GB - would need 42GB for EC
			Server: "server1",
		},
	}

	task2 := &types.Task{
		ID:       "ec_task_2",
		Type:     types.TaskTypeErasureCoding,
		VolumeID: volumeID2,
		Server:   "server1",
	}

	adminServer.taskQueue.Push(task2)

	// Add another worker to test capacity-based rejection
	worker2 := &types.Worker{
		ID:            "worker2",
		Address:       "server1",
		Capabilities:  []types.TaskType{types.TaskTypeErasureCoding},
		MaxConcurrent: 1,
		Status:        "active",
		CurrentLoad:   0,
	}
	adminServer.workerRegistry.RegisterWorker(worker2)

	assignedTask2, err := adminServer.RequestTask("worker2", []types.TaskType{types.TaskTypeErasureCoding})

	// Should not assign due to insufficient capacity
	if assignedTask2 != nil {
		t.Error("Should not assign second EC task due to insufficient server capacity")
	}

	t.Log("✅ Volume state integration test passed")
	t.Log("✅ Admin server correctly uses comprehensive state for task assignment decisions")
}

// Benchmark for task assignment performance
func BenchmarkAdminServer_RequestTask(b *testing.B) {
	adminServer := NewAdminServer(DefaultAdminConfig(), nil)
	adminServer.workerRegistry = NewWorkerRegistry()
	adminServer.taskQueue = NewPriorityTaskQueue()
	adminServer.volumeStateManager = NewVolumeStateManager(nil)
	adminServer.inProgressTasks = make(map[string]*InProgressTask)

	// Setup worker
	worker := &types.Worker{
		ID:            "bench_worker",
		Capabilities:  []types.TaskType{types.TaskTypeVacuum},
		MaxConcurrent: 1000, // High limit for benchmark
		Status:        "active",
		CurrentLoad:   0,
	}
	adminServer.workerRegistry.RegisterWorker(worker)

	// Setup many tasks
	for i := 0; i < 1000; i++ {
		volumeID := uint32(i + 1)
		adminServer.volumeStateManager.volumes[volumeID] = &VolumeState{
			VolumeID:     volumeID,
			CurrentState: &VolumeInfo{ID: volumeID, Size: 1024 * 1024 * 1024},
		}

		task := &types.Task{
			ID:       fmt.Sprintf("task_%d", i),
			Type:     types.TaskTypeVacuum,
			VolumeID: volumeID,
		}
		adminServer.taskQueue.Push(task)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		adminServer.RequestTask("bench_worker", []types.TaskType{types.TaskTypeVacuum})
	}
}
