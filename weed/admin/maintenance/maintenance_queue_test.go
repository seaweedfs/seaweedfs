package maintenance

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// Test suite for canScheduleTaskNow() function and related scheduling logic
//
// This test suite ensures that:
// 1. The fallback scheduling logic works correctly when no integration is present
// 2. Task concurrency limits are properly enforced per task type
// 3. Different task types don't interfere with each other's concurrency limits
// 4. Custom policies with higher concurrency limits work correctly
// 5. Edge cases (nil tasks, empty task types) are handled gracefully
// 6. Helper functions (GetRunningTaskCount, canExecuteTaskType, etc.) work correctly
//
// Background: The canScheduleTaskNow() function is critical for task assignment.
// It was previously failing due to an overly restrictive integration scheduler,
// so we implemented a temporary fix that bypasses the integration and uses
// fallback logic based on simple concurrency limits per task type.

func TestCanScheduleTaskNow_FallbackLogic(t *testing.T) {
	// Test the current implementation which uses fallback logic
	mq := &MaintenanceQueue{
		tasks:        make(map[string]*MaintenanceTask),
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
		policy:       nil, // No policy for default behavior
		integration:  nil, // No integration to force fallback
	}

	task := &MaintenanceTask{
		ID:     "test-task-1",
		Type:   MaintenanceTaskType("erasure_coding"),
		Status: TaskStatusPending,
	}

	// Should return true with fallback logic (no running tasks, default max concurrent = 1)
	result := mq.canScheduleTaskNow(task)
	if !result {
		t.Errorf("Expected canScheduleTaskNow to return true with fallback logic, got false")
	}
}

func TestCanScheduleTaskNow_FallbackWithRunningTasks(t *testing.T) {
	// Test fallback logic when there are already running tasks
	mq := &MaintenanceQueue{
		tasks: map[string]*MaintenanceTask{
			"running-task": {
				ID:     "running-task",
				Type:   MaintenanceTaskType("erasure_coding"),
				Status: TaskStatusInProgress,
			},
		},
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
		policy:       nil,
		integration:  nil,
	}

	task := &MaintenanceTask{
		ID:     "test-task-2",
		Type:   MaintenanceTaskType("erasure_coding"),
		Status: TaskStatusPending,
	}

	// Should return false because max concurrent is 1 and we have 1 running task
	result := mq.canScheduleTaskNow(task)
	if result {
		t.Errorf("Expected canScheduleTaskNow to return false when at capacity, got true")
	}
}

func TestCanScheduleTaskNow_DifferentTaskTypes(t *testing.T) {
	// Test that different task types don't interfere with each other
	mq := &MaintenanceQueue{
		tasks: map[string]*MaintenanceTask{
			"running-ec-task": {
				ID:     "running-ec-task",
				Type:   MaintenanceTaskType("erasure_coding"),
				Status: TaskStatusInProgress,
			},
		},
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
		policy:       nil,
		integration:  nil,
	}

	// Test vacuum task when EC task is running
	vacuumTask := &MaintenanceTask{
		ID:     "vacuum-task",
		Type:   MaintenanceTaskType("vacuum"),
		Status: TaskStatusPending,
	}

	// Should return true because vacuum and erasure_coding are different task types
	result := mq.canScheduleTaskNow(vacuumTask)
	if !result {
		t.Errorf("Expected canScheduleTaskNow to return true for different task type, got false")
	}

	// Test another EC task when one is already running
	ecTask := &MaintenanceTask{
		ID:     "ec-task",
		Type:   MaintenanceTaskType("erasure_coding"),
		Status: TaskStatusPending,
	}

	// Should return false because max concurrent for EC is 1 and we have 1 running
	result = mq.canScheduleTaskNow(ecTask)
	if result {
		t.Errorf("Expected canScheduleTaskNow to return false for same task type at capacity, got true")
	}
}

func TestCanScheduleTaskNow_WithIntegration(t *testing.T) {
	// Test with a real MaintenanceIntegration (will use fallback logic in current implementation)
	policy := &MaintenancePolicy{
		TaskPolicies:                 make(map[string]*worker_pb.TaskPolicy),
		GlobalMaxConcurrent:          10,
		DefaultRepeatIntervalSeconds: 24 * 60 * 60, // 24 hours in seconds
		DefaultCheckIntervalSeconds:  60 * 60,      // 1 hour in seconds
	}
	mq := NewMaintenanceQueue(policy)

	// Create a basic integration (this would normally be more complex)
	integration := NewMaintenanceIntegration(mq, policy)
	mq.SetIntegration(integration)

	task := &MaintenanceTask{
		ID:     "test-task-3",
		Type:   MaintenanceTaskType("erasure_coding"),
		Status: TaskStatusPending,
	}

	// With our current implementation (fallback logic), this should return true
	result := mq.canScheduleTaskNow(task)
	if !result {
		t.Errorf("Expected canScheduleTaskNow to return true with fallback logic, got false")
	}
}

func TestGetRunningTaskCount(t *testing.T) {
	// Test the helper function used by fallback logic
	mq := &MaintenanceQueue{
		tasks: map[string]*MaintenanceTask{
			"task1": {
				ID:     "task1",
				Type:   MaintenanceTaskType("erasure_coding"),
				Status: TaskStatusInProgress,
			},
			"task2": {
				ID:     "task2",
				Type:   MaintenanceTaskType("erasure_coding"),
				Status: TaskStatusAssigned,
			},
			"task3": {
				ID:     "task3",
				Type:   MaintenanceTaskType("vacuum"),
				Status: TaskStatusInProgress,
			},
			"task4": {
				ID:     "task4",
				Type:   MaintenanceTaskType("erasure_coding"),
				Status: TaskStatusCompleted,
			},
		},
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
	}

	// Should count 2 running EC tasks (in_progress + assigned)
	ecCount := mq.GetRunningTaskCount(MaintenanceTaskType("erasure_coding"))
	if ecCount != 2 {
		t.Errorf("Expected 2 running EC tasks, got %d", ecCount)
	}

	// Should count 1 running vacuum task
	vacuumCount := mq.GetRunningTaskCount(MaintenanceTaskType("vacuum"))
	if vacuumCount != 1 {
		t.Errorf("Expected 1 running vacuum task, got %d", vacuumCount)
	}

	// Should count 0 running balance tasks
	balanceCount := mq.GetRunningTaskCount(MaintenanceTaskType("balance"))
	if balanceCount != 0 {
		t.Errorf("Expected 0 running balance tasks, got %d", balanceCount)
	}
}

func TestCanExecuteTaskType(t *testing.T) {
	// Test the fallback logic helper function
	mq := &MaintenanceQueue{
		tasks: map[string]*MaintenanceTask{
			"running-task": {
				ID:     "running-task",
				Type:   MaintenanceTaskType("erasure_coding"),
				Status: TaskStatusInProgress,
			},
		},
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
		policy:       nil, // Will use default max concurrent = 1
		integration:  nil,
	}

	// Should return false for EC (1 running, max = 1)
	result := mq.canExecuteTaskType(MaintenanceTaskType("erasure_coding"))
	if result {
		t.Errorf("Expected canExecuteTaskType to return false for EC at capacity, got true")
	}

	// Should return true for vacuum (0 running, max = 1)
	result = mq.canExecuteTaskType(MaintenanceTaskType("vacuum"))
	if !result {
		t.Errorf("Expected canExecuteTaskType to return true for vacuum, got false")
	}
}

func TestGetMaxConcurrentForTaskType_DefaultBehavior(t *testing.T) {
	// Test the default behavior when no policy or integration is set
	mq := &MaintenanceQueue{
		tasks:        make(map[string]*MaintenanceTask),
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
		policy:       nil,
		integration:  nil,
	}

	// Should return default value of 1
	maxConcurrent := mq.getMaxConcurrentForTaskType(MaintenanceTaskType("erasure_coding"))
	if maxConcurrent != 1 {
		t.Errorf("Expected default max concurrent to be 1, got %d", maxConcurrent)
	}

	maxConcurrent = mq.getMaxConcurrentForTaskType(MaintenanceTaskType("vacuum"))
	if maxConcurrent != 1 {
		t.Errorf("Expected default max concurrent to be 1, got %d", maxConcurrent)
	}
}

// Test edge cases and error conditions
func TestCanScheduleTaskNow_NilTask(t *testing.T) {
	mq := &MaintenanceQueue{
		tasks:        make(map[string]*MaintenanceTask),
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
		policy:       nil,
		integration:  nil,
	}

	// This should panic with a nil task, so we expect and catch the panic
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected canScheduleTaskNow to panic with nil task, but it didn't")
		}
	}()

	// This should panic
	mq.canScheduleTaskNow(nil)
}

func TestCanScheduleTaskNow_EmptyTaskType(t *testing.T) {
	mq := &MaintenanceQueue{
		tasks:        make(map[string]*MaintenanceTask),
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
		policy:       nil,
		integration:  nil,
	}

	task := &MaintenanceTask{
		ID:     "empty-type-task",
		Type:   MaintenanceTaskType(""), // Empty task type
		Status: TaskStatusPending,
	}

	// Should handle empty task type gracefully
	result := mq.canScheduleTaskNow(task)
	if !result {
		t.Errorf("Expected canScheduleTaskNow to handle empty task type, got false")
	}
}

func TestCanScheduleTaskNow_WithPolicy(t *testing.T) {
	// Test with a policy that allows higher concurrency
	policy := &MaintenancePolicy{
		TaskPolicies: map[string]*worker_pb.TaskPolicy{
			string(MaintenanceTaskType("erasure_coding")): {
				Enabled:               true,
				MaxConcurrent:         3,
				RepeatIntervalSeconds: 60 * 60, // 1 hour
				CheckIntervalSeconds:  60 * 60, // 1 hour
			},
			string(MaintenanceTaskType("vacuum")): {
				Enabled:               true,
				MaxConcurrent:         2,
				RepeatIntervalSeconds: 60 * 60, // 1 hour
				CheckIntervalSeconds:  60 * 60, // 1 hour
			},
		},
		GlobalMaxConcurrent:          10,
		DefaultRepeatIntervalSeconds: 24 * 60 * 60, // 24 hours in seconds
		DefaultCheckIntervalSeconds:  60 * 60,      // 1 hour in seconds
	}

	mq := &MaintenanceQueue{
		tasks: map[string]*MaintenanceTask{
			"running-task-1": {
				ID:     "running-task-1",
				Type:   MaintenanceTaskType("erasure_coding"),
				Status: TaskStatusInProgress,
			},
			"running-task-2": {
				ID:     "running-task-2",
				Type:   MaintenanceTaskType("erasure_coding"),
				Status: TaskStatusAssigned,
			},
		},
		pendingTasks: []*MaintenanceTask{},
		workers:      make(map[string]*MaintenanceWorker),
		policy:       policy,
		integration:  nil,
	}

	task := &MaintenanceTask{
		ID:     "test-task-policy",
		Type:   MaintenanceTaskType("erasure_coding"),
		Status: TaskStatusPending,
	}

	// Should return true because we have 2 running EC tasks but max is 3
	result := mq.canScheduleTaskNow(task)
	if !result {
		t.Errorf("Expected canScheduleTaskNow to return true with policy allowing 3 concurrent, got false")
	}

	// Add one more running task to reach the limit
	mq.tasks["running-task-3"] = &MaintenanceTask{
		ID:     "running-task-3",
		Type:   MaintenanceTaskType("erasure_coding"),
		Status: TaskStatusInProgress,
	}

	// Should return false because we now have 3 running EC tasks (at limit)
	result = mq.canScheduleTaskNow(task)
	if result {
		t.Errorf("Expected canScheduleTaskNow to return false when at policy limit, got true")
	}
}

func TestMaintenanceQueue_TaskIDPreservation(t *testing.T) {
	// Setup Policy
	policy := &MaintenancePolicy{
		TaskPolicies:        make(map[string]*worker_pb.TaskPolicy),
		GlobalMaxConcurrent: 10,
	}

	// Setup Queue and Integration
	mq := NewMaintenanceQueue(policy)
	// We handle the integration manually to avoid complex setup
	// integration := NewMaintenanceIntegration(mq, policy)
	// mq.SetIntegration(integration)

	// 2. Verify ID Preservation in AddTasksFromResults
	originalID := "ec_task_123"
	results := []*TaskDetectionResult{
		{
			TaskID:      originalID,
			TaskType:    MaintenanceTaskType("erasure_coding"),
			VolumeID:    100,
			Server:      "server1",
			Priority:    PriorityNormal,
			TypedParams: &worker_pb.TaskParams{},
		},
	}

	mq.AddTasksFromResults(results)

	// Verify task exists with correct ID
	queuedTask, exists := mq.tasks[originalID]
	if !exists {
		t.Errorf("Task with original ID %s not found in queue", originalID)
	} else {
		if queuedTask.ID != originalID {
			t.Errorf("Task ID mismatch: expected %s, got %s", originalID, queuedTask.ID)
		}
	}

	// 3. Verify AddTask preserves ID
	manualTask := &MaintenanceTask{
		ID:     "manual_id_456",
		Type:   MaintenanceTaskType("vacuum"),
		Status: TaskStatusPending,
	}
	mq.AddTask(manualTask)

	if manualTask.ID != "manual_id_456" {
		t.Errorf("AddTask overwrote ID: expected manual_id_456, got %s", manualTask.ID)
	}
}

func TestMaintenanceQueue_ActiveTopologySync(t *testing.T) {
	// Setup Policy
	policy := &MaintenancePolicy{
		TaskPolicies: map[string]*worker_pb.TaskPolicy{
			"balance": {MaxConcurrent: 1},
		},
		GlobalMaxConcurrent: 10,
	}

	// Setup Queue and Integration
	mq := NewMaintenanceQueue(policy)
	integration := NewMaintenanceIntegration(mq, policy)
	mq.SetIntegration(integration)

	// 4. Verify ActiveTopology Synchronization (Assign and Complete)
	// Get and Setup Topology
	at := integration.GetActiveTopology()
	if at == nil {
		t.Fatalf("ActiveTopology not found in integration")
	}

	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "server1",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										DiskId:         1,
										VolumeCount:    1,
										MaxVolumeCount: 10,
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 100, Collection: "col1"},
										},
									},
									"hdd2": {
										DiskId:         2,
										VolumeCount:    0,
										MaxVolumeCount: 10,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	at.UpdateTopology(topologyInfo)

	// Add pending task to ActiveTopology
	taskID := "sync_test_123"
	err := at.AddPendingTask(topology.TaskSpec{
		TaskID:     taskID,
		TaskType:   topology.TaskTypeBalance,
		VolumeID:   100,
		VolumeSize: 1024 * 1024,
		Sources: []topology.TaskSourceSpec{
			{ServerID: "server1", DiskID: 1},
		},
		Destinations: []topology.TaskDestinationSpec{
			{ServerID: "server1", DiskID: 2},
		},
	})
	if err != nil {
		t.Fatalf("Failed to add pending task to ActiveTopology: %v", err)
	}

	// Add the same task to MaintenanceQueue
	mq.AddTask(&MaintenanceTask{
		ID:         taskID,
		Type:       MaintenanceTaskType("balance"),
		VolumeID:   100,
		Server:     "server1",
		Collection: "col1",
		TypedParams: &worker_pb.TaskParams{
			TaskId: taskID,
			Targets: []*worker_pb.TaskTarget{
				{Node: "server1", DiskId: 2},
			},
		},
	})

	// Check initial available capacity on destination disk (server1:2)
	// server1:2 has MaxVolumeCount=10, VolumeCount=0.
	// Capacity should be 9 because AddPendingTask already reserved 1 slot.
	capacityBefore := at.GetEffectiveAvailableCapacity("server1", 2)
	if capacityBefore != 9 {
		t.Errorf("Expected capacity 9 after AddPendingTask, got %d", capacityBefore)
	}

	// 5. Verify AssignTask (via GetNextTask)
	mq.workers["worker1"] = &MaintenanceWorker{
		ID:            "worker1",
		Status:        "active",
		Capabilities:  []MaintenanceTaskType{"balance"},
		MaxConcurrent: 10,
	}

	taskFound := mq.GetNextTask("worker1", []MaintenanceTaskType{"balance"})
	if taskFound == nil || taskFound.ID != taskID {
		t.Fatalf("Expected to get task %s, got %+v", taskID, taskFound)
	}

	// Capacity should still be 9 on destination disk (server1:2)
	capacityAfterAssign := at.GetEffectiveAvailableCapacity("server1", 2)
	if capacityAfterAssign != 9 {
		t.Errorf("Capacity should still be 9 after assignment, got %d", capacityAfterAssign)
	}

	// 6. Verify CompleteTask
	mq.CompleteTask(taskID, "")

	// Capacity should be released back to 10
	capacityAfterComplete := at.GetEffectiveAvailableCapacity("server1", 2)
	if capacityAfterComplete != 10 {
		t.Errorf("Capacity should have returned to 10 after completion, got %d", capacityAfterComplete)
	}
}

func TestMaintenanceQueue_StaleWorkerCapacityRelease(t *testing.T) {
	// Setup
	policy := &MaintenancePolicy{
		TaskPolicies: map[string]*worker_pb.TaskPolicy{
			"balance": {MaxConcurrent: 1},
		},
	}
	mq := NewMaintenanceQueue(policy)
	integration := NewMaintenanceIntegration(mq, policy)
	mq.SetIntegration(integration)
	at := integration.GetActiveTopology()

	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "server1",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd1": {DiskId: 1, VolumeCount: 1, MaxVolumeCount: 10},
									"hdd2": {DiskId: 2, VolumeCount: 0, MaxVolumeCount: 10},
								},
							},
						},
					},
				},
			},
		},
	}
	at.UpdateTopology(topologyInfo)

	taskID := "stale_test_123"
	at.AddPendingTask(topology.TaskSpec{
		TaskID:       taskID,
		TaskType:     topology.TaskTypeBalance,
		VolumeID:     100,
		VolumeSize:   1024,
		Sources:      []topology.TaskSourceSpec{{ServerID: "server1", DiskID: 1}},
		Destinations: []topology.TaskDestinationSpec{{ServerID: "server1", DiskID: 2}},
	})

	mq.AddTask(&MaintenanceTask{
		ID:       taskID,
		Type:     "balance",
		VolumeID: 100,
		Server:   "server1",
		TypedParams: &worker_pb.TaskParams{
			TaskId:  taskID,
			Targets: []*worker_pb.TaskTarget{{Node: "server1", DiskId: 2}},
		},
	})

	mq.workers["worker1"] = &MaintenanceWorker{
		ID:            "worker1",
		Status:        "active",
		Capabilities:  []MaintenanceTaskType{"balance"},
		MaxConcurrent: 1,
		LastHeartbeat: time.Now(),
	}

	// Assign task
	mq.GetNextTask("worker1", []MaintenanceTaskType{"balance"})

	// Verify capacity reserved (9 left)
	if at.GetEffectiveAvailableCapacity("server1", 2) != 9 {
		t.Errorf("Expected capacity 9, got %d", at.GetEffectiveAvailableCapacity("server1", 2))
	}

	// Make worker stale
	mq.workers["worker1"].LastHeartbeat = time.Now().Add(-1 * time.Hour)

	// Remove stale workers
	mq.RemoveStaleWorkers(10 * time.Minute)

	// Verify capacity released (back to 10)
	if at.GetEffectiveAvailableCapacity("server1", 2) != 10 {
		t.Errorf("Expected capacity 10 after removing stale worker, got %d", at.GetEffectiveAvailableCapacity("server1", 2))
	}
}

func TestMaintenanceManager_CancelTaskCapacityRelease(t *testing.T) {
	// Setup Manager
	config := DefaultMaintenanceConfig()
	mm := NewMaintenanceManager(nil, config)
	integration := mm.scanner.integration
	mq := mm.queue
	at := integration.GetActiveTopology()

	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "server1",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd1": {DiskId: 1, VolumeCount: 1, MaxVolumeCount: 10},
									"hdd2": {DiskId: 2, VolumeCount: 0, MaxVolumeCount: 10},
								},
							},
						},
					},
				},
			},
		},
	}
	at.UpdateTopology(topologyInfo)

	taskID := "cancel_test_123"
	// Note: AddPendingTask reserves capacity
	at.AddPendingTask(topology.TaskSpec{
		TaskID:       taskID,
		TaskType:     topology.TaskTypeBalance,
		VolumeID:     100,
		VolumeSize:   1024,
		Sources:      []topology.TaskSourceSpec{{ServerID: "server1", DiskID: 1}},
		Destinations: []topology.TaskDestinationSpec{{ServerID: "server1", DiskID: 2}},
	})

	mq.AddTask(&MaintenanceTask{
		ID:       taskID,
		Type:     "balance",
		VolumeID: 100,
		Server:   "server1",
		TypedParams: &worker_pb.TaskParams{
			TaskId:  taskID,
			Targets: []*worker_pb.TaskTarget{{Node: "server1", DiskId: 2}},
		},
	})

	// Verify capacity reserved (9 left)
	if at.GetEffectiveAvailableCapacity("server1", 2) != 9 {
		t.Errorf("Expected capacity 9, got %d", at.GetEffectiveAvailableCapacity("server1", 2))
	}

	// Cancel task
	err := mm.CancelTask(taskID)
	if err != nil {
		t.Fatalf("Failed to cancel task: %v", err)
	}

	// Verify capacity released (back to 10)
	if at.GetEffectiveAvailableCapacity("server1", 2) != 10 {
		t.Errorf("Expected capacity 10 after cancelling task, got %d", at.GetEffectiveAvailableCapacity("server1", 2))
	}
}

type MockPersistence struct {
	tasks []*MaintenanceTask
}

func (m *MockPersistence) SaveTaskState(task *MaintenanceTask) error                { return nil }
func (m *MockPersistence) LoadTaskState(taskID string) (*MaintenanceTask, error)    { return nil, nil }
func (m *MockPersistence) LoadAllTaskStates() ([]*MaintenanceTask, error)           { return m.tasks, nil }
func (m *MockPersistence) DeleteTaskState(taskID string) error                      { return nil }
func (m *MockPersistence) CleanupCompletedTasks() error                             { return nil }
func (m *MockPersistence) SaveTaskPolicy(taskType string, policy *TaskPolicy) error { return nil }

func TestMaintenanceQueue_LoadTasksCapacitySync(t *testing.T) {
	// Setup
	policy := &MaintenancePolicy{
		TaskPolicies: map[string]*worker_pb.TaskPolicy{
			"balance": {MaxConcurrent: 1},
		},
	}
	mq := NewMaintenanceQueue(policy)
	integration := NewMaintenanceIntegration(mq, policy)
	mq.SetIntegration(integration)
	at := integration.GetActiveTopology()

	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "server1",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd1": {DiskId: 1, VolumeCount: 1, MaxVolumeCount: 10},
									"hdd2": {DiskId: 2, VolumeCount: 0, MaxVolumeCount: 10},
								},
							},
						},
					},
				},
			},
		},
	}
	at.UpdateTopology(topologyInfo)

	// Setup mock persistence with a pending task
	taskID := "load_test_123"
	mockTask := &MaintenanceTask{
		ID:     taskID,
		Type:   "balance",
		Status: TaskStatusPending,
		TypedParams: &worker_pb.TaskParams{
			TaskId:  taskID,
			Sources: []*worker_pb.TaskSource{{Node: "server1", DiskId: 1}},
			Targets: []*worker_pb.TaskTarget{{Node: "server1", DiskId: 2}},
		},
	}
	mq.SetPersistence(&MockPersistence{tasks: []*MaintenanceTask{mockTask}})

	// Load tasks
	err := mq.LoadTasksFromPersistence()
	if err != nil {
		t.Fatalf("Failed to load tasks: %v", err)
	}

	// Verify capacity is reserved in ActiveTopology after loading (9 left)
	if at.GetEffectiveAvailableCapacity("server1", 2) != 9 {
		t.Errorf("Expected capacity 9 after loading tasks, got %d", at.GetEffectiveAvailableCapacity("server1", 2))
	}
}

func TestMaintenanceQueue_AssignTaskRollback(t *testing.T) {
	// Setup Policy
	policy := &MaintenancePolicy{
		TaskPolicies: map[string]*worker_pb.TaskPolicy{
			"balance": {MaxConcurrent: 1},
		},
		GlobalMaxConcurrent: 10,
	}

	// Setup Queue and Integration
	mq := NewMaintenanceQueue(policy)
	integration := NewMaintenanceIntegration(mq, policy)
	mq.SetIntegration(integration)

	// Get Topology
	at := integration.GetActiveTopology()
	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "server1",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										DiskId:         1,
										VolumeCount:    1,
										MaxVolumeCount: 1, // Only 1 slot
										VolumeInfos: []*master_pb.VolumeInformationMessage{
											{Id: 100, Collection: "col1"},
										},
									},
									"hdd2": {
										DiskId:         2,
										VolumeCount:    0,
										MaxVolumeCount: 0, // NO CAPACITY for target
									},
								},
							},
						},
					},
				},
			},
		},
	}
	at.UpdateTopology(topologyInfo)

	taskID := "rollback_test_123"

	// 1. Add task to MaintenanceQueue ONLY
	// It's not in ActiveTopology, so AssignTask will fail with "pending task not found"
	mq.AddTask(&MaintenanceTask{
		ID:         taskID,
		Type:       MaintenanceTaskType("balance"),
		VolumeID:   100,
		Server:     "server1",
		Collection: "col1",
		TypedParams: &worker_pb.TaskParams{
			TaskId: taskID,
			Targets: []*worker_pb.TaskTarget{
				{Node: "server1", DiskId: 2},
			},
		},
	})

	// 2. Setup worker
	mq.workers["worker1"] = &MaintenanceWorker{
		ID:            "worker1",
		Status:        "active",
		Capabilities:  []MaintenanceTaskType{"balance"},
		MaxConcurrent: 10,
	}

	// 3. Try to get next task
	taskFound := mq.GetNextTask("worker1", []MaintenanceTaskType{"balance"})

	// 4. Verify GetNextTask returned nil due to ActiveTopology.AssignTask failure
	if taskFound != nil {
		t.Errorf("Expected GetNextTask to return nil, got task %s", taskFound.ID)
	}

	// 5. Verify the task in MaintenanceQueue is rolled back to pending
	mq.mutex.RLock()
	task, exists := mq.tasks[taskID]
	mq.mutex.RUnlock()

	if !exists {
		t.Fatalf("Task %s should still exist in MaintenanceQueue", taskID)
	}
	if task.Status != TaskStatusPending {
		t.Errorf("Expected task status %v, got %v", TaskStatusPending, task.Status)
	}
	if task.WorkerID != "" {
		t.Errorf("Expected task WorkerID to be empty, got %s", task.WorkerID)
	}
	if len(task.AssignmentHistory) != 0 {
		t.Errorf("Expected assignment history to be empty, got %d records", len(task.AssignmentHistory))
	}

	// 6. Verify the task is still in pendingTasks slice
	mq.mutex.RLock()
	foundInPending := false
	for _, pt := range mq.pendingTasks {
		if pt.ID == taskID {
			foundInPending = true
			break
		}
	}
	mq.mutex.RUnlock()

	if !foundInPending {
		t.Errorf("Task %s should still be in pendingTasks slice", taskID)
	}
}
