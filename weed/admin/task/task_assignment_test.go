package task

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestTaskAssignment_BasicAssignment(t *testing.T) {
	registry := NewWorkerRegistry()
	queue := NewPriorityTaskQueue()
	scheduler := NewTaskScheduler(registry, queue)

	// Register worker
	worker := &types.Worker{
		ID:            "worker1",
		Capabilities:  []types.TaskType{types.TaskTypeVacuum},
		MaxConcurrent: 1,
		Status:        "active",
		CurrentLoad:   0,
	}
	registry.RegisterWorker(worker)

	// Create task
	task := &types.Task{
		ID:       "task1",
		Type:     types.TaskTypeVacuum,
		Priority: types.TaskPriorityNormal,
	}
	queue.Push(task)

	// Test assignment
	nextTask := scheduler.GetNextTask("worker1", []types.TaskType{types.TaskTypeVacuum})
	if nextTask == nil {
		t.Fatal("Expected task to be assigned")
	}

	if nextTask.ID != "task1" {
		t.Errorf("Expected task1, got %s", nextTask.ID)
	}

	t.Log("✅ Basic task assignment test passed")
}

func TestTaskAssignment_CapabilityMatching(t *testing.T) {
	registry := NewWorkerRegistry()
	queue := NewPriorityTaskQueue()
	scheduler := NewTaskScheduler(registry, queue)

	// Register workers with different capabilities
	ecWorker := &types.Worker{
		ID:           "ec_worker",
		Capabilities: []types.TaskType{types.TaskTypeErasureCoding},
		Status:       "active",
		CurrentLoad:  0,
	}
	registry.RegisterWorker(ecWorker)

	vacuumWorker := &types.Worker{
		ID:           "vacuum_worker",
		Capabilities: []types.TaskType{types.TaskTypeVacuum},
		Status:       "active",
		CurrentLoad:  0,
	}
	registry.RegisterWorker(vacuumWorker)

	// Create different types of tasks
	ecTask := &types.Task{
		ID:   "ec_task",
		Type: types.TaskTypeErasureCoding,
	}
	vacuumTask := &types.Task{
		ID:   "vacuum_task",
		Type: types.TaskTypeVacuum,
	}

	queue.Push(ecTask)
	queue.Push(vacuumTask)

	// Test EC worker gets EC task
	assignedECTask := scheduler.GetNextTask("ec_worker", []types.TaskType{types.TaskTypeErasureCoding})
	if assignedECTask == nil || assignedECTask.Type != types.TaskTypeErasureCoding {
		t.Error("EC worker should get EC task")
	}

	// Test vacuum worker gets vacuum task
	assignedVacuumTask := scheduler.GetNextTask("vacuum_worker", []types.TaskType{types.TaskTypeVacuum})
	if assignedVacuumTask == nil || assignedVacuumTask.Type != types.TaskTypeVacuum {
		t.Error("Vacuum worker should get vacuum task")
	}

	// Test wrong capability - should get nothing
	wrongTask := scheduler.GetNextTask("ec_worker", []types.TaskType{types.TaskTypeVacuum})
	if wrongTask != nil {
		t.Error("EC worker should not get vacuum task")
	}

	t.Log("✅ Capability matching test passed")
}

func TestTaskAssignment_PriorityOrdering(t *testing.T) {
	queue := NewPriorityTaskQueue()

	// Add tasks in reverse priority order
	lowTask := &types.Task{
		ID:       "low_task",
		Priority: types.TaskPriorityLow,
	}
	highTask := &types.Task{
		ID:       "high_task",
		Priority: types.TaskPriorityHigh,
	}
	normalTask := &types.Task{
		ID:       "normal_task",
		Priority: types.TaskPriorityNormal,
	}

	queue.Push(lowTask)
	queue.Push(normalTask)
	queue.Push(highTask)

	// Should get high priority first
	first := queue.Pop()
	if first.Priority != types.TaskPriorityHigh {
		t.Errorf("Expected high priority first, got %d", first.Priority)
	}

	// Then normal priority
	second := queue.Pop()
	if second.Priority != types.TaskPriorityNormal {
		t.Errorf("Expected normal priority second, got %d", second.Priority)
	}

	// Finally low priority
	third := queue.Pop()
	if third.Priority != types.TaskPriorityLow {
		t.Errorf("Expected low priority third, got %d", third.Priority)
	}

	t.Log("✅ Priority ordering test passed")
}

func TestTaskAssignment_WorkerCapacityLimits(t *testing.T) {
	registry := NewWorkerRegistry()

	// Register worker with limited capacity
	worker := &types.Worker{
		ID:            "limited_worker",
		Capabilities:  []types.TaskType{types.TaskTypeVacuum},
		MaxConcurrent: 2,
		Status:        "active",
		CurrentLoad:   2, // Already at capacity
	}
	registry.RegisterWorker(worker)

	// Worker should not be available
	availableWorkers := registry.GetAvailableWorkers()
	if len(availableWorkers) != 0 {
		t.Error("Worker at capacity should not be available")
	}

	// Reduce load
	worker.CurrentLoad = 1

	// Worker should now be available
	availableWorkers = registry.GetAvailableWorkers()
	if len(availableWorkers) != 1 {
		t.Error("Worker with capacity should be available")
	}

	t.Log("✅ Worker capacity limits test passed")
}

func TestTaskAssignment_ScheduledTasks(t *testing.T) {
	registry := NewWorkerRegistry()
	queue := NewPriorityTaskQueue()
	scheduler := NewTaskScheduler(registry, queue)

	worker := &types.Worker{
		ID:           "worker1",
		Capabilities: []types.TaskType{types.TaskTypeVacuum},
		Status:       "active",
		CurrentLoad:  0,
	}
	registry.RegisterWorker(worker)

	// Create task scheduled for future
	futureTask := &types.Task{
		ID:          "future_task",
		Type:        types.TaskTypeVacuum,
		ScheduledAt: time.Now().Add(1 * time.Hour), // 1 hour from now
	}

	// Create task ready now
	readyTask := &types.Task{
		ID:          "ready_task",
		Type:        types.TaskTypeVacuum,
		ScheduledAt: time.Now().Add(-1 * time.Minute), // 1 minute ago
	}

	queue.Push(futureTask)
	queue.Push(readyTask)

	// Should get ready task, not future task
	assignedTask := scheduler.GetNextTask("worker1", []types.TaskType{types.TaskTypeVacuum})
	if assignedTask == nil || assignedTask.ID != "ready_task" {
		t.Error("Should assign ready task, not future scheduled task")
	}

	t.Log("✅ Scheduled tasks test passed")
}

func TestTaskAssignment_WorkerSelection(t *testing.T) {
	registry := NewWorkerRegistry()
	queue := NewPriorityTaskQueue()
	scheduler := NewTaskScheduler(registry, queue)

	// Register workers with different characteristics
	highPerformanceWorker := &types.Worker{
		ID:            "high_perf_worker",
		Address:       "server1",
		Capabilities:  []types.TaskType{types.TaskTypeErasureCoding},
		Status:        "active",
		CurrentLoad:   0,
		MaxConcurrent: 4,
	}

	lowPerformanceWorker := &types.Worker{
		ID:            "low_perf_worker",
		Address:       "server2",
		Capabilities:  []types.TaskType{types.TaskTypeErasureCoding},
		Status:        "active",
		CurrentLoad:   1,
		MaxConcurrent: 2,
	}

	registry.RegisterWorker(highPerformanceWorker)
	registry.RegisterWorker(lowPerformanceWorker)

	// Set up metrics to favor high performance worker
	registry.metrics[highPerformanceWorker.ID] = &WorkerMetrics{
		TasksCompleted:  100,
		TasksFailed:     5,
		SuccessRate:     0.95,
		AverageTaskTime: 10 * time.Minute,
		LastTaskTime:    time.Now().Add(-5 * time.Minute),
	}

	registry.metrics[lowPerformanceWorker.ID] = &WorkerMetrics{
		TasksCompleted:  50,
		TasksFailed:     10,
		SuccessRate:     0.83,
		AverageTaskTime: 20 * time.Minute,
		LastTaskTime:    time.Now().Add(-1 * time.Hour),
	}

	// Create high priority task
	task := &types.Task{
		ID:       "important_task",
		Type:     types.TaskTypeErasureCoding,
		Priority: types.TaskPriorityHigh,
		Server:   "server1", // Prefers server1
	}

	availableWorkers := []*types.Worker{highPerformanceWorker, lowPerformanceWorker}
	selectedWorker := scheduler.SelectWorker(task, availableWorkers)

	if selectedWorker == nil {
		t.Fatal("No worker selected")
	}

	if selectedWorker.ID != "high_perf_worker" {
		t.Errorf("Expected high performance worker to be selected, got %s", selectedWorker.ID)
	}

	t.Log("✅ Worker selection test passed")
}

func TestTaskAssignment_ServerAffinity(t *testing.T) {
	registry := NewWorkerRegistry()
	queue := NewPriorityTaskQueue()
	scheduler := NewTaskScheduler(registry, queue)

	// Workers on different servers
	worker1 := &types.Worker{
		ID:           "worker1",
		Address:      "server1",
		Capabilities: []types.TaskType{types.TaskTypeVacuum},
		Status:       "active",
		CurrentLoad:  0,
	}

	worker2 := &types.Worker{
		ID:           "worker2",
		Address:      "server2",
		Capabilities: []types.TaskType{types.TaskTypeVacuum},
		Status:       "active",
		CurrentLoad:  0,
	}

	registry.RegisterWorker(worker1)
	registry.RegisterWorker(worker2)

	// Task that prefers server1
	task := &types.Task{
		ID:     "affinity_task",
		Type:   types.TaskTypeVacuum,
		Server: "server1", // Should prefer worker on server1
	}

	availableWorkers := []*types.Worker{worker1, worker2}
	selectedWorker := scheduler.SelectWorker(task, availableWorkers)

	if selectedWorker == nil {
		t.Fatal("No worker selected")
	}

	if selectedWorker.Address != "server1" {
		t.Errorf("Expected worker on server1 to be selected for server affinity")
	}

	t.Log("✅ Server affinity test passed")
}

func TestTaskAssignment_DuplicateTaskPrevention(t *testing.T) {
	queue := NewPriorityTaskQueue()

	// Add initial task
	task1 := &types.Task{
		ID:       "task1",
		Type:     types.TaskTypeVacuum,
		VolumeID: 1,
	}
	queue.Push(task1)

	// Check for duplicate
	hasDuplicate := queue.HasTask(1, types.TaskTypeVacuum)
	if !hasDuplicate {
		t.Error("Should detect existing task for volume")
	}

	// Check for non-existent task
	hasNonExistent := queue.HasTask(2, types.TaskTypeVacuum)
	if hasNonExistent {
		t.Error("Should not detect task for different volume")
	}

	// Check for different task type
	hasDifferentType := queue.HasTask(1, types.TaskTypeErasureCoding)
	if hasDifferentType {
		t.Error("Should not detect different task type for same volume")
	}

	t.Log("✅ Duplicate task prevention test passed")
}

func TestTaskAssignment_TaskRemoval(t *testing.T) {
	queue := NewPriorityTaskQueue()

	// Add tasks
	task1 := &types.Task{ID: "task1", Priority: types.TaskPriorityNormal}
	task2 := &types.Task{ID: "task2", Priority: types.TaskPriorityHigh}
	task3 := &types.Task{ID: "task3", Priority: types.TaskPriorityLow}

	queue.Push(task1)
	queue.Push(task2)
	queue.Push(task3)

	if queue.Size() != 3 {
		t.Errorf("Expected queue size 3, got %d", queue.Size())
	}

	// Remove middle priority task
	removed := queue.RemoveTask("task1")
	if !removed {
		t.Error("Should have removed task1")
	}

	if queue.Size() != 2 {
		t.Errorf("Expected queue size 2 after removal, got %d", queue.Size())
	}

	// Verify order maintained (high priority first)
	next := queue.Peek()
	if next.ID != "task2" {
		t.Errorf("Expected task2 (high priority) to be next, got %s", next.ID)
	}

	t.Log("✅ Task removal test passed")
}

func TestTaskAssignment_EdgeCases(t *testing.T) {
	t.Run("EmptyQueue", func(t *testing.T) {
		registry := NewWorkerRegistry()
		queue := NewPriorityTaskQueue()
		scheduler := NewTaskScheduler(registry, queue)

		worker := &types.Worker{
			ID:           "worker1",
			Capabilities: []types.TaskType{types.TaskTypeVacuum},
			Status:       "active",
		}
		registry.RegisterWorker(worker)

		// Empty queue should return nil
		task := scheduler.GetNextTask("worker1", []types.TaskType{types.TaskTypeVacuum})
		if task != nil {
			t.Error("Empty queue should return nil task")
		}
	})

	t.Run("UnknownWorker", func(t *testing.T) {
		registry := NewWorkerRegistry()
		queue := NewPriorityTaskQueue()
		scheduler := NewTaskScheduler(registry, queue)

		task := &types.Task{ID: "task1", Type: types.TaskTypeVacuum}
		queue.Push(task)

		// Unknown worker should return nil
		assignedTask := scheduler.GetNextTask("unknown_worker", []types.TaskType{types.TaskTypeVacuum})
		if assignedTask != nil {
			t.Error("Unknown worker should not get tasks")
		}
	})

	t.Run("InactiveWorker", func(t *testing.T) {
		registry := NewWorkerRegistry()

		worker := &types.Worker{
			ID:           "inactive_worker",
			Capabilities: []types.TaskType{types.TaskTypeVacuum},
			Status:       "inactive",
			CurrentLoad:  0,
		}
		registry.RegisterWorker(worker)

		// Inactive worker should not be available
		available := registry.GetAvailableWorkers()
		if len(available) != 0 {
			t.Error("Inactive worker should not be available")
		}
	})

	t.Log("✅ Edge cases test passed")
}

// Performance test for task assignment
func BenchmarkTaskAssignment_GetNextTask(b *testing.B) {
	registry := NewWorkerRegistry()
	queue := NewPriorityTaskQueue()
	scheduler := NewTaskScheduler(registry, queue)

	// Setup worker
	worker := &types.Worker{
		ID:           "bench_worker",
		Capabilities: []types.TaskType{types.TaskTypeVacuum},
		Status:       "active",
		CurrentLoad:  0,
	}
	registry.RegisterWorker(worker)

	// Add many tasks
	for i := 0; i < 1000; i++ {
		task := &types.Task{
			ID:       fmt.Sprintf("task_%d", i),
			Type:     types.TaskTypeVacuum,
			Priority: types.TaskPriorityNormal,
		}
		queue.Push(task)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scheduler.GetNextTask("bench_worker", []types.TaskType{types.TaskTypeVacuum})
	}
}

func BenchmarkTaskAssignment_WorkerSelection(b *testing.B) {
	registry := NewWorkerRegistry()
	scheduler := NewTaskScheduler(registry, nil)

	// Create many workers
	workers := make([]*types.Worker, 100)
	for i := 0; i < 100; i++ {
		worker := &types.Worker{
			ID:           fmt.Sprintf("worker_%d", i),
			Capabilities: []types.TaskType{types.TaskTypeVacuum},
			Status:       "active",
			CurrentLoad:  i % 3, // Varying loads
		}
		registry.RegisterWorker(worker)
		workers[i] = worker
	}

	task := &types.Task{
		ID:   "bench_task",
		Type: types.TaskTypeVacuum,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scheduler.SelectWorker(task, workers)
	}
}
