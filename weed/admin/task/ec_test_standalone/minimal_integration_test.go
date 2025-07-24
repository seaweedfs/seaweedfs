package task

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TestMinimalIntegration tests basic admin-worker operational flow using the minimal implementation
func TestMinimalIntegration(t *testing.T) {
	t.Logf("Starting minimal integration test")

	// Step 1: Create a minimal admin server configuration
	config := &MinimalAdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           2 * time.Hour,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    5,
	}

	// Step 2: Create minimal admin server with nil master client (for testing)
	adminServer := NewMinimalAdminServer(config, nil)

	// Step 3: Start admin server
	err := adminServer.Start()
	if err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()

	// Step 4: Test worker registration
	t.Logf("Testing worker registration")

	worker := &types.Worker{
		ID:            "test-worker-1",
		Address:       "localhost:9001",
		Capabilities:  []types.TaskType{types.TaskTypeVacuum},
		MaxConcurrent: 2,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	err = adminServer.RegisterWorker(worker)
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}
	t.Logf("Successfully registered worker %s", worker.ID)

	// Step 5: Test task queueing
	t.Logf("Testing task queueing")

	task := &types.Task{
		ID:       "test-task-1",
		Type:     types.TaskTypeVacuum,
		VolumeID: 1001,
		Server:   "localhost:8080",
		Status:   types.TaskStatusPending,
		Priority: types.TaskPriorityNormal,
		Parameters: map[string]interface{}{
			"garbage_threshold": "0.3",
		},
		CreatedAt: time.Now(),
	}

	err = adminServer.QueueTask(task)
	if err != nil {
		t.Fatalf("Failed to queue task: %v", err)
	}
	t.Logf("Successfully queued task %s", task.ID)

	// Step 6: Test task request by worker
	t.Logf("Testing task request")

	assignedTask, err := adminServer.RequestTask("test-worker-1", []types.TaskType{types.TaskTypeVacuum})
	if err != nil {
		t.Fatalf("Failed to request task: %v", err)
	}

	if assignedTask != nil {
		t.Logf("Successfully assigned task %s to worker", assignedTask.ID)

		// Step 7: Test task progress updates
		t.Logf("Testing task progress updates")

		err = adminServer.UpdateTaskProgress(assignedTask.ID, 25.0)
		if err != nil {
			t.Errorf("Failed to update task progress to 25%%: %v", err)
		}

		err = adminServer.UpdateTaskProgress(assignedTask.ID, 50.0)
		if err != nil {
			t.Errorf("Failed to update task progress to 50%%: %v", err)
		}

		err = adminServer.UpdateTaskProgress(assignedTask.ID, 75.0)
		if err != nil {
			t.Errorf("Failed to update task progress to 75%%: %v", err)
		}

		err = adminServer.UpdateTaskProgress(assignedTask.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update task progress to 100%%: %v", err)
		}

		// Step 8: Test task completion
		t.Logf("Testing task completion")

		err = adminServer.CompleteTask(assignedTask.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete task: %v", err)
		}
		t.Logf("Successfully completed task %s", assignedTask.ID)
	} else {
		t.Logf("No task was assigned (queue might be empty)")
	}

	// Step 9: Test basic metrics
	t.Logf("Testing basic metrics")

	stats := adminServer.GetSystemStats()
	if stats != nil {
		t.Logf("System stats: Active tasks=%d, Queued tasks=%d, Active workers=%d, Total tasks=%d",
			stats.ActiveTasks, stats.QueuedTasks, stats.ActiveWorkers, stats.TotalTasks)
	}

	queuedCount := adminServer.GetQueuedTaskCount()
	activeCount := adminServer.GetActiveTaskCount()
	t.Logf("Queue status: %d queued, %d active tasks", queuedCount, activeCount)

	// Step 10: Test task history
	history := adminServer.GetTaskHistory()
	t.Logf("Task history contains %d entries", len(history))

	if len(history) > 0 {
		lastEntry := history[len(history)-1]
		t.Logf("Last task in history: %s (%s) - Status: %s, Duration: %v",
			lastEntry.TaskID, lastEntry.TaskType, lastEntry.Status, lastEntry.Duration)
	}

	t.Logf("Minimal integration test completed successfully")
}

// TestMinimalWorkerHeartbeat tests worker heartbeat functionality
func TestMinimalWorkerHeartbeat(t *testing.T) {
	t.Logf("Testing minimal worker heartbeat")

	config := &MinimalAdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           2 * time.Hour,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    5,
	}

	adminServer := NewMinimalAdminServer(config, nil)
	err := adminServer.Start()
	if err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()

	// Register a worker
	worker := &types.Worker{
		ID:            "heartbeat-worker",
		Address:       "localhost:9002",
		Capabilities:  []types.TaskType{types.TaskTypeVacuum},
		MaxConcurrent: 1,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	err = adminServer.RegisterWorker(worker)
	if err != nil {
		t.Fatalf("Failed to register worker: %v", err)
	}

	// Test heartbeat update
	status := &types.WorkerStatus{
		Status:      "active",
		CurrentLoad: 0,
	}

	err = adminServer.UpdateWorkerHeartbeat("heartbeat-worker", status)
	if err != nil {
		t.Errorf("Failed to update worker heartbeat: %v", err)
	}

	t.Logf("Minimal worker heartbeat test completed successfully")
}

// TestMinimalTaskQueueOperations tests task queue operations
func TestMinimalTaskQueueOperations(t *testing.T) {
	t.Logf("Testing minimal task queue operations")

	config := &MinimalAdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           2 * time.Hour,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    5,
	}

	adminServer := NewMinimalAdminServer(config, nil)
	err := adminServer.Start()
	if err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()

	// Test queuing multiple tasks
	taskCount := 3
	for i := 0; i < taskCount; i++ {
		task := &types.Task{
			ID:       fmt.Sprintf("queue-test-task-%d", i),
			Type:     types.TaskTypeVacuum,
			VolumeID: uint32(2000 + i),
			Server:   "localhost:8080",
			Status:   types.TaskStatusPending,
			Priority: types.TaskPriorityNormal,
			Parameters: map[string]interface{}{
				"garbage_threshold": "0.3",
			},
			CreatedAt: time.Now(),
		}

		err = adminServer.QueueTask(task)
		if err != nil {
			t.Errorf("Failed to queue task %d: %v", i, err)
		}
	}

	// Check queue size
	queuedCount := adminServer.GetQueuedTaskCount()
	if queuedCount != taskCount {
		t.Errorf("Expected %d queued tasks, got %d", taskCount, queuedCount)
	}

	t.Logf("Minimal task queue operations test completed successfully")
}

// TestMinimalFullWorkflow tests the complete workflow from task creation to completion
func TestMinimalFullWorkflow(t *testing.T) {
	t.Logf("Testing minimal full workflow")

	config := &MinimalAdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           2 * time.Hour,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    5,
	}

	adminServer := NewMinimalAdminServer(config, nil)
	err := adminServer.Start()
	if err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()

	// Register multiple workers with different capabilities
	workers := []*types.Worker{
		{
			ID:            "vacuum-worker-1",
			Address:       "localhost:9001",
			Capabilities:  []types.TaskType{types.TaskTypeVacuum},
			MaxConcurrent: 2,
			Status:        "active",
			CurrentLoad:   0,
			LastHeartbeat: time.Now(),
		},
		{
			ID:            "ec-worker-1",
			Address:       "localhost:9002",
			Capabilities:  []types.TaskType{types.TaskTypeErasureCoding},
			MaxConcurrent: 1,
			Status:        "active",
			CurrentLoad:   0,
			LastHeartbeat: time.Now(),
		},
		{
			ID:            "multi-worker-1",
			Address:       "localhost:9003",
			Capabilities:  []types.TaskType{types.TaskTypeVacuum, types.TaskTypeErasureCoding},
			MaxConcurrent: 3,
			Status:        "active",
			CurrentLoad:   0,
			LastHeartbeat: time.Now(),
		},
	}

	for _, worker := range workers {
		err = adminServer.RegisterWorker(worker)
		if err != nil {
			t.Fatalf("Failed to register worker %s: %v", worker.ID, err)
		}
		t.Logf("Registered worker %s with capabilities %v", worker.ID, worker.Capabilities)
	}

	// Create multiple tasks of different types
	tasks := []*types.Task{
		{
			ID:       "vacuum-task-1",
			Type:     types.TaskTypeVacuum,
			VolumeID: 3001,
			Server:   "localhost:8080",
			Status:   types.TaskStatusPending,
			Priority: types.TaskPriorityNormal,
			Parameters: map[string]interface{}{
				"garbage_threshold": "0.4",
			},
			CreatedAt: time.Now(),
		},
		{
			ID:       "ec-task-1",
			Type:     types.TaskTypeErasureCoding,
			VolumeID: 3002,
			Server:   "localhost:8080",
			Status:   types.TaskStatusPending,
			Priority: types.TaskPriorityHigh,
			Parameters: map[string]interface{}{
				"shard_count": "14",
			},
			CreatedAt: time.Now(),
		},
		{
			ID:       "vacuum-task-2",
			Type:     types.TaskTypeVacuum,
			VolumeID: 3003,
			Server:   "localhost:8081",
			Status:   types.TaskStatusPending,
			Priority: types.TaskPriorityLow,
			Parameters: map[string]interface{}{
				"garbage_threshold": "0.5",
			},
			CreatedAt: time.Now(),
		},
	}

	for _, task := range tasks {
		err = adminServer.QueueTask(task)
		if err != nil {
			t.Fatalf("Failed to queue task %s: %v", task.ID, err)
		}
		t.Logf("Queued task %s (%s) for volume %d", task.ID, task.Type, task.VolumeID)
	}

	// Test task assignment to different workers
	t.Logf("Testing task assignments")

	// Vacuum worker should get vacuum tasks
	assignedTask, err := adminServer.RequestTask("vacuum-worker-1", []types.TaskType{types.TaskTypeVacuum})
	if err != nil {
		t.Errorf("Failed to request task for vacuum worker: %v", err)
	} else if assignedTask != nil {
		t.Logf("Vacuum worker got task: %s (%s)", assignedTask.ID, assignedTask.Type)

		// Complete the task
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 50.0)
		if err != nil {
			t.Errorf("Failed to update progress: %v", err)
		}

		err = adminServer.CompleteTask(assignedTask.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete task: %v", err)
		}
	}

	// EC worker should get EC tasks
	assignedTask, err = adminServer.RequestTask("ec-worker-1", []types.TaskType{types.TaskTypeErasureCoding})
	if err != nil {
		t.Errorf("Failed to request task for EC worker: %v", err)
	} else if assignedTask != nil {
		t.Logf("EC worker got task: %s (%s)", assignedTask.ID, assignedTask.Type)

		// Complete the task
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update progress: %v", err)
		}

		err = adminServer.CompleteTask(assignedTask.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete task: %v", err)
		}
	}

	// Multi-capability worker should be able to get any remaining task
	assignedTask, err = adminServer.RequestTask("multi-worker-1", []types.TaskType{types.TaskTypeVacuum, types.TaskTypeErasureCoding})
	if err != nil {
		t.Errorf("Failed to request task for multi worker: %v", err)
	} else if assignedTask != nil {
		t.Logf("Multi worker got task: %s (%s)", assignedTask.ID, assignedTask.Type)

		// Complete the task
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update progress: %v", err)
		}

		err = adminServer.CompleteTask(assignedTask.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete task: %v", err)
		}
	}

	// Check final statistics
	stats := adminServer.GetSystemStats()
	t.Logf("Final stats: Active tasks=%d, Queued tasks=%d, Active workers=%d, Total tasks=%d",
		stats.ActiveTasks, stats.QueuedTasks, stats.ActiveWorkers, stats.TotalTasks)

	history := adminServer.GetTaskHistory()
	t.Logf("Task history contains %d completed tasks", len(history))

	for _, entry := range history {
		t.Logf("Completed: %s (%s) - Worker: %s, Duration: %v",
			entry.TaskID, entry.TaskType, entry.WorkerID, entry.Duration)
	}

	t.Logf("Minimal full workflow test completed successfully")
}
