package task

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TestSimpleIntegration tests basic admin-worker operational flow without complex dependencies
func TestSimpleIntegration(t *testing.T) {
	t.Logf("Starting simple integration test")

	// Step 1: Create a minimal admin server configuration
	config := &AdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           2 * time.Hour,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    5,
	}

	// Step 2: Create admin server with nil master client (for testing)
	adminServer := NewAdminServer(config, nil)

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

		err = adminServer.UpdateTaskProgress(assignedTask.ID, 50.0)
		if err != nil {
			t.Errorf("Failed to update task progress: %v", err)
		}

		err = adminServer.UpdateTaskProgress(assignedTask.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update task progress: %v", err)
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
		t.Logf("System stats: Active tasks=%d, Queued tasks=%d, Active workers=%d",
			stats.ActiveTasks, stats.QueuedTasks, stats.ActiveWorkers)
	}

	queuedCount := adminServer.GetQueuedTaskCount()
	activeCount := adminServer.GetActiveTaskCount()
	t.Logf("Queue status: %d queued, %d active tasks", queuedCount, activeCount)

	// Step 10: Test task history
	history := adminServer.GetTaskHistory()
	t.Logf("Task history contains %d entries", len(history))

	t.Logf("Simple integration test completed successfully")
}

// TestWorkerHeartbeat tests worker heartbeat functionality
func TestWorkerHeartbeat(t *testing.T) {
	t.Logf("Testing worker heartbeat")

	config := &AdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           2 * time.Hour,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    5,
	}

	adminServer := NewAdminServer(config, nil)
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

	t.Logf("Worker heartbeat test completed successfully")
}

// TestTaskQueueOperations tests task queue operations
func TestTaskQueueOperations(t *testing.T) {
	t.Logf("Testing task queue operations")

	config := &AdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           2 * time.Hour,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    5,
	}

	adminServer := NewAdminServer(config, nil)
	err := adminServer.Start()
	if err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()

	// Test queuing multiple tasks
	for i := 0; i < 3; i++ {
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
	if queuedCount != 3 {
		t.Errorf("Expected 3 queued tasks, got %d", queuedCount)
	}

	t.Logf("Task queue operations test completed successfully")
}
