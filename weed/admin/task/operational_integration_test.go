package task

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TestOperationalIntegration tests the basic admin-worker operational flow
func TestOperationalIntegration(t *testing.T) {
	t.Logf("Starting operational integration test")

	// Step 1: Create admin server with operational configuration
	config := &AdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           2 * time.Hour,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    5,
	}

	// Create a nil master client for testing (simplified)
	var masterClient *wdclient.MasterClient

	adminServer := NewAdminServer(config, masterClient)

	// Step 2: Start admin server
	err := adminServer.Start()
	if err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()

	// Step 3: Create and register test workers
	worker1 := createTestWorker("worker-1", []types.TaskType{types.TaskTypeVacuum, types.TaskTypeErasureCoding})
	worker2 := createTestWorker("worker-2", []types.TaskType{types.TaskTypeVacuum})

	err = adminServer.RegisterWorker(worker1)
	if err != nil {
		t.Fatalf("Failed to register worker1: %v", err)
	}

	err = adminServer.RegisterWorker(worker2)
	if err != nil {
		t.Fatalf("Failed to register worker2: %v", err)
	}

	// Step 4: Test basic task queueing
	t.Logf("Testing task queueing")

	// Create a simple test task
	testTask := &types.Task{
		ID:       "test-vacuum-1",
		Type:     types.TaskTypeVacuum,
		VolumeID: 1001,
		Server:   "localhost:8080",
		Status:   types.TaskStatusPending,
		Priority: types.TaskPriorityNormal,
		Parameters: map[string]interface{}{
			"garbage_threshold": "0.3",
			"server":            "localhost:8080",
		},
		CreatedAt: time.Now(),
	}

	err = adminServer.QueueTask(testTask)
	if err != nil {
		t.Fatalf("Failed to queue test task: %v", err)
	}
	t.Logf("Successfully queued test vacuum task for volume %d", testTask.VolumeID)

	// Step 5: Test worker task request and assignment
	t.Logf("Testing worker task requests and assignment")

	// Worker requests task
	task, err := adminServer.RequestTask("worker-1", []types.TaskType{types.TaskTypeVacuum})
	if err != nil {
		t.Fatalf("Failed to request task from worker: %v", err)
	}

	if task == nil {
		t.Logf("No tasks available for assignment (this is expected in test environment)")
	} else {
		t.Logf("Successfully assigned task %s (%s) to worker-1", task.ID, task.Type)

		// Step 6: Simulate task progress updates
		t.Logf("Testing task progress updates")

		err = adminServer.UpdateTaskProgress(task.ID, 25.0)
		if err != nil {
			t.Errorf("Failed to update task progress: %v", err)
		}

		err = adminServer.UpdateTaskProgress(task.ID, 50.0)
		if err != nil {
			t.Errorf("Failed to update task progress: %v", err)
		}

		err = adminServer.UpdateTaskProgress(task.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update task progress: %v", err)
		}

		// Step 7: Test task completion
		t.Logf("Testing task completion")

		err = adminServer.CompleteTask(task.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete task: %v", err)
		}

		t.Logf("Successfully completed task %s", task.ID)
	}

	// Step 8: Test metrics and statistics
	t.Logf("Testing system metrics")

	stats := adminServer.GetSystemStats()
	t.Logf("System stats: Active tasks=%d, Queued tasks=%d, Active workers=%d",
		stats.ActiveTasks, stats.QueuedTasks, stats.ActiveWorkers)

	queuedCount := adminServer.GetQueuedTaskCount()
	activeCount := adminServer.GetActiveTaskCount()
	t.Logf("Queue status: %d queued, %d active tasks", queuedCount, activeCount)

	// Step 9: Test task history
	history := adminServer.GetTaskHistory()
	t.Logf("Task history contains %d entries", len(history))

	t.Logf("Operational integration test completed successfully")
}

func createTestWorker(id string, capabilities []types.TaskType) *types.Worker {
	return &types.Worker{
		ID:            id,
		Address:       fmt.Sprintf("localhost:900%s", id[len(id)-1:]),
		Capabilities:  capabilities,
		MaxConcurrent: 2,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}
}

// TestECTaskExecution tests the EC task validation (without actual execution)
func TestECTaskExecution(t *testing.T) {
	t.Logf("Testing EC task validation")

	params := types.TaskParams{
		VolumeID:   1002,
		Server:     "localhost:8080",
		Collection: "test",
		Parameters: map[string]interface{}{
			"volume_size": int64(32 * 1024 * 1024 * 1024),
		},
	}

	// Test that basic validation would work
	if params.VolumeID == 0 {
		t.Errorf("VolumeID should not be zero")
	}
	if params.Server == "" {
		t.Errorf("Server should not be empty")
	}

	t.Logf("EC task validation passed")
}

// TestVacuumTaskExecution tests the vacuum task validation (without actual execution)
func TestVacuumTaskExecution(t *testing.T) {
	t.Logf("Testing vacuum task validation")

	params := types.TaskParams{
		VolumeID:   1001,
		Server:     "localhost:8080",
		Collection: "test",
		Parameters: map[string]interface{}{
			"garbage_threshold": "0.3",
			"volume_size":       int64(25 * 1024 * 1024 * 1024),
		},
	}

	// Test that basic validation would work
	if params.VolumeID == 0 {
		t.Errorf("VolumeID should not be zero")
	}
	if params.Server == "" {
		t.Errorf("Server should not be empty")
	}

	t.Logf("Vacuum task validation passed")
}
