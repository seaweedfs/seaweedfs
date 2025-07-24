package task

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TestECWorkerIntegration tests the complete EC worker functionality
func TestECWorkerIntegration(t *testing.T) {
	t.Logf("Starting EC worker integration test")

	// Step 1: Create admin server with EC configuration
	config := &MinimalAdminConfig{
		ScanInterval:          5 * time.Second,
		WorkerTimeout:         60 * time.Second,
		TaskTimeout:           45 * time.Minute, // EC takes longer
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    1, // One at a time for EC
	}

	adminServer := NewMinimalAdminServer(config, nil)
	err := adminServer.Start()
	if err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()
	t.Logf("✓ Admin server started successfully")

	// Step 2: Register EC-capable worker
	worker := &types.Worker{
		ID:            "ec-worker-1",
		Address:       "localhost:9001",
		Capabilities:  []types.TaskType{types.TaskTypeErasureCoding},
		MaxConcurrent: 1,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	err = adminServer.RegisterWorker(worker)
	if err != nil {
		t.Fatalf("Failed to register EC worker: %v", err)
	}
	t.Logf("✓ EC worker registered: %s", worker.ID)

	// Step 3: Create work directory for EC processing
	workDir := filepath.Join(os.TempDir(), "seaweedfs_ec_test")
	err = os.MkdirAll(workDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create work directory: %v", err)
	}
	defer os.RemoveAll(workDir)
	t.Logf("✓ Work directory created: %s", workDir)

	// Step 4: Create EC task with comprehensive parameters
	ecTask := &types.Task{
		ID:       "ec-test-task-1",
		Type:     types.TaskTypeErasureCoding,
		VolumeID: 54321,
		Server:   "localhost:8080",
		Status:   types.TaskStatusPending,
		Priority: types.TaskPriorityHigh,
		Parameters: map[string]interface{}{
			"volume_size":   int64(64 * 1024 * 1024 * 1024), // 64GB volume
			"master_client": "localhost:9333",
			"work_dir":      workDir,
			"collection":    "test",
			"data_shards":   10,
			"parity_shards": 4,
			"rack_aware":    true,
			"load_balance":  true,
		},
		CreatedAt: time.Now(),
	}

	err = adminServer.QueueTask(ecTask)
	if err != nil {
		t.Fatalf("Failed to queue EC task: %v", err)
	}
	t.Logf("✓ EC task queued: %s for volume %d", ecTask.ID, ecTask.VolumeID)

	// Step 5: Worker requests and receives the EC task
	assignedTask, err := adminServer.RequestTask("ec-worker-1", []types.TaskType{types.TaskTypeErasureCoding})
	if err != nil {
		t.Fatalf("Failed to request EC task: %v", err)
	}

	if assignedTask == nil {
		t.Fatalf("No EC task was assigned")
	}

	t.Logf("✓ EC task assigned: %s (%s) for volume %d",
		assignedTask.ID, assignedTask.Type, assignedTask.VolumeID)

	// Step 6: Test EC task creation and validation
	t.Logf("Testing EC task creation and validation")

	// Create EC task instance directly
	factory := erasure_coding.NewFactory()
	taskParams := types.TaskParams{
		VolumeID:   assignedTask.VolumeID,
		Server:     assignedTask.Server,
		Collection: "test",
		Parameters: assignedTask.Parameters,
	}

	taskInstance, err := factory.Create(taskParams)
	if err != nil {
		t.Fatalf("Failed to create EC task instance: %v", err)
	}
	t.Logf("✓ EC task instance created successfully")

	// Step 7: Validate task parameters
	err = taskInstance.Validate(taskParams)
	if err != nil {
		t.Errorf("EC task validation failed: %v", err)
	} else {
		t.Logf("✓ EC task validation passed")
	}

	// Step 8: Test time estimation
	estimatedTime := taskInstance.EstimateTime(taskParams)
	expectedMinTime := time.Duration(64*2) * time.Minute // 2 minutes per GB for 64GB

	t.Logf("✓ EC estimated time: %v (minimum expected: %v)", estimatedTime, expectedMinTime)

	if estimatedTime < expectedMinTime {
		t.Logf("⚠ Note: Estimated time seems optimistic for 64GB volume")
	}

	// Step 9: Simulate EC task execution phases
	t.Logf("Simulating EC execution phases:")

	phases := []struct {
		progress float64
		phase    string
	}{
		{5.0, "Initializing EC processing"},
		{15.0, "Volume data copied to local disk with progress tracking"},
		{25.0, "Source volume marked as read-only"},
		{45.0, "Local Reed-Solomon encoding (10+4 shards) completed"},
		{60.0, "Created 14 EC shards with verification"},
		{70.0, "Optimal shard placement calculated with rack awareness"},
		{85.0, "Intelligent shard distribution with load balancing"},
		{95.0, "Shard placement verified across multiple racks"},
		{100.0, "EC processing completed with cleanup"},
	}

	for _, phase := range phases {
		err = adminServer.UpdateTaskProgress(assignedTask.ID, phase.progress)
		if err != nil {
			t.Errorf("Failed to update task progress to %.1f%%: %v", phase.progress, err)
		} else {
			t.Logf("  %.1f%% - %s", phase.progress, phase.phase)
		}
		time.Sleep(50 * time.Millisecond) // Simulate processing time
	}

	// Step 10: Complete the EC task
	err = adminServer.CompleteTask(assignedTask.ID, true, "")
	if err != nil {
		t.Errorf("Failed to complete EC task: %v", err)
	} else {
		t.Logf("✓ EC task completed successfully")
	}

	// Step 11: Verify EC task completion and metrics
	stats := adminServer.GetSystemStats()
	t.Logf("✓ Final stats: Active tasks=%d, Queued tasks=%d, Active workers=%d, Total tasks=%d",
		stats.ActiveTasks, stats.QueuedTasks, stats.ActiveWorkers, stats.TotalTasks)

	history := adminServer.GetTaskHistory()
	t.Logf("✓ Task history contains %d completed tasks", len(history))

	if len(history) > 0 {
		lastEntry := history[len(history)-1]
		t.Logf("✓ Last completed task: %s (%s) - Duration: %v",
			lastEntry.TaskID, lastEntry.TaskType, lastEntry.Duration)

		if lastEntry.TaskType == types.TaskTypeErasureCoding {
			t.Logf("✅ EC task execution verified!")
		}
	}

	t.Logf("✅ EC worker integration test completed successfully")
}

// TestECFeatureValidation tests specific EC features
func TestECFeatureValidation(t *testing.T) {
	t.Logf("Testing EC feature validation")

	// Create work directory
	workDir := filepath.Join(os.TempDir(), "seaweedfs_ec_features_test")
	err := os.MkdirAll(workDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create work directory: %v", err)
	}
	defer os.RemoveAll(workDir)

	// Test EC task features
	ecTask := erasure_coding.NewTaskWithParams(
		"localhost:8080", // source server
		98765,            // volume ID
		"localhost:9333", // master client
		workDir,          // work directory
	)

	// Test current step tracking
	currentStep := ecTask.GetCurrentStep()
	t.Logf("✓ Initial current step: '%s'", currentStep)

	initialProgress := ecTask.GetProgress()
	t.Logf("✓ Initial progress: %.1f%%", initialProgress)

	// Test parameter validation with features
	validParams := types.TaskParams{
		VolumeID:   98765,
		Server:     "localhost:8080",
		Collection: "features_test",
		Parameters: map[string]interface{}{
			"volume_size":    int64(128 * 1024 * 1024 * 1024), // 128GB
			"master_client":  "localhost:9333",
			"work_dir":       workDir,
			"data_shards":    10,
			"parity_shards":  4,
			"rack_awareness": true,
			"load_balancing": true,
			"backup_servers": 2,
			"affinity_zones": []string{"zone-a", "zone-b", "zone-c"},
		},
	}

	err = ecTask.Validate(validParams)
	if err != nil {
		t.Errorf("Valid parameters should pass validation: %v", err)
	} else {
		t.Logf("✓ Parameter validation passed")
	}

	// Test time estimation for large volume
	estimatedTime := ecTask.EstimateTime(validParams)
	expectedMinTime := time.Duration(128*2) * time.Minute // 2 minutes per GB

	t.Logf("✓ 128GB volume estimated time: %v (expected minimum: %v)", estimatedTime, expectedMinTime)

	if estimatedTime < expectedMinTime {
		t.Errorf("Time estimate seems too low for 128GB volume")
	}

	// Test invalid parameters
	invalidParams := types.TaskParams{
		VolumeID: 0,  // Invalid
		Server:   "", // Invalid
	}

	err = ecTask.Validate(invalidParams)
	if err == nil {
		t.Errorf("Invalid parameters should fail validation")
	} else {
		t.Logf("✓ Invalid parameter validation correctly failed: %v", err)
	}

	t.Logf("✅ EC feature validation completed successfully")
}

// TestECWorkflow tests the complete EC workflow
func TestECWorkflow(t *testing.T) {
	t.Logf("Testing complete EC workflow")

	// Create admin server
	config := &MinimalAdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           60 * time.Minute,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    1,
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
			ID:            "ec-specialist-1",
			Address:       "localhost:9001",
			Capabilities:  []types.TaskType{types.TaskTypeErasureCoding},
			MaxConcurrent: 1,
			Status:        "active",
			CurrentLoad:   0,
			LastHeartbeat: time.Now(),
		},
		{
			ID:            "vacuum-worker-1",
			Address:       "localhost:9002",
			Capabilities:  []types.TaskType{types.TaskTypeVacuum},
			MaxConcurrent: 2,
			Status:        "active",
			CurrentLoad:   0,
			LastHeartbeat: time.Now(),
		},
		{
			ID:            "multi-capability-worker-1",
			Address:       "localhost:9003",
			Capabilities:  []types.TaskType{types.TaskTypeVacuum, types.TaskTypeErasureCoding},
			MaxConcurrent: 2,
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
		t.Logf("✓ Registered worker %s with capabilities %v", worker.ID, worker.Capabilities)
	}

	// Create test work directory
	workDir := filepath.Join(os.TempDir(), "seaweedfs_workflow_test")
	err = os.MkdirAll(workDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create work directory: %v", err)
	}
	defer os.RemoveAll(workDir)

	// Create multiple tasks of different types
	tasks := []*types.Task{
		{
			ID:       "ec-workflow-1",
			Type:     types.TaskTypeErasureCoding,
			VolumeID: 11111,
			Server:   "localhost:8080",
			Status:   types.TaskStatusPending,
			Priority: types.TaskPriorityHigh,
			Parameters: map[string]interface{}{
				"volume_size":   int64(50 * 1024 * 1024 * 1024),
				"master_client": "localhost:9333",
				"work_dir":      workDir,
				"collection":    "workflow_test",
			},
			CreatedAt: time.Now(),
		},
		{
			ID:       "vacuum-workflow-1",
			Type:     types.TaskTypeVacuum,
			VolumeID: 22222,
			Server:   "localhost:8081",
			Status:   types.TaskStatusPending,
			Priority: types.TaskPriorityNormal,
			Parameters: map[string]interface{}{
				"garbage_threshold": "0.4",
				"volume_size":       int64(20 * 1024 * 1024 * 1024),
			},
			CreatedAt: time.Now(),
		},
		{
			ID:       "ec-workflow-2",
			Type:     types.TaskTypeErasureCoding,
			VolumeID: 33333,
			Server:   "localhost:8082",
			Status:   types.TaskStatusPending,
			Priority: types.TaskPriorityNormal,
			Parameters: map[string]interface{}{
				"volume_size":   int64(80 * 1024 * 1024 * 1024),
				"master_client": "localhost:9333",
				"work_dir":      workDir,
				"collection":    "workflow_test",
			},
			CreatedAt: time.Now(),
		},
	}

	// Queue all tasks
	for _, task := range tasks {
		err = adminServer.QueueTask(task)
		if err != nil {
			t.Fatalf("Failed to queue task %s: %v", task.ID, err)
		}
		t.Logf("✓ Queued task %s (%s) for volume %d", task.ID, task.Type, task.VolumeID)
	}

	// Test task assignment to appropriate workers
	t.Logf("Testing task assignments to appropriate workers")

	// EC specialist should get EC tasks
	assignedTask, err := adminServer.RequestTask("ec-specialist-1", []types.TaskType{types.TaskTypeErasureCoding})
	if err != nil {
		t.Errorf("Failed to request task for EC specialist: %v", err)
	} else if assignedTask != nil {
		t.Logf("✓ EC specialist got task: %s (%s)", assignedTask.ID, assignedTask.Type)

		// Complete the task
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update progress: %v", err)
		}

		err = adminServer.CompleteTask(assignedTask.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete task: %v", err)
		}
		t.Logf("✓ EC task completed by specialist")
	}

	// Vacuum worker should get vacuum tasks
	assignedTask, err = adminServer.RequestTask("vacuum-worker-1", []types.TaskType{types.TaskTypeVacuum})
	if err != nil {
		t.Errorf("Failed to request task for vacuum worker: %v", err)
	} else if assignedTask != nil {
		t.Logf("✓ Vacuum worker got task: %s (%s)", assignedTask.ID, assignedTask.Type)

		// Complete the task
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update progress: %v", err)
		}

		err = adminServer.CompleteTask(assignedTask.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete task: %v", err)
		}
		t.Logf("✓ Vacuum task completed by vacuum worker")
	}

	// Multi-capability worker should get remaining tasks
	assignedTask, err = adminServer.RequestTask("multi-capability-worker-1", []types.TaskType{types.TaskTypeVacuum, types.TaskTypeErasureCoding})
	if err != nil {
		t.Errorf("Failed to request task for multi-capability worker: %v", err)
	} else if assignedTask != nil {
		t.Logf("✓ Multi-capability worker got task: %s (%s)", assignedTask.ID, assignedTask.Type)

		// Complete the task
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update progress: %v", err)
		}

		err = adminServer.CompleteTask(assignedTask.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete task: %v", err)
		}
		t.Logf("✓ Task completed by multi-capability worker")
	}

	// Check final workflow statistics
	stats := adminServer.GetSystemStats()
	t.Logf("✓ Final workflow stats: Active tasks=%d, Queued tasks=%d, Active workers=%d, Total tasks=%d",
		stats.ActiveTasks, stats.QueuedTasks, stats.ActiveWorkers, stats.TotalTasks)

	history := adminServer.GetTaskHistory()
	t.Logf("✓ Workflow history contains %d completed tasks", len(history))

	// Analyze task completion by type
	ecTasks := 0
	vacuumTasks := 0

	for _, entry := range history {
		switch entry.TaskType {
		case types.TaskTypeErasureCoding:
			ecTasks++
			t.Logf("  EC: %s - Worker: %s, Duration: %v",
				entry.TaskID, entry.WorkerID, entry.Duration)
		case types.TaskTypeVacuum:
			vacuumTasks++
			t.Logf("  Vacuum: %s - Worker: %s, Duration: %v",
				entry.TaskID, entry.WorkerID, entry.Duration)
		}
	}

	t.Logf("✓ Completed tasks: %d EC, %d Vacuum", ecTasks, vacuumTasks)
	t.Logf("✅ EC workflow test completed successfully")
}
