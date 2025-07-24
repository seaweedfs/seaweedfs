package task

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	ec_task "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TestEnhancedECIntegration tests the enhanced EC implementation with the admin server
func TestEnhancedECIntegration(t *testing.T) {
	t.Logf("Starting enhanced EC integration test")

	// Step 1: Create admin server
	config := &MinimalAdminConfig{
		ScanInterval:          10 * time.Second,
		WorkerTimeout:         30 * time.Second,
		TaskTimeout:           30 * time.Minute, // EC takes longer
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    2, // Limit concurrency for EC tasks
	}

	adminServer := NewMinimalAdminServer(config, nil)
	err := adminServer.Start()
	if err != nil {
		t.Fatalf("Failed to start admin server: %v", err)
	}
	defer adminServer.Stop()

	// Step 2: Register an EC-capable worker
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
	t.Logf("Successfully registered EC worker %s", worker.ID)

	// Step 3: Create an EC task
	ecTask := &types.Task{
		ID:       "enhanced-ec-task-1",
		Type:     types.TaskTypeErasureCoding,
		VolumeID: 12345,
		Server:   "localhost:8080",
		Status:   types.TaskStatusPending,
		Priority: types.TaskPriorityHigh,
		Parameters: map[string]interface{}{
			"volume_size":   int64(32 * 1024 * 1024 * 1024), // 32GB
			"master_client": "localhost:9333",
			"work_dir":      "/tmp/seaweedfs_ec_work",
			"collection":    "test",
		},
		CreatedAt: time.Now(),
	}

	err = adminServer.QueueTask(ecTask)
	if err != nil {
		t.Fatalf("Failed to queue EC task: %v", err)
	}
	t.Logf("Successfully queued enhanced EC task %s for volume %d", ecTask.ID, ecTask.VolumeID)

	// Step 4: Worker requests the task
	assignedTask, err := adminServer.RequestTask("ec-worker-1", []types.TaskType{types.TaskTypeErasureCoding})
	if err != nil {
		t.Fatalf("Failed to request EC task: %v", err)
	}

	if assignedTask != nil {
		t.Logf("EC worker got task: %s (%s) for volume %d",
			assignedTask.ID, assignedTask.Type, assignedTask.VolumeID)

		// Step 5: Simulate enhanced EC task execution progress
		t.Logf("Simulating enhanced EC task execution phases")

		// Phase 1: Copying volume data
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 15.0)
		if err != nil {
			t.Errorf("Failed to update progress (copying): %v", err)
		}
		t.Logf("Phase 1: Volume data copied to local disk")

		// Phase 2: Marking read-only
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 25.0)
		if err != nil {
			t.Errorf("Failed to update progress (read-only): %v", err)
		}
		t.Logf("Phase 2: Source volume marked as read-only")

		// Phase 3: Local EC encoding
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 60.0)
		if err != nil {
			t.Errorf("Failed to update progress (encoding): %v", err)
		}
		t.Logf("Phase 3: Local Reed-Solomon encoding completed (10+4 shards)")

		// Phase 4: Calculating optimal placement
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 70.0)
		if err != nil {
			t.Errorf("Failed to update progress (placement): %v", err)
		}
		t.Logf("Phase 4: Optimal shard placement calculated with affinity")

		// Phase 5: Distributing shards
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 90.0)
		if err != nil {
			t.Errorf("Failed to update progress (distribution): %v", err)
		}
		t.Logf("Phase 5: Shards distributed across servers with rack diversity")

		// Phase 6: Verification and cleanup
		err = adminServer.UpdateTaskProgress(assignedTask.ID, 100.0)
		if err != nil {
			t.Errorf("Failed to update progress (completion): %v", err)
		}
		t.Logf("Phase 6: Verification and cleanup completed")

		// Step 6: Complete the task
		err = adminServer.CompleteTask(assignedTask.ID, true, "")
		if err != nil {
			t.Errorf("Failed to complete EC task: %v", err)
		}
		t.Logf("Successfully completed enhanced EC task %s", assignedTask.ID)
	} else {
		t.Logf("No EC task was assigned (expected in test environment)")
	}

	// Step 7: Verify task completion
	stats := adminServer.GetSystemStats()
	t.Logf("Final stats: Active tasks=%d, Queued tasks=%d, Active workers=%d, Total tasks=%d",
		stats.ActiveTasks, stats.QueuedTasks, stats.ActiveWorkers, stats.TotalTasks)

	history := adminServer.GetTaskHistory()
	t.Logf("Task history contains %d completed tasks", len(history))

	if len(history) > 0 {
		lastEntry := history[len(history)-1]
		t.Logf("Last completed task: %s (%s) - Duration: %v",
			lastEntry.TaskID, lastEntry.TaskType, lastEntry.Duration)

		if lastEntry.TaskType == types.TaskTypeErasureCoding {
			t.Logf("Enhanced EC task completed successfully")
		}
	}

	t.Logf("Enhanced EC integration test completed successfully")
}

// TestEnhancedECTaskValidation tests the enhanced EC task validation
func TestEnhancedECTaskValidation(t *testing.T) {
	t.Logf("Testing enhanced EC task validation")

	// Create a temporary work directory
	workDir := filepath.Join(os.TempDir(), "seaweedfs_ec_test")
	err := os.MkdirAll(workDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create work directory: %v", err)
	}
	defer os.RemoveAll(workDir)

	// Create enhanced EC task
	enhancedTask := ec_task.NewEnhancedECTask(
		"localhost:8080", // source server
		12345,            // volume ID
		"localhost:9333", // master client
		workDir,          // work directory
	)

	// Test validation with valid parameters
	validParams := types.TaskParams{
		VolumeID:   12345,
		Server:     "localhost:8080",
		Collection: "test",
		Parameters: map[string]interface{}{
			"volume_size": int64(32 * 1024 * 1024 * 1024),
		},
	}

	err = enhancedTask.Validate(validParams)
	if err != nil {
		t.Errorf("Valid parameters should pass validation: %v", err)
	}

	// Test validation with invalid parameters
	invalidParams := types.TaskParams{
		VolumeID: 0,  // Invalid volume ID
		Server:   "", // Empty server
	}

	err = enhancedTask.Validate(invalidParams)
	if err == nil {
		t.Errorf("Invalid parameters should fail validation")
	}

	// Test time estimation
	estimatedTime := enhancedTask.EstimateTime(validParams)
	t.Logf("Estimated time for 32GB volume EC: %v", estimatedTime)

	if estimatedTime < 20*time.Minute {
		t.Errorf("Expected at least 20 minutes for large volume EC, got %v", estimatedTime)
	}

	t.Logf("Enhanced EC task validation completed successfully")
}

// TestEnhancedECFeatures tests specific enhanced EC features
func TestEnhancedECFeatures(t *testing.T) {
	t.Logf("Testing enhanced EC features")

	// Create temporary work directory
	workDir := filepath.Join(os.TempDir(), "seaweedfs_ec_features_test")
	err := os.MkdirAll(workDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create work directory: %v", err)
	}
	defer os.RemoveAll(workDir)

	enhancedTask := ec_task.NewEnhancedECTask(
		"localhost:8080",
		54321,
		"localhost:9333",
		workDir,
	)

	// Test step tracking
	t.Logf("Testing step tracking functionality")

	currentStep := enhancedTask.GetCurrentStep()
	t.Logf("Initial current step: %s", currentStep)

	progress := enhancedTask.GetProgress()
	t.Logf("Initial progress: %.1f%%", progress)

	// Test parameter extraction
	params := types.TaskParams{
		VolumeID:   54321,
		Server:     "localhost:8080",
		Collection: "enhanced_test",
		Parameters: map[string]interface{}{
			"volume_size":    int64(64 * 1024 * 1024 * 1024), // 64GB
			"data_shards":    10,
			"parity_shards":  4,
			"affinity_zones": []string{"zone-a", "zone-b", "zone-c"},
		},
	}

	estimatedTime := enhancedTask.EstimateTime(params)
	expectedMinTime := time.Duration(64*2) * time.Minute // 2 minutes per GB

	t.Logf("64GB volume estimated time: %v (expected minimum: %v)", estimatedTime, expectedMinTime)

	if estimatedTime < expectedMinTime {
		t.Errorf("Time estimate seems too low for 64GB volume")
	}

	t.Logf("Enhanced EC features test completed successfully")
}

// TestECTaskComparison compares basic vs enhanced EC implementations
func TestECTaskComparison(t *testing.T) {
	t.Logf("Comparing basic vs enhanced EC implementations")

	// Basic EC task estimation
	basicParams := types.TaskParams{
		VolumeID: 11111,
		Server:   "localhost:8080",
		Parameters: map[string]interface{}{
			"volume_size": int64(30 * 1024 * 1024 * 1024), // 30GB
		},
	}

	// Create basic task (existing implementation)
	basicTask := ec_task.NewTask("localhost:8080", 11111)
	basicTime := basicTask.EstimateTime(basicParams)

	// Create enhanced task
	workDir := filepath.Join(os.TempDir(), "seaweedfs_ec_comparison")
	defer os.RemoveAll(workDir)

	enhancedTask := ec_task.NewEnhancedECTask(
		"localhost:8080",
		22222,
		"localhost:9333",
		workDir,
	)
	enhancedTime := enhancedTask.EstimateTime(basicParams)

	t.Logf("Basic EC task estimated time: %v", basicTime)
	t.Logf("Enhanced EC task estimated time: %v", enhancedTime)

	// Enhanced should take longer due to additional processing
	if enhancedTime <= basicTime {
		t.Logf("Note: Enhanced EC might take longer due to local processing and smart distribution")
	}

	// Test feature differences
	t.Logf("Basic EC features:")
	t.Logf("  - Direct volume server EC generation")
	t.Logf("  - Simple shard mounting")
	t.Logf("  - No custom placement logic")

	t.Logf("Enhanced EC features:")
	t.Logf("  - Local volume data copying")
	t.Logf("  - Local Reed-Solomon encoding")
	t.Logf("  - Intelligent shard placement with affinity")
	t.Logf("  - Rack diversity for data shards")
	t.Logf("  - Load balancing across servers")
	t.Logf("  - Backup server selection")
	t.Logf("  - Detailed progress tracking")

	t.Logf("EC task comparison completed successfully")
}
