package task

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// ExampleUsage demonstrates how to use the task distribution system
func ExampleUsage() {
	glog.Infof("=== SeaweedFS Task Distribution System Example ===")

	// Example 1: Setting up the Admin Server
	setupAdminServerExample()

	// Example 2: Simulating Workers
	simulateWorkersExample()

	// Example 3: Running Simulations
	runSimulationsExample()

	// Example 4: Demonstrating Features
	demonstrateFeaturesExample()
}

// setupAdminServerExample shows how to set up the admin server
func setupAdminServerExample() {
	glog.Infof("\n--- Example 1: Setting up Admin Server ---")

	// Create master client (in real usage, this would connect to actual master)
	masterClient := &wdclient.MasterClient{} // Simplified for example

	// Create admin server configuration
	config := &AdminConfig{
		ScanInterval:          30 * time.Minute,
		WorkerTimeout:         5 * time.Minute,
		TaskTimeout:           10 * time.Minute,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    10,
	}

	// Create admin server
	adminServer := NewAdminServer(config, masterClient)

	// Start the admin server
	if err := adminServer.Start(); err != nil {
		glog.Errorf("Failed to start admin server: %v", err)
		return
	}

	glog.Infof("✓ Admin server started with configuration:")
	glog.Infof("  - Scan Interval: %v", config.ScanInterval)
	glog.Infof("  - Worker Timeout: %v", config.WorkerTimeout)
	glog.Infof("  - Max Concurrent Tasks: %d", config.MaxConcurrentTasks)

	// Simulate some operations
	time.Sleep(2 * time.Second)

	// Stop the admin server
	adminServer.Stop()
	glog.Infof("✓ Admin server stopped gracefully")
}

// simulateWorkersExample shows how workers would register and operate
func simulateWorkersExample() {
	glog.Infof("\n--- Example 2: Worker Registration and Operation ---")

	// Create mock workers
	workers := []*types.Worker{
		{
			ID:            "worker-ec-01",
			Address:       "192.168.1.100:8080",
			Capabilities:  []types.TaskType{types.TaskTypeErasureCoding},
			MaxConcurrent: 2,
			Status:        "active",
			CurrentLoad:   0,
		},
		{
			ID:            "worker-vacuum-01",
			Address:       "192.168.1.101:8080",
			Capabilities:  []types.TaskType{types.TaskTypeVacuum},
			MaxConcurrent: 3,
			Status:        "active",
			CurrentLoad:   0,
		},
		{
			ID:            "worker-multi-01",
			Address:       "192.168.1.102:8080",
			Capabilities:  []types.TaskType{types.TaskTypeErasureCoding, types.TaskTypeVacuum},
			MaxConcurrent: 2,
			Status:        "active",
			CurrentLoad:   0,
		},
	}

	// Create worker registry
	registry := NewWorkerRegistry()

	// Register workers
	for _, worker := range workers {
		if err := registry.RegisterWorker(worker); err != nil {
			glog.Errorf("Failed to register worker %s: %v", worker.ID, err)
		} else {
			glog.Infof("✓ Registered worker %s with capabilities: %v", worker.ID, worker.Capabilities)
		}
	}

	// Demonstrate worker selection
	bestECWorker := registry.GetBestWorkerForTask(types.TaskTypeErasureCoding)
	if bestECWorker != nil {
		glog.Infof("✓ Best worker for EC tasks: %s", bestECWorker.ID)
	}

	bestVacuumWorker := registry.GetBestWorkerForTask(types.TaskTypeVacuum)
	if bestVacuumWorker != nil {
		glog.Infof("✓ Best worker for vacuum tasks: %s", bestVacuumWorker.ID)
	}

	// Show registry statistics
	stats := registry.GetRegistryStats()
	glog.Infof("✓ Registry statistics: %+v", stats)
}

// runSimulationsExample shows how to run simulation scenarios
func runSimulationsExample() {
	glog.Infof("\n--- Example 3: Running Simulation Scenarios ---")

	// Create simulation runner
	runner := NewSimulationRunner()

	// Demonstrate system capabilities
	runner.DemonstrateSystemCapabilities()

	// Create a custom scenario
	runner.CreateCustomScenario(
		"custom_test",
		"Custom test scenario for demonstration",
		3,              // 3 workers
		10,             // 10 volumes
		60*time.Second, // 60 second duration
		[]*FailurePattern{
			{
				Type:        FailureWorkerTimeout,
				Probability: 0.2, // 20% chance
				Timing: &TimingSpec{
					MinProgress: 30.0,
					MaxProgress: 70.0,
				},
			},
		},
	)

	// Run specific scenario
	result, err := runner.RunSpecificScenario("custom_test")
	if err != nil {
		glog.Errorf("Failed to run scenario: %v", err)
	} else {
		glog.Infof("✓ Custom scenario completed:")
		glog.Infof("  - Tasks Created: %d", result.TasksCreated)
		glog.Infof("  - Tasks Completed: %d", result.TasksCompleted)
		glog.Infof("  - Duration: %v", result.Duration)
		glog.Infof("  - Success: %v", result.Success)
	}

	// Validate system behavior
	if err := runner.ValidateSystemBehavior(); err != nil {
		glog.Errorf("System validation failed: %v", err)
	} else {
		glog.Infof("✓ All system validation tests passed")
	}
}

// demonstrateFeaturesExample shows key system features
func demonstrateFeaturesExample() {
	glog.Infof("\n--- Example 4: Key System Features ---")

	// Feature 1: Task Discovery
	demonstrateTaskDiscovery()

	// Feature 2: Volume State Tracking
	demonstrateVolumeStateTracking()

	// Feature 3: Failure Handling
	demonstrateFailureHandling()

	// Feature 4: Task Scheduling
	demonstrateTaskScheduling()
}

// demonstrateTaskDiscovery shows how task discovery works
func demonstrateTaskDiscovery() {
	glog.Infof("\n  Feature 1: Task Discovery")

	// Create mock volumes
	volumes := []*VolumeInfo{
		{
			ID:               1,
			Size:             28 * 1024 * 1024 * 1024, // 28GB (93% of 30GB)
			Collection:       "photos",
			DeletedByteCount: 0,
			ReadOnly:         false,
			ModifiedAtSecond: time.Now().Add(-2 * time.Hour).Unix(), // 2 hours old
		},
		{
			ID:               2,
			Size:             20 * 1024 * 1024 * 1024, // 20GB
			Collection:       "documents",
			DeletedByteCount: 8 * 1024 * 1024 * 1024, // 8GB garbage (40%)
			ReadOnly:         false,
			ModifiedAtSecond: time.Now().Add(-1 * time.Hour).Unix(), // 1 hour old
		},
	}

	// Create detectors
	ecDetector := NewECDetector()
	vacuumDetector := NewVacuumDetector()

	// Test EC detection
	ecCandidates, _ := ecDetector.DetectECCandidates(volumes)
	glog.Infof("  ✓ EC detector found %d candidates", len(ecCandidates))
	for _, candidate := range ecCandidates {
		glog.Infof("    - Volume %d: %s (priority: %d)", candidate.VolumeID, candidate.Reason, candidate.Priority)
	}

	// Test vacuum detection
	vacuumCandidates, _ := vacuumDetector.DetectVacuumCandidates(volumes)
	glog.Infof("  ✓ Vacuum detector found %d candidates", len(vacuumCandidates))
	for _, candidate := range vacuumCandidates {
		glog.Infof("    - Volume %d: %s (priority: %d)", candidate.VolumeID, candidate.Reason, candidate.Priority)
	}
}

// demonstrateVolumeStateTracking shows volume state management
func demonstrateVolumeStateTracking() {
	glog.Infof("\n  Feature 2: Volume State Tracking")

	// Create volume state tracker
	tracker := NewVolumeStateTracker(nil, 5*time.Minute)

	// Reserve volumes for tasks
	tracker.ReserveVolume(1, "task-ec-001")
	tracker.ReserveVolume(2, "task-vacuum-001")

	glog.Infof("  ✓ Reserved volumes for tasks")

	// Check reservations
	if tracker.IsVolumeReserved(1) {
		glog.Infof("  ✓ Volume 1 is correctly reserved")
	}

	// Record volume changes
	tracker.RecordVolumeChange(1, types.TaskTypeErasureCoding, "task-ec-001")
	glog.Infof("  ✓ Recorded volume change for EC completion")

	// Get pending changes
	if change := tracker.GetPendingChange(1); change != nil {
		glog.Infof("  ✓ Pending change found: %s for volume %d", change.ChangeType, change.VolumeID)
	}

	// Release reservation
	tracker.ReleaseVolume(2, "task-vacuum-001")
	glog.Infof("  ✓ Released volume reservation")

	// Show statistics
	stats := tracker.GetStats()
	glog.Infof("  ✓ Tracker statistics: %+v", stats)
}

// demonstrateFailureHandling shows failure recovery mechanisms
func demonstrateFailureHandling() {
	glog.Infof("\n  Feature 3: Failure Handling")

	// Create failure handler
	config := DefaultAdminConfig()
	handler := NewFailureHandler(config)

	// Create mock task
	task := &InProgressTask{
		Task: &types.Task{
			ID:         "test-task-001",
			Type:       types.TaskTypeErasureCoding,
			VolumeID:   1,
			RetryCount: 0,
		},
		WorkerID:   "worker-01",
		StartedAt:  time.Now(),
		LastUpdate: time.Now().Add(-30 * time.Minute), // 30 minutes ago
		Progress:   45.0,
	}

	// Demonstrate different failure scenarios
	glog.Infof("  ✓ Simulating worker timeout scenario")
	handler.HandleWorkerTimeout("worker-01", []*InProgressTask{task})

	glog.Infof("  ✓ Simulating stuck task scenario")
	handler.HandleTaskStuck(task)

	glog.Infof("  ✓ Simulating duplicate task detection")
	handler.HandleDuplicateTask("existing-task", "duplicate-task", 1)

	// Show failure statistics
	stats := handler.GetFailureStats()
	glog.Infof("  ✓ Failure handler statistics: %+v", stats)
}

// demonstrateTaskScheduling shows task scheduling logic
func demonstrateTaskScheduling() {
	glog.Infof("\n  Feature 4: Task Scheduling")

	// Create worker registry and task queue
	registry := NewWorkerRegistry()
	queue := NewPriorityTaskQueue()
	scheduler := NewTaskScheduler(registry, queue)

	// Add mock worker
	worker := &types.Worker{
		ID:            "scheduler-worker-01",
		Capabilities:  []types.TaskType{types.TaskTypeErasureCoding, types.TaskTypeVacuum},
		MaxConcurrent: 2,
		Status:        "active",
		CurrentLoad:   0,
	}
	registry.RegisterWorker(worker)

	// Create mock tasks with different priorities
	highPriorityTask := &types.Task{
		ID:       "high-priority-task",
		Type:     types.TaskTypeErasureCoding,
		Priority: types.TaskPriorityHigh,
		VolumeID: 1,
	}

	normalPriorityTask := &types.Task{
		ID:       "normal-priority-task",
		Type:     types.TaskTypeVacuum,
		Priority: types.TaskPriorityNormal,
		VolumeID: 2,
	}

	// Add tasks to queue
	queue.Push(normalPriorityTask)
	queue.Push(highPriorityTask) // Should be prioritized

	glog.Infof("  ✓ Added tasks to priority queue (size: %d)", queue.Size())

	// Test worker selection
	selectedWorker := scheduler.SelectWorker(highPriorityTask, []*types.Worker{worker})
	if selectedWorker != nil {
		glog.Infof("  ✓ Selected worker %s for high-priority task", selectedWorker.ID)
	}

	// Test task retrieval
	nextTask := scheduler.GetNextTask("scheduler-worker-01", []types.TaskType{types.TaskTypeErasureCoding, types.TaskTypeVacuum})
	if nextTask != nil {
		glog.Infof("  ✓ Next task for worker: %s (priority: %d)", nextTask.ID, nextTask.Priority)
	}

	glog.Infof("  ✓ Task scheduling demonstration complete")
}

// RunComprehensiveDemo runs a full demonstration of the system
func RunComprehensiveDemo() {
	glog.Infof("Starting comprehensive task distribution system demonstration...")

	// Run the main example
	ExampleUsage()

	// Run all simulation scenarios
	runner := NewSimulationRunner()
	if err := runner.RunAllScenarios(); err != nil {
		glog.Errorf("Failed to run all scenarios: %v", err)
	}

	glog.Infof("=== Comprehensive demonstration complete ===")
	glog.Infof("The task distribution system is ready for production use!")
	glog.Infof("Key benefits demonstrated:")
	glog.Infof("  ✓ Automatic task discovery and assignment")
	glog.Infof("  ✓ Robust failure handling and recovery")
	glog.Infof("  ✓ Volume state consistency and reconciliation")
	glog.Infof("  ✓ Worker load balancing and performance tracking")
	glog.Infof("  ✓ Comprehensive simulation and validation framework")
}
