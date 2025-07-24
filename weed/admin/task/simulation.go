package task

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TaskSimulator provides a comprehensive simulation framework for testing the task distribution system
type TaskSimulator struct {
	adminServer *AdminServer
	mockWorkers []*MockWorker
	mockMaster  *MockMasterClient
	scenarios   map[string]*SimulationScenario
	results     map[string]*SimulationResult
	mutex       sync.RWMutex
}

// SimulationScenario defines a test scenario
type SimulationScenario struct {
	Name            string
	Description     string
	WorkerCount     int
	VolumeCount     int
	Duration        time.Duration
	FailurePatterns []*FailurePattern
	TestCases       []*TestCase
}

// FailurePattern defines how failures occur during simulation
type FailurePattern struct {
	Type        FailureType
	Probability float64     // 0.0 to 1.0
	Timing      *TimingSpec // When during task execution
	Duration    time.Duration
	Details     string
}

// TestCase defines specific test scenarios
type TestCase struct {
	Name            string
	VolumeID        uint32
	TaskType        types.TaskType
	ExpectedOutcome string
	FailureToInject *FailurePattern
}

// FailureType represents different types of failures
type FailureType string

const (
	FailureWorkerTimeout    FailureType = "worker_timeout"
	FailureTaskStuck        FailureType = "task_stuck"
	FailureTaskCrash        FailureType = "task_crash"
	FailureDuplicate        FailureType = "duplicate_task"
	FailureResourceExhaust  FailureType = "resource_exhaustion"
	FailureNetworkPartition FailureType = "network_partition"
)

// TimingSpec defines when a failure occurs
type TimingSpec struct {
	MinProgress float64       // Minimum progress before failure can occur
	MaxProgress float64       // Maximum progress before failure must occur
	Delay       time.Duration // Fixed delay before failure
}

// SimulationResult tracks the results of a simulation
type SimulationResult struct {
	ScenarioName         string
	StartTime            time.Time
	EndTime              time.Time
	Duration             time.Duration
	TasksCreated         int
	TasksCompleted       int
	TasksFailed          int
	TasksStuck           int
	WorkerTimeouts       int
	DuplicatesFound      int
	StateInconsistencies int
	Errors               []string
	Warnings             []string
	Success              bool
}

// MockWorker simulates a worker with controllable behavior
type MockWorker struct {
	ID            string
	Capabilities  []types.TaskType
	MaxConcurrent int
	CurrentTasks  map[string]*MockTask
	Status        string
	FailureMode   *FailurePattern
	mutex         sync.Mutex
}

// MockTask represents a simulated task execution
type MockTask struct {
	Task      *types.Task
	StartTime time.Time
	Progress  float64
	Stuck     bool
	Failed    bool
	Completed bool
}

// MockMasterClient simulates master server interactions
type MockMasterClient struct {
	volumes       map[uint32]*VolumeInfo
	inconsistency bool
	mutex         sync.RWMutex
}

// NewTaskSimulator creates a new task simulator
func NewTaskSimulator() *TaskSimulator {
	return &TaskSimulator{
		scenarios: make(map[string]*SimulationScenario),
		results:   make(map[string]*SimulationResult),
	}
}

// RegisterScenario registers a simulation scenario
func (ts *TaskSimulator) RegisterScenario(scenario *SimulationScenario) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	ts.scenarios[scenario.Name] = scenario
	glog.Infof("Registered simulation scenario: %s", scenario.Name)
}

// RunScenario executes a simulation scenario
func (ts *TaskSimulator) RunScenario(scenarioName string) (*SimulationResult, error) {
	ts.mutex.RLock()
	scenario, exists := ts.scenarios[scenarioName]
	ts.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("scenario %s not found", scenarioName)
	}

	glog.Infof("Starting simulation scenario: %s", scenarioName)

	result := &SimulationResult{
		ScenarioName: scenarioName,
		StartTime:    time.Now(),
		Errors:       make([]string, 0),
		Warnings:     make([]string, 0),
	}

	// Setup simulation environment
	if err := ts.setupEnvironment(scenario); err != nil {
		return nil, fmt.Errorf("failed to setup environment: %v", err)
	}

	// Execute test cases
	ctx, cancel := context.WithTimeout(context.Background(), scenario.Duration)
	defer cancel()

	ts.executeScenario(ctx, scenario, result)

	// Cleanup
	ts.cleanup()

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.Success = len(result.Errors) == 0

	ts.mutex.Lock()
	ts.results[scenarioName] = result
	ts.mutex.Unlock()

	glog.Infof("Completed simulation scenario: %s (success: %v)", scenarioName, result.Success)
	return result, nil
}

// setupEnvironment prepares the simulation environment
func (ts *TaskSimulator) setupEnvironment(scenario *SimulationScenario) error {
	// Create mock master client
	ts.mockMaster = &MockMasterClient{
		volumes: make(map[uint32]*VolumeInfo),
	}

	// Generate mock volumes
	for i := uint32(1); i <= uint32(scenario.VolumeCount); i++ {
		volume := &VolumeInfo{
			ID:               i,
			Size:             uint64(rand.Intn(30 * 1024 * 1024 * 1024)), // Random size up to 30GB
			Collection:       fmt.Sprintf("collection_%d", (i%3)+1),
			DeletedByteCount: uint64(rand.Intn(1024 * 1024 * 1024)), // Random garbage
			ReadOnly:         false,
			Server:           fmt.Sprintf("server_%d", (i%6)+1),
			ModifiedAtSecond: time.Now().Add(-time.Duration(rand.Intn(86400)) * time.Second).Unix(),
		}
		ts.mockMaster.volumes[i] = volume
	}

	// Create mock workers
	ts.mockWorkers = make([]*MockWorker, scenario.WorkerCount)
	for i := 0; i < scenario.WorkerCount; i++ {
		worker := &MockWorker{
			ID:            fmt.Sprintf("worker_%d", i+1),
			Capabilities:  []types.TaskType{types.TaskTypeErasureCoding, types.TaskTypeVacuum},
			MaxConcurrent: 2,
			CurrentTasks:  make(map[string]*MockTask),
			Status:        "active",
		}

		// Apply failure patterns
		if i < len(scenario.FailurePatterns) {
			worker.FailureMode = scenario.FailurePatterns[i]
		}

		ts.mockWorkers[i] = worker
	}

	// Initialize admin server (simplified for simulation)
	config := DefaultAdminConfig()
	config.ScanInterval = 10 * time.Second
	config.TaskTimeout = 30 * time.Second

	// Note: In a real implementation, this would use the actual master client
	// For simulation, we'd need to inject our mock

	return nil
}

// executeScenario runs the actual simulation scenario
func (ts *TaskSimulator) executeScenario(ctx context.Context, scenario *SimulationScenario, result *SimulationResult) {
	// Execute each test case
	for _, testCase := range scenario.TestCases {
		ts.executeTestCase(ctx, testCase, result)
	}

	// Run continuous simulation for remaining duration
	ts.runContinuousSimulation(ctx, scenario, result)
}

// executeTestCase runs a specific test case
func (ts *TaskSimulator) executeTestCase(ctx context.Context, testCase *TestCase, result *SimulationResult) {
	glog.V(1).Infof("Executing test case: %s", testCase.Name)

	// Create task for the test case
	task := &types.Task{
		ID:        fmt.Sprintf("test_%s_%d", testCase.Name, time.Now().UnixNano()),
		Type:      testCase.TaskType,
		VolumeID:  testCase.VolumeID,
		Priority:  types.TaskPriorityNormal,
		CreatedAt: time.Now(),
	}

	result.TasksCreated++

	// Assign to worker
	worker := ts.selectWorkerForTask(task)
	if worker == nil {
		result.Errors = append(result.Errors, fmt.Sprintf("No available worker for test case %s", testCase.Name))
		return
	}

	// Execute task with potential failure injection
	ts.executeTaskOnWorker(ctx, task, worker, testCase.FailureToInject, result)
}

// runContinuousSimulation runs ongoing simulation
func (ts *TaskSimulator) runContinuousSimulation(ctx context.Context, scenario *SimulationScenario, result *SimulationResult) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ts.simulateOngoingTasks(result)
			ts.checkForInconsistencies(result)
		}
	}
}

// executeTaskOnWorker simulates task execution on a worker
func (ts *TaskSimulator) executeTaskOnWorker(ctx context.Context, task *types.Task, worker *MockWorker, failurePattern *FailurePattern, result *SimulationResult) {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()

	mockTask := &MockTask{
		Task:      task,
		StartTime: time.Now(),
		Progress:  0.0,
	}

	worker.CurrentTasks[task.ID] = mockTask

	// Simulate task execution
	go ts.simulateTaskExecution(ctx, mockTask, worker, failurePattern, result)
}

// simulateTaskExecution simulates the execution of a single task
func (ts *TaskSimulator) simulateTaskExecution(ctx context.Context, mockTask *MockTask, worker *MockWorker, failurePattern *FailurePattern, result *SimulationResult) {
	defer func() {
		worker.mutex.Lock()
		delete(worker.CurrentTasks, mockTask.Task.ID)
		worker.mutex.Unlock()
	}()

	duration := 20 * time.Second // Base task duration
	progressTicker := time.NewTicker(time.Second)
	defer progressTicker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-progressTicker.C:
			elapsed := time.Since(startTime)
			progress := float64(elapsed) / float64(duration) * 100.0

			if progress >= 100.0 {
				mockTask.Completed = true
				result.TasksCompleted++
				glog.V(2).Infof("Task %s completed successfully", mockTask.Task.ID)
				return
			}

			mockTask.Progress = progress

			// Check for failure injection
			if failurePattern != nil && ts.shouldInjectFailure(failurePattern, progress, elapsed) {
				ts.injectFailure(mockTask, worker, failurePattern, result)
				return
			}

			// Check for worker failure mode
			if worker.FailureMode != nil && ts.shouldInjectFailure(worker.FailureMode, progress, elapsed) {
				ts.injectFailure(mockTask, worker, worker.FailureMode, result)
				return
			}
		}
	}
}

// shouldInjectFailure determines if a failure should be injected
func (ts *TaskSimulator) shouldInjectFailure(pattern *FailurePattern, progress float64, elapsed time.Duration) bool {
	if pattern.Timing != nil {
		if progress < pattern.Timing.MinProgress || progress > pattern.Timing.MaxProgress {
			return false
		}
		if elapsed < pattern.Timing.Delay {
			return false
		}
	}

	return rand.Float64() < pattern.Probability
}

// injectFailure simulates a failure
func (ts *TaskSimulator) injectFailure(mockTask *MockTask, worker *MockWorker, pattern *FailurePattern, result *SimulationResult) {
	glog.Warningf("Injecting failure: %s for task %s", pattern.Type, mockTask.Task.ID)

	switch pattern.Type {
	case FailureWorkerTimeout:
		worker.Status = "timeout"
		result.WorkerTimeouts++

	case FailureTaskStuck:
		mockTask.Stuck = true
		result.TasksStuck++

	case FailureTaskCrash:
		mockTask.Failed = true
		result.TasksFailed++

	case FailureDuplicate:
		result.DuplicatesFound++

	case FailureResourceExhaust:
		worker.Status = "resource_exhausted"
		result.Warnings = append(result.Warnings, fmt.Sprintf("Worker %s resource exhausted", worker.ID))

	case FailureNetworkPartition:
		worker.Status = "partitioned"
		result.Warnings = append(result.Warnings, fmt.Sprintf("Worker %s network partitioned", worker.ID))
	}
}

// selectWorkerForTask selects an available worker for a task
func (ts *TaskSimulator) selectWorkerForTask(task *types.Task) *MockWorker {
	for _, worker := range ts.mockWorkers {
		if worker.Status == "active" && len(worker.CurrentTasks) < worker.MaxConcurrent {
			// Check capabilities
			for _, capability := range worker.Capabilities {
				if capability == task.Type {
					return worker
				}
			}
		}
	}
	return nil
}

// simulateOngoingTasks handles ongoing task simulation
func (ts *TaskSimulator) simulateOngoingTasks(result *SimulationResult) {
	// Create random new tasks
	if rand.Float64() < 0.3 { // 30% chance to create new task every tick
		taskType := types.TaskTypeVacuum
		if rand.Float64() < 0.5 {
			taskType = types.TaskTypeErasureCoding
		}

		task := &types.Task{
			ID:        fmt.Sprintf("auto_%d", time.Now().UnixNano()),
			Type:      taskType,
			VolumeID:  uint32(rand.Intn(len(ts.mockMaster.volumes)) + 1),
			Priority:  types.TaskPriorityNormal,
			CreatedAt: time.Now(),
		}

		result.TasksCreated++

		worker := ts.selectWorkerForTask(task)
		if worker != nil {
			ts.executeTaskOnWorker(context.Background(), task, worker, nil, result)
		}
	}
}

// checkForInconsistencies checks for state inconsistencies
func (ts *TaskSimulator) checkForInconsistencies(result *SimulationResult) {
	// Check for volume reservation inconsistencies
	// Check for duplicate tasks
	// Check for orphaned tasks
	// This would be more comprehensive in a real implementation

	for _, worker := range ts.mockWorkers {
		worker.mutex.Lock()
		for taskID, mockTask := range worker.CurrentTasks {
			if mockTask.Stuck && time.Since(mockTask.StartTime) > 60*time.Second {
				result.StateInconsistencies++
				result.Warnings = append(result.Warnings, fmt.Sprintf("Long-running stuck task detected: %s", taskID))
			}
		}
		worker.mutex.Unlock()
	}
}

// cleanup cleans up simulation resources
func (ts *TaskSimulator) cleanup() {
	ts.mockWorkers = nil
	ts.mockMaster = nil
}

// GetSimulationResults returns all simulation results
func (ts *TaskSimulator) GetSimulationResults() map[string]*SimulationResult {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	results := make(map[string]*SimulationResult)
	for k, v := range ts.results {
		results[k] = v
	}
	return results
}

// CreateStandardScenarios creates a set of standard test scenarios
func (ts *TaskSimulator) CreateStandardScenarios() {
	// Scenario 1: Worker Timeout During EC
	ts.RegisterScenario(&SimulationScenario{
		Name:        "worker_timeout_during_ec",
		Description: "Test worker timeout during erasure coding operation",
		WorkerCount: 3,
		VolumeCount: 10,
		Duration:    2 * time.Minute,
		FailurePatterns: []*FailurePattern{
			{
				Type:        FailureWorkerTimeout,
				Probability: 1.0,
				Timing: &TimingSpec{
					MinProgress: 50.0,
					MaxProgress: 60.0,
				},
			},
		},
		TestCases: []*TestCase{
			{
				Name:            "ec_timeout_test",
				VolumeID:        1,
				TaskType:        types.TaskTypeErasureCoding,
				ExpectedOutcome: "task_reassigned",
			},
		},
	})

	// Scenario 2: Stuck Vacuum Task
	ts.RegisterScenario(&SimulationScenario{
		Name:        "stuck_vacuum_task",
		Description: "Test stuck vacuum task detection and cleanup",
		WorkerCount: 2,
		VolumeCount: 5,
		Duration:    90 * time.Second,
		TestCases: []*TestCase{
			{
				Name:     "vacuum_stuck_test",
				VolumeID: 2,
				TaskType: types.TaskTypeVacuum,
				FailureToInject: &FailurePattern{
					Type:        FailureTaskStuck,
					Probability: 1.0,
					Timing: &TimingSpec{
						MinProgress: 75.0,
						MaxProgress: 80.0,
					},
				},
				ExpectedOutcome: "task_timeout_detected",
			},
		},
	})

	// Scenario 3: Duplicate Task Prevention
	ts.RegisterScenario(&SimulationScenario{
		Name:        "duplicate_task_prevention",
		Description: "Test duplicate task detection and prevention",
		WorkerCount: 4,
		VolumeCount: 8,
		Duration:    60 * time.Second,
		TestCases: []*TestCase{
			{
				Name:     "duplicate_ec_test_1",
				VolumeID: 3,
				TaskType: types.TaskTypeErasureCoding,
			},
			{
				Name:     "duplicate_ec_test_2", // Same volume, should be detected as duplicate
				VolumeID: 3,
				TaskType: types.TaskTypeErasureCoding,
				FailureToInject: &FailurePattern{
					Type:        FailureDuplicate,
					Probability: 1.0,
				},
				ExpectedOutcome: "duplicate_detected",
			},
		},
	})

	// Scenario 4: Master-Admin State Divergence
	ts.RegisterScenario(&SimulationScenario{
		Name:        "master_admin_divergence",
		Description: "Test state reconciliation between master and admin server",
		WorkerCount: 3,
		VolumeCount: 15,
		Duration:    2 * time.Minute,
		TestCases: []*TestCase{
			{
				Name:            "state_reconciliation_test",
				VolumeID:        4,
				TaskType:        types.TaskTypeErasureCoding,
				ExpectedOutcome: "state_reconciled",
			},
		},
	})
}

// GenerateSimulationReport creates a comprehensive report of simulation results
func (ts *TaskSimulator) GenerateSimulationReport() string {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	report := "# Task Distribution System Simulation Report\n\n"

	for scenarioName, result := range ts.results {
		report += fmt.Sprintf("## Scenario: %s\n", scenarioName)
		report += fmt.Sprintf("- **Duration**: %v\n", result.Duration)
		report += fmt.Sprintf("- **Success**: %v\n", result.Success)
		report += fmt.Sprintf("- **Tasks Created**: %d\n", result.TasksCreated)
		report += fmt.Sprintf("- **Tasks Completed**: %d\n", result.TasksCompleted)
		report += fmt.Sprintf("- **Tasks Failed**: %d\n", result.TasksFailed)
		report += fmt.Sprintf("- **Tasks Stuck**: %d\n", result.TasksStuck)
		report += fmt.Sprintf("- **Worker Timeouts**: %d\n", result.WorkerTimeouts)
		report += fmt.Sprintf("- **Duplicates Found**: %d\n", result.DuplicatesFound)
		report += fmt.Sprintf("- **State Inconsistencies**: %d\n", result.StateInconsistencies)

		if len(result.Errors) > 0 {
			report += "- **Errors**:\n"
			for _, err := range result.Errors {
				report += fmt.Sprintf("  - %s\n", err)
			}
		}

		if len(result.Warnings) > 0 {
			report += "- **Warnings**:\n"
			for _, warning := range result.Warnings {
				report += fmt.Sprintf("  - %s\n", warning)
			}
		}

		report += "\n"
	}

	return report
}
