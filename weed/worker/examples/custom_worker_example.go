package main

import (
	"fmt"
	"log"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func main() {
	// Create worker configuration
	config := &types.WorkerConfig{
		AdminServer:         "localhost:9333",
		MaxConcurrent:       3,
		HeartbeatInterval:   15 * time.Second,
		TaskRequestInterval: 3 * time.Second,
		Capabilities: []types.TaskType{
			types.TaskTypeVacuum,
			types.TaskTypeErasureCoding,
			CustomTaskType, // Our custom task type
		},
	}

	// Create worker instance
	workerInstance, err := worker.NewWorker(config)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	// Register custom task
	registry := workerInstance.GetTaskRegistry()
	RegisterCustomTask(registry)

	// Create mock admin client for testing
	adminClient := worker.NewMockAdminClient()
	workerInstance.SetAdminClient(adminClient)

	// Start the worker
	if err := workerInstance.Start(); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	fmt.Printf("Custom worker %s started at %s\n", workerInstance.ID(), workerInstance.Address())

	// Run for a while
	time.Sleep(30 * time.Second)

	// Stop the worker
	if err := workerInstance.Stop(); err != nil {
		log.Printf("Error stopping worker: %v", err)
	}

	fmt.Println("Custom worker stopped")
}

// Custom task type (extending the existing enum)
const CustomTaskType types.TaskType = "custom_task"

// RegisterCustomTask registers our custom task with the registry
func RegisterCustomTask(registry *tasks.TaskRegistry) {
	factory := &CustomTaskFactory{}
	registry.Register(CustomTaskType, factory)
}

// CustomTaskFactory creates custom task instances
type CustomTaskFactory struct{}

// Create creates a new custom task instance
func (f *CustomTaskFactory) Create(params types.TaskParams) (types.TaskInterface, error) {
	return &CustomTask{
		params:   params,
		progress: 0.0,
	}, nil
}

// Capabilities returns the capabilities required for this task type
func (f *CustomTaskFactory) Capabilities() []string {
	return []string{"custom", "processing"}
}

// Description returns the description of this task type
func (f *CustomTaskFactory) Description() string {
	return "Custom task implementation for demonstration purposes"
}

// CustomTask implements our custom task logic
type CustomTask struct {
	params   types.TaskParams
	progress float64
}

// Type returns the task type
func (t *CustomTask) Type() types.TaskType {
	return CustomTaskType
}

// Execute executes the custom task
func (t *CustomTask) Execute(params types.TaskParams) error {
	fmt.Printf("Executing custom task: %+v\n", params)

	// Simulate work with progress updates
	for i := 0; i <= 100; i += 10 {
		time.Sleep(100 * time.Millisecond)
		t.progress = float64(i)
		fmt.Printf("Custom task progress: %.0f%%\n", t.progress)
	}

	fmt.Println("Custom task completed successfully")
	return nil
}

// Validate validates the task parameters
func (t *CustomTask) Validate(params types.TaskParams) error {
	if params.Server == "" {
		return fmt.Errorf("server is required")
	}
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *CustomTask) EstimateTime(params types.TaskParams) time.Duration {
	return 2 * time.Second
}

// GetProgress returns the current progress (0.0 to 100.0)
func (t *CustomTask) GetProgress() float64 {
	return t.progress
}

// Cancel cancels the task
func (t *CustomTask) Cancel() error {
	fmt.Println("Custom task cancelled")
	return nil
}

// Example of how to create a worker with custom configuration
func ExampleCustomWorkerCreation() {
	// Simple configuration - ID and address are auto-generated
	config := &types.WorkerConfig{
		AdminServer:   "localhost:9333",
		MaxConcurrent: 1,
		Capabilities: []types.TaskType{
			types.TaskTypeVacuum,
		},
	}

	worker, err := worker.NewWorker(config)
	if err != nil {
		log.Printf("Failed to create worker: %v", err)
		return
	}

	fmt.Printf("Worker created with ID: %s, Address: %s\n", worker.ID(), worker.Address())
}

// Example of how to register multiple custom tasks
func ExampleMultipleCustomTasks() {
	config := &types.WorkerConfig{
		AdminServer:   "localhost:9333",
		MaxConcurrent: 2,
		Capabilities: []types.TaskType{
			CustomTaskType,
			"another_custom_task", // Another custom task type
		},
	}

	worker, err := worker.NewWorker(config)
	if err != nil {
		log.Printf("Failed to create worker: %v", err)
		return
	}

	registry := worker.GetTaskRegistry()

	// Register multiple custom tasks
	RegisterCustomTask(registry)
	registry.Register("another_custom_task", &AnotherCustomTaskFactory{})

	fmt.Printf("Worker with multiple custom tasks: %s\n", worker.ID())
}

// AnotherCustomTaskFactory for demonstration
type AnotherCustomTaskFactory struct{}

func (f *AnotherCustomTaskFactory) Create(params types.TaskParams) (types.TaskInterface, error) {
	return &AnotherCustomTask{}, nil
}

func (f *AnotherCustomTaskFactory) Capabilities() []string {
	return []string{"another", "custom"}
}

func (f *AnotherCustomTaskFactory) Description() string {
	return "Another custom task for demonstration"
}

// AnotherCustomTask for demonstration
type AnotherCustomTask struct{}

func (t *AnotherCustomTask) Type() types.TaskType {
	return "another_custom_task"
}

func (t *AnotherCustomTask) Execute(params types.TaskParams) error {
	fmt.Println("Executing another custom task")
	time.Sleep(time.Second)
	return nil
}

func (t *AnotherCustomTask) Validate(params types.TaskParams) error {
	return nil
}

func (t *AnotherCustomTask) EstimateTime(params types.TaskParams) time.Duration {
	return time.Second
}

func (t *AnotherCustomTask) GetProgress() float64 {
	return 100.0
}

func (t *AnotherCustomTask) Cancel() error {
	return nil
}
