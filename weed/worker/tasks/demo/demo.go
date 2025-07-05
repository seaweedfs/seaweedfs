package demo

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements a demo task to showcase the new task structure
type Task struct {
	*tasks.BaseTask
	server      string
	volumeID    uint32
	demoMessage string
}

// NewTask creates a new demo task instance
func NewTask(server string, volumeID uint32, demoMessage string) *Task {
	task := &Task{
		BaseTask:    tasks.NewBaseTask("demo_task"),
		server:      server,
		volumeID:    volumeID,
		demoMessage: demoMessage,
	}
	return task
}

// Execute executes the demo task
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("🚀 Starting demo task for volume %d on server %s", t.volumeID, t.server)

	if t.demoMessage != "" {
		glog.Infof("📝 Demo message: %s", t.demoMessage)
	}

	// Simulate a multi-step process with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
		emoji    string
	}{
		{"Initializing demo", 500 * time.Millisecond, 10, "🔧"},
		{"Loading data", 1 * time.Second, 25, "📊"},
		{"Processing volume", 2 * time.Second, 60, "⚡"},
		{"Optimizing", 1 * time.Second, 80, "🎯"},
		{"Finalizing", 500 * time.Millisecond, 100, "✅"},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			glog.Warningf("❌ Demo task cancelled during %s", step.name)
			return fmt.Errorf("demo task cancelled during %s", step.name)
		}

		glog.Infof("%s Demo task step: %s (%.0f%%)", step.emoji, step.name, step.progress)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("🎉 Demo task completed successfully for volume %d on server %s", t.volumeID, t.server)
	return nil
}

// Validate validates the task parameters
func (t *Task) Validate(params types.TaskParams) error {
	if params.VolumeID == 0 {
		return fmt.Errorf("volume_id is required for demo task")
	}
	if params.Server == "" {
		return fmt.Errorf("server is required for demo task")
	}
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
	// Demo task takes about 5 seconds
	return 5 * time.Second
}

// Factory creates demo task instances
type Factory struct {
	*tasks.BaseTaskFactory
}

// NewFactory creates a new demo task factory
func NewFactory() *Factory {
	return &Factory{
		BaseTaskFactory: tasks.NewBaseTaskFactory(
			"demo_task",
			[]string{"demo", "showcase", "example"},
			"Demo task to showcase the new modular task structure",
		),
	}
}

// Create creates a new demo task instance
func (f *Factory) Create(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	// Extract demo message from parameters
	demoMessage := ""
	if params.Parameters != nil {
		if msg, ok := params.Parameters["message"].(string); ok {
			demoMessage = msg
		}
	}

	task := NewTask(params.Server, params.VolumeID, demoMessage)
	task.SetEstimatedDuration(task.EstimateTime(params))

	return task, nil
}

// Register registers the demo task with the given registry
// This is the only function needed to add a new task type!
func Register(registry *tasks.TaskRegistry) {
	factory := NewFactory()
	registry.Register("demo_task", factory)
	glog.V(1).Infof("🎪 Registered demo task type - showcasing easy task addition!")
}

// Example usage:
/*
To add this demo task to your worker:

1. This file is already created (weed/worker/tasks/demo/demo.go)

2. Import and register in weed/worker/worker.go:
   import "github.com/seaweedfs/seaweedfs/weed/worker/tasks/demo"

   func RegisterAllTasks(registry *tasks.TaskRegistry) {
       // ... existing registrations ...
       demo.Register(registry)
   }

3. That's it! The demo task is now available to all workers.

The task can be used with parameters like:
{
    "volume_id": 123,
    "server": "localhost:8080",
    "parameters": {
        "message": "Hello from demo task!"
    }
}
*/
