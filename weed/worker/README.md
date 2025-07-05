# SeaweedFS Worker System

A comprehensive worker system for SeaweedFS maintenance tasks with gRPC communication, automatic configuration, and modular task architecture.

## Features

- **Automatic Configuration**: Worker ID and address are auto-generated
- **gRPC Communication**: Persistent bidirectional streams for real-time communication
- **Modular Task System**: Each task type in its own self-contained package
- **Easy Task Addition**: Simple 3-step process to add new task types
- **Production Ready**: Comprehensive error handling, logging, and monitoring

## Quick Start

### Basic Usage

```bash
# Start a worker (connects to admin server via gRPC)
weed worker -admin=localhost:9333

# Worker will auto-generate:
# - ID: worker-{hostname}-{timestamp}
# - Address: :8082 (not used for HTTP, just identification)
# - gRPC connection to admin server (HTTP port + 10000)
```

### Default Ports

- **Admin HTTP**: `localhost:9333`
- **Admin gRPC**: `localhost:19333` (HTTP port + 10000)

### Configuration

The worker configuration has been simplified to focus on essential parameters:

```go
config := &types.WorkerConfig{
    AdminServer:         "localhost:9333",  // HTTP address, gRPC calculated automatically
    Capabilities:        []types.TaskType{types.TaskTypeVacuum, types.TaskTypeErasureCoding},
    MaxConcurrent:       2,
    HeartbeatInterval:   30 * time.Second,
    TaskRequestInterval: 5 * time.Second,
}
```

Worker ID and address are automatically generated:
- **ID**: `worker-{hostname}-{timestamp}`
- **Address**: `:8082` (no longer used for HTTP server)

## Architecture

### Core Components

```
weed/worker/
├── worker.go              # Main worker implementation
├── registry.go            # Worker registry and statistics
├── client.go              # gRPC admin client with bidirectional streaming
├── types/                 # Type definitions
│   ├── task_types.go      # Task types and structures
│   ├── worker_types.go    # Worker types and interfaces
│   └── config_types.go    # Configuration types
├── tasks/                 # Task implementations
│   ├── task.go            # Base task interface and registry
│   ├── vacuum/            # Vacuum task package
│   │   └── vacuum.go      # All vacuum-related code
│   ├── erasure_coding/    # Erasure coding task package
│   │   └── ec.go          # All EC-related code
│   ├── remote_upload/     # Remote upload task package
│   │   └── remote.go      # All remote upload code
│   ├── replication/       # Replication task package
│   │   └── replication.go # All replication code
│   ├── balance/           # Balance task package
│   │   └── balance.go     # All balance code
│   └── cluster_replication/ # Cluster replication task package
│       └── cluster_replication.go # All cluster replication code
└── examples/              # Usage examples
    └── custom_worker_example.go
```

### Task Package Structure

Each task is now completely self-contained in its own package:

```go
// weed/worker/tasks/my_task/my_task.go
package my_task

// Task implements the actual task logic
type Task struct {
    *tasks.BaseTask
    // task-specific fields
}

// Factory creates task instances
type Factory struct {
    *tasks.BaseTaskFactory
}

// Register registers this task type (single function call)
func Register(registry *tasks.TaskRegistry) {
    factory := NewFactory()
    registry.Register(types.TaskTypeMyTask, factory)
}
```

### gRPC Protocol

The worker system uses `weed/pb/worker.proto` for communication:

- **WorkerMessage**: Messages from worker to admin (registration, heartbeat, task requests, completion)
- **AdminMessage**: Messages from admin to worker (task assignments, cancellations)
- **Bidirectional Stream**: Single persistent connection for all communication

### Task Types

The system supports the following built-in task types:

- **vacuum**: Reclaim disk space by removing deleted files
- **erasure_coding**: Convert volumes to erasure coded format
- **remote_upload**: Upload volumes to remote storage
- **fix_replication**: Fix replication issues
- **balance**: Balance data across volume servers
- **cluster_replication**: Replicate data between clusters

## Adding New Tasks (Easy 3-Step Process)

### Step 1: Create Task Package

Create a new directory under `weed/worker/tasks/`:

```bash
mkdir weed/worker/tasks/my_task
```

### Step 2: Implement Task

Create `weed/worker/tasks/my_task/my_task.go`:

```go
package my_task

import (
    "fmt"
    "time"
    "github.com/seaweedfs/seaweedfs/weed/glog"
    "github.com/seaweedfs/seaweedfs/weed/worker/tasks"
    "github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements your custom task logic
type Task struct {
    *tasks.BaseTask
    server   string
    volumeID uint32
}

// NewTask creates a new task instance
func NewTask(server string, volumeID uint32) *Task {
    return &Task{
        BaseTask: tasks.NewBaseTask(types.TaskTypeMyTask),
        server:   server,
        volumeID: volumeID,
    }
}

// Execute executes the task
func (t *Task) Execute(params types.TaskParams) error {
    glog.Infof("Starting my_task for volume %d on server %s", t.volumeID, t.server)
    
    // Your task implementation here
    t.SetProgress(50)
    time.Sleep(2 * time.Second)
    t.SetProgress(100)
    
    glog.Infof("Completed my_task for volume %d", t.volumeID)
    return nil
}

// Validate validates the task parameters
func (t *Task) Validate(params types.TaskParams) error {
    if params.VolumeID == 0 {
        return fmt.Errorf("volume_id is required")
    }
    if params.Server == "" {
        return fmt.Errorf("server is required")
    }
    return nil
}

// EstimateTime estimates the time needed for the task
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
    return 5 * time.Second
}

// Factory creates task instances
type Factory struct {
    *tasks.BaseTaskFactory
}

// NewFactory creates a new task factory
func NewFactory() *Factory {
    return &Factory{
        BaseTaskFactory: tasks.NewBaseTaskFactory(
            types.TaskTypeMyTask,
            []string{"my_task", "custom"},
            "My custom task description",
        ),
    }
}

// Create creates a new task instance
func (f *Factory) Create(params types.TaskParams) (types.TaskInterface, error) {
    if params.VolumeID == 0 {
        return nil, fmt.Errorf("volume_id is required")
    }
    if params.Server == "" {
        return nil, fmt.Errorf("server is required")
    }

    task := NewTask(params.Server, params.VolumeID)
    task.SetEstimatedDuration(task.EstimateTime(params))
    return task, nil
}

// Register registers the task with the given registry
func Register(registry *tasks.TaskRegistry) {
    factory := NewFactory()
    registry.Register(types.TaskTypeMyTask, factory)
    glog.V(1).Infof("Registered my_task type")
}
```

### Step 3: Register Task

Add your task to `weed/worker/worker.go`:

```go
// Add import
import (
    // ... existing imports ...
    "github.com/seaweedfs/seaweedfs/weed/worker/tasks/my_task"
)

// Add registration call in RegisterAllTasks function
func RegisterAllTasks(registry *tasks.TaskRegistry) {
    // ... existing registrations ...
    my_task.Register(registry)
}
```

### Step 4: Add Task Type (Optional)

If you want to use a new task type enum, add it to `weed/worker/types/task_types.go`:

```go
const (
    // ... existing types ...
    TaskTypeMyTask TaskType = "my_task"
)
```

That's it! Your task is now available to all workers.

## Usage Examples

### Basic Worker

```go
package main

import (
    "github.com/seaweedfs/seaweedfs/weed/worker"
    "github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func main() {
    config := &types.WorkerConfig{
        AdminServer:  "localhost:9333",
        MaxConcurrent: 3,
        Capabilities: []types.TaskType{
            types.TaskTypeVacuum,
            types.TaskTypeErasureCoding,
        },
    }

    worker, err := worker.NewWorker(config)
    if err != nil {
        log.Fatalf("Failed to create worker: %v", err)
    }

    // Built-in tasks are automatically registered
    // Custom tasks can be registered here:
    // registry := worker.GetTaskRegistry()
    // my_task.Register(registry)

    adminClient := worker.CreateAdminClient("grpc", config.AdminServer)
    worker.SetAdminClient(adminClient)

    if err := worker.Start(); err != nil {
        log.Fatalf("Failed to start worker: %v", err)
    }

    fmt.Printf("Worker %s started with gRPC connection\n", worker.ID())
    select {} // Keep running
}
```

### Worker with Custom Task

```go
package main

import (
    "github.com/seaweedfs/seaweedfs/weed/worker"
    "github.com/seaweedfs/seaweedfs/weed/worker/types"
    "github.com/seaweedfs/seaweedfs/weed/worker/tasks"
)

func main() {
    config := &types.WorkerConfig{
        AdminServer:  "localhost:9333",
        MaxConcurrent: 2,
        Capabilities: []types.TaskType{
            types.TaskTypeVacuum,
            "custom_task", // Custom task type
        },
    }

    worker, err := worker.NewWorker(config)
    if err != nil {
        log.Fatalf("Failed to create worker: %v", err)
    }

    // Register custom task
    registry := worker.GetTaskRegistry()
    RegisterCustomTask(registry)

    // Start worker
    adminClient := worker.CreateAdminClient("grpc", config.AdminServer)
    worker.SetAdminClient(adminClient)

    if err := worker.Start(); err != nil {
        log.Fatalf("Failed to start worker: %v", err)
    }

    fmt.Printf("Worker %s started with custom task\n", worker.ID())
    select {} // Keep running
}

// Custom task registration
func RegisterCustomTask(registry *tasks.TaskRegistry) {
    factory := &CustomTaskFactory{}
    registry.Register("custom_task", factory)
}
```

## Configuration Options

### WorkerConfig

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| AdminServer | string | Admin server HTTP address (gRPC = HTTP+10000) | "localhost:9333" |
| Capabilities | []TaskType | Supported task types | All built-in types |
| MaxConcurrent | int | Maximum concurrent tasks | 2 |
| HeartbeatInterval | time.Duration | Heartbeat interval | 30s |
| TaskRequestInterval | time.Duration | Task request interval | 5s |
| CustomParameters | map[string]interface{} | Custom parameters | nil |

### Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| -admin | Admin server HTTP address | "localhost:9333" |
| -capabilities | Comma-separated task types | "vacuum,erasure_coding,remote_upload,fix_replication,balance,cluster_replication" |
| -maxConcurrent | Maximum concurrent tasks | 2 |
| -heartbeat | Heartbeat interval | 30s |
| -taskInterval | Task request interval | 5s |

## Integration with Admin Server

The worker system integrates seamlessly with the SeaweedFS admin server via gRPC:

1. **Worker Connection**: Workers establish persistent gRPC connections (admin HTTP port + 10000)
2. **Task Assignment**: Admin server assigns tasks based on worker capabilities and load
3. **Real-time Updates**: Bidirectional streaming for heartbeats, task updates, and completion
4. **Load Balancing**: Automatic load balancing across available workers

## Benefits of New Task Structure

### Before (scattered code):
- Task logic spread across multiple files
- Difficult to find all related code
- Hard to add new tasks
- Tight coupling between components

### After (self-contained packages):
- ✅ Each task in its own package
- ✅ All related code in one place
- ✅ Easy 3-step process to add new tasks
- ✅ Loose coupling, high cohesion
- ✅ Clear separation of concerns
- ✅ Simple testing and maintenance

## Testing

### Testing gRPC Connection

```go
func TestWorkerConnection() {
    worker, err := worker.NewWorker(config)
    if err != nil {
        t.Fatalf("Failed to create worker: %v", err)
    }

    adminClient := worker.CreateAdminClient("grpc", "localhost:9333")
    worker.SetAdminClient(adminClient)

    if err := worker.Start(); err != nil {
        t.Fatalf("Failed to start worker: %v", err)
    }

    // Test connection
    if !adminClient.IsConnected() {
        t.Error("Worker should be connected to admin server")
    }

    worker.Stop()
}
```

### Testing Custom Tasks

```go
func TestCustomTask() {
    registry := tasks.NewTaskRegistry()
    my_task.Register(registry)

    taskParams := types.TaskParams{
        VolumeID: 123,
        Server:   "localhost:8080",
    }

    task, err := registry.CreateTask("my_task", taskParams)
    if err != nil {
        t.Fatalf("Failed to create task: %v", err)
    }

    if err := task.Execute(taskParams); err != nil {
        t.Fatalf("Failed to execute task: %v", err)
    }
}
```

## Migration from Old Structure

### Before
```
weed/worker/
├── worker.go          # Mixed worker + task logic
├── tasks/
│   ├── task.go        # Base + vacuum implementation
│   └── vacuum.go      # Duplicate vacuum code
```

### After
```
weed/worker/
├── worker.go          # Pure worker logic
├── tasks/
│   ├── task.go        # Base interfaces only
│   ├── vacuum/        # Self-contained vacuum package
│   ├── erasure_coding/ # Self-contained EC package
│   └── ...            # Each task in its own package
```

### Migration Benefits
- **Cleaner Architecture**: Clear separation of concerns
- **Easier Maintenance**: Each task is self-contained
- **Simple Testing**: Test each task in isolation
- **Better Documentation**: Each task package is self-documenting
- **Rapid Development**: Add new tasks in minutes, not hours

## Troubleshooting

### Common Issues

1. **gRPC Connection Failed**
   - Check admin server is running
   - Verify gRPC port (HTTP port + 10000)
   - Check firewall settings

2. **Task Not Found**
   - Verify task is registered in `RegisterAllTasks()`
   - Check task type enum is defined
   - Ensure package imports are correct

3. **Worker Not Receiving Tasks**
   - Check worker capabilities match task requirements
   - Verify admin server has pending tasks
   - Check worker load is below MaxConcurrent

### Debug Mode

```bash
# Enable debug logging
weed worker -admin=localhost:9333 -v=2
```

## Performance

- **Real-time Communication**: gRPC streaming vs HTTP polling
- **Reduced Overhead**: Single persistent connection
- **Better Resource Usage**: Automatic load balancing
- **Faster Task Assignment**: Sub-second task distribution

The new architecture provides significant performance improvements while maintaining full backward compatibility. 