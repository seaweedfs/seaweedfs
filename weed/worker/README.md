# SeaweedFS Worker Package

This package provides a comprehensive worker system for SeaweedFS maintenance tasks such as vacuum, erasure coding, remote upload, and replication fixes.

## Features

- **Automatic Configuration**: Worker ID and address are automatically generated
- **Task Plugin System**: Easy registration of custom task types
- **Concurrent Processing**: Configurable concurrent task execution
- **Admin Integration**: Seamless integration with SeaweedFS admin server
- **Extensible Architecture**: Simple interfaces for custom workers and tasks
- **Type Safety**: Strong typing throughout the system
- **Comprehensive Logging**: Detailed logging for monitoring and debugging

## Quick Start

### Basic Usage

```bash
# Start a worker with default configuration
weed worker -admin=localhost:9333

# Start a worker with specific capabilities
weed worker -admin=localhost:9333 -capabilities=vacuum,ec

# Start a worker with custom concurrency
weed worker -admin=localhost:9333 -maxConcurrent=4
```

### Configuration

The worker configuration has been simplified to focus on essential parameters:

```go
config := &types.WorkerConfig{
    AdminServer:         "localhost:9333",
    Capabilities:        []types.TaskType{types.TaskTypeVacuum, types.TaskTypeErasureCoding},
    MaxConcurrent:       2,
    HeartbeatInterval:   30 * time.Second,
    TaskRequestInterval: 5 * time.Second,
}
```

Worker ID and address are automatically generated:
- **ID**: `worker-{hostname}-{timestamp}`
- **Address**: `:8082` (default)

## Architecture

### Core Components

```
weed/worker/
├── worker.go              # Main worker implementation
├── registry.go            # Worker registry and statistics
├── client.go              # Admin server client
├── types/                 # Type definitions
│   ├── task_types.go      # Task types and structures
│   ├── worker_types.go    # Worker types and interfaces
│   └── config_types.go    # Configuration types
├── tasks/                 # Task implementations
│   ├── task.go            # Base task interface and registry
│   └── vacuum.go          # Vacuum task implementation
└── examples/              # Usage examples
    └── custom_worker_example.go
```

### Task Types

The system supports the following built-in task types:

- **vacuum**: Reclaim disk space by removing deleted files
- **ec** (erasure_coding): Convert volumes to erasure coded format
- **remote**: Upload volumes to remote storage
- **replication**: Fix replication issues
- **balance**: Balance data across volume servers
- **cluster_replication**: Replicate data between clusters

## Creating Custom Tasks

### Step 1: Define Task Type

```go
const CustomTaskType types.TaskType = "custom_task"
```

### Step 2: Implement Task Interface

```go
type CustomTask struct {
    params   types.TaskParams
    progress float64
}

func (t *CustomTask) Type() types.TaskType {
    return CustomTaskType
}

func (t *CustomTask) Execute(params types.TaskParams) error {
    // Your task implementation here
    return nil
}

func (t *CustomTask) Validate(params types.TaskParams) error {
    // Validate parameters
    return nil
}

func (t *CustomTask) EstimateTime(params types.TaskParams) time.Duration {
    return 5 * time.Second
}

func (t *CustomTask) GetProgress() float64 {
    return t.progress
}

func (t *CustomTask) Cancel() error {
    // Handle cancellation
    return nil
}
```

### Step 3: Create Task Factory

```go
type CustomTaskFactory struct{}

func (f *CustomTaskFactory) Create(params types.TaskParams) (types.TaskInterface, error) {
    return &CustomTask{params: params}, nil
}

func (f *CustomTaskFactory) Capabilities() []string {
    return []string{"custom", "processing"}
}

func (f *CustomTaskFactory) Description() string {
    return "Custom task implementation"
}
```

### Step 4: Register Task

```go
func RegisterCustomTask(registry *tasks.TaskRegistry) {
    factory := &CustomTaskFactory{}
    registry.Register(CustomTaskType, factory)
}
```

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
        MaxConcurrent: 2,
        Capabilities: []types.TaskType{
            types.TaskTypeVacuum,
            types.TaskTypeErasureCoding,
        },
    }

    worker, err := worker.NewWorker(config)
    if err != nil {
        log.Fatalf("Failed to create worker: %v", err)
    }

    // Worker ID and address are automatically generated
    fmt.Printf("Worker ID: %s\n", worker.ID())
    fmt.Printf("Worker Address: %s\n", worker.Address())

    // Set up admin client
    adminClient, err := worker.CreateAdminClient(config.AdminServer, worker.ID(), "http")
    if err != nil {
        log.Fatalf("Failed to create admin client: %v", err)
    }

    worker.SetAdminClient(adminClient)

    // Start worker
    if err := worker.Start(); err != nil {
        log.Fatalf("Failed to start worker: %v", err)
    }

    // Worker is now running
    select {} // Keep running
}
```

### Custom Worker with Custom Tasks

```go
package main

import (
    "github.com/seaweedfs/seaweedfs/weed/worker"
    "github.com/seaweedfs/seaweedfs/weed/worker/types"
)

const CustomTaskType types.TaskType = "custom_task"

func main() {
    config := &types.WorkerConfig{
        AdminServer:  "localhost:9333",
        MaxConcurrent: 3,
        Capabilities: []types.TaskType{
            types.TaskTypeVacuum,
            CustomTaskType,
        },
    }

    worker, err := worker.NewWorker(config)
    if err != nil {
        log.Fatalf("Failed to create worker: %v", err)
    }

    // Register custom task
    registry := worker.GetTaskRegistry()
    RegisterCustomTask(registry)

    // Set up and start worker
    adminClient := worker.NewMockAdminClient()
    worker.SetAdminClient(adminClient)

    if err := worker.Start(); err != nil {
        log.Fatalf("Failed to start worker: %v", err)
    }

    fmt.Printf("Custom worker %s started\n", worker.ID())
    select {} // Keep running
}
```

## Configuration Options

### WorkerConfig

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| AdminServer | string | Admin server address | "localhost:9333" |
| Capabilities | []TaskType | Supported task types | All built-in types |
| MaxConcurrent | int | Maximum concurrent tasks | 2 |
| HeartbeatInterval | time.Duration | Heartbeat interval | 30s |
| TaskRequestInterval | time.Duration | Task request interval | 5s |
| CustomParameters | map[string]interface{} | Custom parameters | nil |

### Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| -admin | Admin server address | "localhost:9333" |
| -capabilities | Comma-separated task types | "vacuum,ec,remote,replication,balance" |
| -maxConcurrent | Maximum concurrent tasks | 2 |
| -heartbeat | Heartbeat interval | 30s |
| -taskInterval | Task request interval | 5s |
| -clientType | Admin client type | "http" |

## Integration with Admin Server

The worker system integrates seamlessly with the SeaweedFS admin server:

1. **Worker Registration**: Workers automatically register with the admin server
2. **Task Assignment**: Admin server assigns tasks based on worker capabilities
3. **Progress Reporting**: Workers report task progress and completion
4. **Health Monitoring**: Regular heartbeats ensure worker health

## Monitoring and Metrics

### Worker Status

```go
status := worker.GetStatus()
fmt.Printf("Worker %s: %s\n", status.WorkerID, status.Status)
fmt.Printf("Current Load: %d/%d\n", status.CurrentLoad, status.MaxConcurrent)
fmt.Printf("Tasks Completed: %d\n", status.TasksCompleted)
fmt.Printf("Tasks Failed: %d\n", status.TasksFailed)
```

### Performance Metrics

```go
metrics := worker.GetPerformanceMetrics()
fmt.Printf("Success Rate: %.2f%%\n", metrics.SuccessRate)
fmt.Printf("Uptime: %v\n", metrics.Uptime)
```

## Best Practices

### 1. Configuration

- **Keep it simple**: Use default values unless you have specific requirements
- **Match capabilities**: Only enable capabilities your infrastructure supports
- **Reasonable concurrency**: Don't overload your system with too many concurrent tasks

### 2. Custom Tasks

- **Validate parameters**: Always validate input parameters
- **Handle cancellation**: Implement proper cancellation logic
- **Report progress**: Update progress for long-running tasks
- **Use appropriate timeouts**: Set realistic time estimates

### 3. Error Handling

- **Graceful degradation**: Handle errors gracefully
- **Proper logging**: Use structured logging for debugging
- **Retry logic**: Implement retry logic for transient failures

### 4. Resource Management

- **Memory usage**: Monitor memory usage for large tasks
- **Disk space**: Ensure sufficient disk space for operations
- **Network bandwidth**: Consider network impact of operations

## Troubleshooting

### Common Issues

1. **Worker won't start**
   - Check admin server connectivity
   - Verify worker configuration
   - Review logs for errors

2. **Tasks not executing**
   - Check worker capabilities
   - Verify task parameters
   - Review admin server logs

3. **Performance issues**
   - Monitor resource usage
   - Adjust concurrency settings
   - Check network connectivity

### Debug Mode

Enable debug logging for more detailed information:

```bash
weed worker -admin=localhost:9333 -v=2
```

## Migration Guide

If you're upgrading from an older version:

### Before (Old Configuration)

```bash
weed worker -admin=localhost:9333 -id=worker-1 -addr=:8082 -type=maintenance
```

### After (New Simplified Configuration)

```bash
weed worker -admin=localhost:9333
```

The worker ID and address are now automatically generated, making configuration much simpler while maintaining all the functionality.

## Contributing

When adding new features to the worker system:

1. Follow the existing interface patterns
2. Add comprehensive tests
3. Update documentation
4. Provide usage examples
5. Consider backward compatibility

## License

This package is part of SeaweedFS and follows the same licensing terms. 