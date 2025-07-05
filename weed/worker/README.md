# SeaweedFS Worker Package

This package provides a comprehensive worker system for SeaweedFS maintenance tasks such as vacuum, erasure coding, remote upload, and replication fixes.

## Features

- **Automatic Configuration**: Worker ID and address are automatically generated
- **gRPC Communication**: Long-running bidirectional gRPC streams for efficient communication
- **Real-time Heartbeats**: Continuous connection with admin server via gRPC
- **Task Plugin System**: Easy registration of custom task types
- **Concurrent Processing**: Configurable concurrent task execution
- **Admin Integration**: Seamless integration with SeaweedFS admin server
- **Extensible Architecture**: Simple interfaces for custom workers and tasks
- **Type Safety**: Strong typing throughout the system
- **Comprehensive Logging**: Detailed logging for monitoring and debugging

## Communication Architecture

The worker system uses **bidirectional gRPC streaming** for all communication with the admin server:

- **Connection**: Workers establish a persistent gRPC connection to admin server (HTTP port + 10000)
- **Heartbeats**: Sent over the gRPC stream with full worker status information
- **Task Assignment**: Admin server pushes tasks to workers via the stream
- **Progress Updates**: Workers send real-time progress updates via the stream
- **Task Completion**: Completion status and results sent via the stream

This approach eliminates HTTP polling overhead and provides real-time communication.

## Quick Start

### Basic Usage

```bash
# Start a worker with default configuration (connects to admin gRPC port)
weed worker -admin=localhost:9333

# Start a worker with specific capabilities
weed worker -admin=localhost:9333 -capabilities=vacuum,ec

# Start a worker with custom concurrency
weed worker -admin=localhost:9333 -maxConcurrent=4
```

### gRPC Connection

Workers automatically connect to the admin server's gRPC port:
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
│   └── vacuum.go          # Vacuum task implementation
└── examples/              # Usage examples
    └── custom_worker_example.go
```

### gRPC Protocol

The worker system uses `weed/pb/worker.proto` for communication:

- **WorkerMessage**: Messages from worker to admin (registration, heartbeat, task requests, completion)
- **AdminMessage**: Messages from admin to worker (task assignments, cancellations)
- **Bidirectional Stream**: Single persistent connection for all communication

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
        AdminServer:  "localhost:9333",  // gRPC will use port 19333
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

    // Set up gRPC admin client
    adminClient, err := worker.CreateAdminClient(config.AdminServer, worker.ID(), "grpc")
    if err != nil {
        log.Fatalf("Failed to create admin client: %v", err)
    }

    worker.SetAdminClient(adminClient)

    // Start worker (automatically connects via gRPC)
    if err := worker.Start(); err != nil {
        log.Fatalf("Failed to start worker: %v", err)
    }

    // Worker is now running with persistent gRPC connection
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

    fmt.Printf("Custom worker %s started with gRPC connection\n", worker.ID())
    select {} // Keep running
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
| -capabilities | Comma-separated task types | "vacuum,ec,remote,replication,balance" |
| -maxConcurrent | Maximum concurrent tasks | 2 |
| -heartbeat | Heartbeat interval | 30s |
| -taskInterval | Task request interval | 5s |

## Integration with Admin Server

The worker system integrates seamlessly with the SeaweedFS admin server via gRPC:

1. **Worker Connection**: Workers establish persistent gRPC connections (admin HTTP port + 10000)
2. **Worker Registration**: Workers register via the gRPC stream
3. **Real-time Heartbeats**: Status updates sent continuously via the stream
4. **Task Assignment**: Admin server pushes tasks to workers via the stream
5. **Progress Reporting**: Workers send real-time progress updates
6. **Health Monitoring**: Connection health monitored via stream status

## gRPC Benefits

The gRPC implementation provides several advantages over HTTP polling:

- **Real-time Communication**: Instant task assignment and progress updates
- **Reduced Overhead**: Single persistent connection vs. multiple HTTP requests
- **Better Error Handling**: Connection state awareness and automatic retry
- **Bidirectional Streams**: Both sides can send messages at any time
- **Built-in Heartbeats**: gRPC keepalive handles connection monitoring

## Monitoring and Metrics

### Worker Status

```go
status := worker.GetStatus()
fmt.Printf("Worker %s: %s\n", status.WorkerID, status.Status)
fmt.Printf("Current Load: %d/%d\n", status.CurrentLoad, status.MaxConcurrent)
fmt.Printf("Tasks Completed: %d\n", status.TasksCompleted)
fmt.Printf("Tasks Failed: %d\n", status.TasksFailed)
fmt.Printf("gRPC Connected: %v\n", worker.GetAdminClient().IsConnected())
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
- **Network considerations**: Ensure gRPC port (HTTP + 10000) is accessible

### 2. Custom Tasks

- **Validate parameters**: Always validate input parameters
- **Handle cancellation**: Implement proper cancellation logic
- **Report progress**: Update progress for long-running tasks
- **Use appropriate timeouts**: Set realistic time estimates

### 3. Error Handling

- **Connection monitoring**: Check gRPC connection status
- **Graceful degradation**: Handle connection failures gracefully
- **Proper logging**: Use structured logging for debugging
- **Retry logic**: gRPC client handles automatic reconnection

### 4. Resource Management

- **Memory usage**: Monitor memory usage for large tasks
- **Disk space**: Ensure sufficient disk space for operations
- **Network bandwidth**: gRPC streams are more efficient than HTTP polling

## Troubleshooting

### Common Issues

1. **Worker won't start**
   - Check admin server gRPC port (HTTP + 10000) connectivity
   - Verify firewall allows gRPC port access
   - Review gRPC connection logs

2. **Tasks not executing**
   - Check gRPC stream status
   - Verify worker capabilities
   - Review admin server logs

3. **Connection issues**
   - Monitor gRPC connection status with `IsConnected()`
   - Check network connectivity to gRPC port
   - Review gRPC keepalive settings

### Debug Mode

Enable debug logging for more detailed information:

```bash
weed worker -admin=localhost:9333 -v=2
```

### gRPC Connection Testing

Test gRPC connectivity manually:

```bash
# Check if gRPC port is accessible
telnet localhost 19333

# Use grpcurl to test gRPC service (if available)
grpcurl -plaintext localhost:19333 list
```

## Migration Guide

If you're upgrading from an older version:

### Before (HTTP Polling)

```bash
weed worker -admin=localhost:9333 -id=worker-1 -addr=:8082
```

### After (gRPC Streaming)

```bash
weed worker -admin=localhost:9333
```

### Key Changes

- **No HTTP server**: Workers no longer run HTTP servers
- **gRPC only**: All communication via bidirectional gRPC streams
- **Auto-configuration**: Worker ID and address automatically generated
- **Real-time**: Instant communication vs. polling intervals
- **Port calculation**: gRPC port = HTTP port + 10000

## Contributing

When adding new features to the worker system:

1. Follow the existing interface patterns
2. Add comprehensive tests
3. Update documentation
4. Provide usage examples
5. Consider backward compatibility
6. Test gRPC communication thoroughly

## License

This package is part of SeaweedFS and follows the same licensing terms. 