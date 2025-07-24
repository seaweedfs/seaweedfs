# SeaweedFS Task Distribution System Design

## Overview

This document describes the design of a distributed task management system for SeaweedFS that handles Erasure Coding (EC) and vacuum operations through a scalable admin server and worker process architecture.

## System Architecture

### High-Level Components

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Master        │◄──►│  Admin Server    │◄──►│   Workers       │
│                 │    │                  │    │                 │
│ - Volume Info   │    │ - Task Discovery │    │ - Task Exec     │
│ - Shard Status  │    │ - Task Assign    │    │ - Progress      │
│ - Heartbeats    │    │ - Progress Track │    │ - Error Report  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                        │
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Volume Servers  │    │ Volume Monitor   │    │ Task Execution  │
│                 │    │                  │    │                 │
│ - Store Volumes │    │ - Health Check   │    │ - EC Convert    │
│ - EC Shards     │    │ - Usage Stats    │    │ - Vacuum Clean  │
│ - Report Status │    │ - State Sync     │    │ - Status Report │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 1. Admin Server Design

### 1.1 Core Responsibilities

- **Task Discovery**: Scan volumes to identify EC and vacuum candidates
- **Worker Management**: Track available workers and their capabilities  
- **Task Assignment**: Match tasks to optimal workers
- **Progress Tracking**: Monitor in-progress tasks for capacity planning
- **State Reconciliation**: Sync with master server for volume state updates

### 1.2 Task Discovery Engine

```go
type TaskDiscoveryEngine struct {
    masterClient   MasterClient
    volumeScanner  VolumeScanner
    taskDetectors  map[TaskType]TaskDetector
    scanInterval   time.Duration
}

type VolumeCandidate struct {
    VolumeID       uint32
    Server         string
    Collection     string
    TaskType       TaskType
    Priority       TaskPriority
    Reason         string
    DetectedAt     time.Time
    Parameters     map[string]interface{}
}
```

**EC Detection Logic**:
- Find volumes >= 95% full and idle for > 1 hour
- Exclude volumes already in EC format
- Exclude volumes with ongoing operations
- Prioritize by collection and age

**Vacuum Detection Logic**:
- Find volumes with garbage ratio > 30%
- Exclude read-only volumes
- Exclude volumes with recent vacuum operations
- Prioritize by garbage percentage

### 1.3 Worker Registry & Management

```go
type WorkerRegistry struct {
    workers        map[string]*Worker
    capabilities   map[TaskType][]*Worker
    lastHeartbeat  map[string]time.Time
    taskAssignment map[string]*Task
    mutex          sync.RWMutex
}

type Worker struct {
    ID            string
    Address       string
    Capabilities  []TaskType
    MaxConcurrent int
    CurrentLoad   int
    Status        WorkerStatus
    LastSeen      time.Time
    Performance   WorkerMetrics
}
```

### 1.4 Task Assignment Algorithm

```go
type TaskScheduler struct {
    registry       *WorkerRegistry
    taskQueue      *PriorityQueue
    inProgressTasks map[string]*InProgressTask
    volumeReservations map[uint32]*VolumeReservation
}

// Worker Selection Criteria:
// 1. Has required capability (EC or Vacuum)
// 2. Available capacity (CurrentLoad < MaxConcurrent)
// 3. Best performance history for task type
// 4. Lowest current load
// 5. Geographically close to volume server (optional)
```

## 2. Worker Process Design

### 2.1 Worker Architecture

```go
type MaintenanceWorker struct {
    id              string
    config          *WorkerConfig
    adminClient     AdminClient
    taskExecutors   map[TaskType]TaskExecutor
    currentTasks    map[string]*RunningTask
    registry        *TaskRegistry
    heartbeatTicker *time.Ticker
    requestTicker   *time.Ticker
}
```

### 2.2 Task Execution Framework

```go
type TaskExecutor interface {
    Execute(ctx context.Context, task *Task) error
    EstimateTime(task *Task) time.Duration
    ValidateResources(task *Task) error
    GetProgress() float64
    Cancel() error
}

type ErasureCodingExecutor struct {
    volumeClient VolumeServerClient
    progress     float64
    cancelled    bool
}

type VacuumExecutor struct {
    volumeClient VolumeServerClient
    progress     float64
    cancelled    bool
}
```

### 2.3 Worker Capabilities & Registration

```go
type WorkerCapabilities struct {
    SupportedTasks   []TaskType
    MaxConcurrent    int
    ResourceLimits   ResourceLimits
    PreferredServers []string  // Affinity for specific volume servers
}

type ResourceLimits struct {
    MaxMemoryMB      int64
    MaxDiskSpaceMB   int64
    MaxNetworkMbps   int64
    MaxCPUPercent    float64
}
```

## 3. Task Lifecycle Management

### 3.1 Task States

```go
type TaskState string

const (
    TaskStatePending     TaskState = "pending"
    TaskStateAssigned    TaskState = "assigned"
    TaskStateInProgress  TaskState = "in_progress"
    TaskStateCompleted   TaskState = "completed"
    TaskStateFailed      TaskState = "failed"
    TaskStateCancelled   TaskState = "cancelled"
    TaskStateStuck       TaskState = "stuck"       // Taking too long
    TaskStateDuplicate   TaskState = "duplicate"   // Detected duplicate
)
```

### 3.2 Progress Tracking & Monitoring

```go
type InProgressTask struct {
    Task           *Task
    WorkerID       string
    StartedAt      time.Time
    LastUpdate     time.Time
    Progress       float64
    EstimatedEnd   time.Time
    VolumeReserved bool  // Reserved for capacity planning
}

type TaskMonitor struct {
    inProgressTasks map[string]*InProgressTask
    timeoutChecker  *time.Ticker
    stuckDetector   *time.Ticker
    duplicateChecker *time.Ticker
}
```

## 4. Volume Capacity Reconciliation

### 4.1 Volume State Tracking

```go
type VolumeStateManager struct {
    masterClient      MasterClient
    inProgressTasks   map[uint32]*InProgressTask  // VolumeID -> Task
    committedChanges  map[uint32]*VolumeChange    // Changes not yet in master
    reconcileInterval time.Duration
}

type VolumeChange struct {
    VolumeID     uint32
    ChangeType   ChangeType  // "ec_encoding", "vacuum_completed"
    OldCapacity  int64
    NewCapacity  int64
    TaskID       string
    CompletedAt  time.Time
    ReportedToMaster bool
}
```

### 4.2 Shard Assignment Integration

When the master needs to assign shards, it must consider:
1. **Current volume state** from its own records
2. **In-progress capacity changes** from admin server
3. **Committed but unreported changes** from admin server

```go
type CapacityOracle struct {
    adminServer   AdminServerClient
    masterState   *MasterVolumeState
    updateFreq    time.Duration
}

func (o *CapacityOracle) GetAdjustedCapacity(volumeID uint32) int64 {
    baseCapacity := o.masterState.GetCapacity(volumeID)
    
    // Adjust for in-progress tasks
    if task := o.adminServer.GetInProgressTask(volumeID); task != nil {
        switch task.Type {
        case TaskTypeErasureCoding:
            // EC reduces effective capacity
            return baseCapacity / 2  // Simplified
        case TaskTypeVacuum:
            // Vacuum may increase available space
            return baseCapacity + int64(float64(baseCapacity) * 0.3)
        }
    }
    
    // Adjust for completed but unreported changes
    if change := o.adminServer.GetPendingChange(volumeID); change != nil {
        return change.NewCapacity
    }
    
    return baseCapacity
}
```

## 5. Error Handling & Recovery

### 5.1 Worker Failure Scenarios

```go
type FailureHandler struct {
    taskRescheduler *TaskRescheduler
    workerMonitor   *WorkerMonitor
    alertManager    *AlertManager
}

// Failure Scenarios:
// 1. Worker becomes unresponsive (heartbeat timeout)
// 2. Task execution fails (reported by worker)
// 3. Task gets stuck (progress timeout)
// 4. Duplicate task detection
// 5. Resource exhaustion
```

### 5.2 Recovery Strategies

**Worker Timeout Recovery**:
- Mark worker as inactive after 3 missed heartbeats
- Reschedule all assigned tasks to other workers
- Cleanup any partial state

**Task Stuck Recovery**:
- Detect tasks with no progress for > 2x estimated time
- Cancel stuck task and mark volume for cleanup
- Reschedule if retry count < max_retries

**Duplicate Task Prevention**:
```go
type DuplicateDetector struct {
    activeFingerprints map[string]bool  // VolumeID+TaskType
    recentCompleted    *LRUCache        // Recently completed tasks
}

func (d *DuplicateDetector) IsTaskDuplicate(task *Task) bool {
    fingerprint := fmt.Sprintf("%d-%s", task.VolumeID, task.Type)
    return d.activeFingerprints[fingerprint] || 
           d.recentCompleted.Contains(fingerprint)
}
```

## 6. Simulation & Testing Framework

### 6.1 Failure Simulation

```go
type TaskSimulator struct {
    scenarios map[string]SimulationScenario
}

type SimulationScenario struct {
    Name            string
    WorkerCount     int
    VolumeCount     int
    FailurePatterns []FailurePattern
    Duration        time.Duration
}

type FailurePattern struct {
    Type        FailureType  // "worker_timeout", "task_stuck", "duplicate"
    Probability float64      // 0.0 to 1.0
    Timing      TimingSpec   // When during task execution
    Duration    time.Duration
}
```

### 6.2 Test Scenarios

**Scenario 1: Worker Timeout During EC**
- Start EC task on 30GB volume
- Kill worker at 50% progress
- Verify task reassignment
- Verify no duplicate EC operations

**Scenario 2: Stuck Vacuum Task**
- Start vacuum on high-garbage volume
- Simulate worker hanging at 75% progress
- Verify timeout detection and cleanup
- Verify volume state consistency

**Scenario 3: Duplicate Task Prevention**
- Submit same EC task from multiple sources
- Verify only one task executes
- Verify proper conflict resolution

**Scenario 4: Master-Admin State Divergence**
- Create in-progress EC task
- Simulate master restart
- Verify state reconciliation
- Verify shard assignment accounts for in-progress work

## 7. Performance & Scalability

### 7.1 Metrics & Monitoring

```go
type SystemMetrics struct {
    TasksPerSecond     float64
    WorkerUtilization  float64
    AverageTaskTime    time.Duration
    FailureRate        float64
    QueueDepth         int
    VolumeStatesSync   bool
}
```

### 7.2 Scalability Considerations

- **Horizontal Worker Scaling**: Add workers without admin server changes
- **Admin Server HA**: Master-slave admin servers for fault tolerance
- **Task Partitioning**: Partition tasks by collection or datacenter
- **Batch Operations**: Group similar tasks for efficiency

## 8. Implementation Plan

### Phase 1: Core Infrastructure
1. Admin server basic framework
2. Worker registration and heartbeat
3. Simple task assignment
4. Basic progress tracking

### Phase 2: Advanced Features
1. Volume state reconciliation
2. Sophisticated worker selection
3. Failure detection and recovery
4. Duplicate prevention

### Phase 3: Optimization & Monitoring
1. Performance metrics
2. Load balancing algorithms
3. Capacity planning integration
4. Comprehensive monitoring

This design provides a robust, scalable foundation for distributed task management in SeaweedFS while maintaining consistency with the existing architecture patterns. 