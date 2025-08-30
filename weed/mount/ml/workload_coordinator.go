package ml

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// WorkloadType represents different types of ML workloads
type WorkloadType int

const (
	WorkloadTypeUnknown WorkloadType = iota
	WorkloadTypeTraining                 // Model training workloads
	WorkloadTypeInference                // Model inference workloads
	WorkloadTypeDataPreprocessing        // Data preprocessing pipelines
	WorkloadTypeFeatureEngineering       // Feature engineering workloads
	WorkloadTypeModelValidation          // Model validation and testing
	WorkloadTypeHyperparameterTuning     // Hyperparameter optimization
	WorkloadTypeAutoML                   // Automated ML pipelines
	WorkloadTypeModelServing             // Model serving workloads
)

// WorkloadPriority represents workload priority levels
type WorkloadPriority int

const (
	PriorityLow WorkloadPriority = iota
	PriorityNormal
	PriorityHigh
	PriorityUrgent
	PriorityCritical
)

// ProcessInfo represents information about a process
type ProcessInfo struct {
	sync.RWMutex
	
	// Process identification
	PID              int               `json:"pid"`
	ProcessName      string            `json:"process_name"`
	CommandLine      string            `json:"command_line"`
	WorkingDirectory string            `json:"working_directory"`
	
	// Process state
	Status           string            `json:"status"`        // running, sleeping, stopped, etc.
	StartTime        time.Time         `json:"start_time"`
	CPUUsage         float64           `json:"cpu_usage"`     // CPU usage percentage
	MemoryUsage      uint64            `json:"memory_usage"`  // Memory usage in bytes
	GPUUsage         map[int]float64   `json:"gpu_usage"`     // GPU ID -> usage percentage
	
	// ML workload characteristics
	WorkloadType     WorkloadType      `json:"workload_type"`
	Priority         WorkloadPriority  `json:"priority"`
	Framework        string            `json:"framework"`     // tensorflow, pytorch, etc.
	
	// File access patterns
	OpenFiles        map[string]*FileDescriptor `json:"open_files"`     // FD -> file info
	RecentAccesses   []FileAccess               `json:"recent_accesses"` // Recent file accesses
	AccessPatterns   map[string]AccessPattern   `json:"access_patterns"` // File -> pattern
	
	// Resource requirements
	ExpectedRuntime  time.Duration     `json:"expected_runtime"`
	MaxMemoryUsage   uint64            `json:"max_memory_usage"`
	RequiredGPUs     []int             `json:"required_gpus"`
	IOIntensity      string            `json:"io_intensity"`  // low, medium, high
	
	// Coordination state
	LastHeartbeat    time.Time         `json:"last_heartbeat"`
	CoordinationGroup string           `json:"coordination_group"` // Group for coordination
	Dependencies     []int             `json:"dependencies"`       // PID dependencies
}

// FileDescriptor represents an open file descriptor
type FileDescriptor struct {
	FD         int                    `json:"fd"`
	FilePath   string                 `json:"file_path"`
	Mode       string                 `json:"mode"`        // read, write, append, etc.
	Position   int64                  `json:"position"`    // Current file position
	OpenTime   time.Time              `json:"open_time"`
	AccessCount int64                 `json:"access_count"`
	LastAccess time.Time              `json:"last_access"`
	FileType   MLFileType             `json:"file_type"`
	Metadata   map[string]interface{} `json:"metadata"`
}

// FileAccess represents a file access event
type FileAccess struct {
	Timestamp time.Time     `json:"timestamp"`
	FilePath  string        `json:"file_path"`
	Operation string        `json:"operation"` // read, write, seek, etc.
	Offset    int64         `json:"offset"`
	Size      int           `json:"size"`
	Duration  time.Duration `json:"duration"`
}

// WorkloadCoordinator coordinates ML workloads across processes
type WorkloadCoordinator struct {
	sync.RWMutex
	
	// Configuration
	enabled              bool                              // Whether coordination is enabled
	monitorInterval      time.Duration                     // Process monitoring interval
	heartbeatTimeout     time.Duration                     // Heartbeat timeout
	maxProcesses         int                               // Maximum processes to track
	
	// Process tracking
	processes            map[int]*ProcessInfo              // PID -> process info
	workloadGroups       map[string][]*ProcessInfo         // Group -> processes
	processHierarchy     map[int][]int                     // Parent PID -> child PIDs
	
	// Resource coordination
	resourcePools        map[string]*ResourcePool          // Resource pools by type
	resourceAllocations  map[int]*ResourceAllocation       // PID -> resource allocation
	conflictResolution   *ConflictResolutionPolicy         // Policy for resolving conflicts
	
	// Performance tracking
	systemMetrics        *SystemMetrics                    // System-wide metrics
	workloadMetrics      map[int]*WorkloadMetrics          // PID -> workload metrics
	
	// Communication
	coordinationChannel  chan *CoordinationEvent           // Coordination events
	processEvents        chan *ProcessEvent                // Process events
	
	// Background tasks
	ctx                  context.Context
	cancel               context.CancelFunc
	signalChan           chan os.Signal                    // OS signal handling
	
	// Metrics
	totalProcesses       int64                             // Total processes seen
	activeWorkloads      int64                             // Active workloads
	coordinationEvents   int64                             // Coordination events
	resourceConflicts    int64                             // Resource conflicts resolved
}

// ResourcePool represents a pool of shared resources
type ResourcePool struct {
	sync.RWMutex
	
	ResourceType     string                 `json:"resource_type"`   // memory, gpu, storage, etc.
	TotalCapacity    uint64                 `json:"total_capacity"`
	AvailableCapacity uint64                `json:"available_capacity"`
	Allocations      map[int]uint64         `json:"allocations"`     // PID -> allocated amount
	WaitingQueue     []*ResourceRequest     `json:"waiting_queue"`   // Waiting resource requests
	Policy           string                 `json:"policy"`          // FIFO, Priority, Fair, etc.
	ReservationTime  time.Duration          `json:"reservation_time"` // How long to hold reservations
}

// ResourceAllocation represents allocated resources for a process
type ResourceAllocation struct {
	PID              int                    `json:"pid"`
	Allocations      map[string]uint64      `json:"allocations"`      // Resource type -> amount
	AllocationTime   time.Time              `json:"allocation_time"`
	ExpirationTime   time.Time              `json:"expiration_time"`
	Priority         WorkloadPriority       `json:"priority"`
	Renewable        bool                   `json:"renewable"`
}

// ResourceRequest represents a request for resources
type ResourceRequest struct {
	PID            int                    `json:"pid"`
	ResourceType   string                 `json:"resource_type"`
	Amount         uint64                 `json:"amount"`
	Priority       WorkloadPriority       `json:"priority"`
	RequestTime    time.Time              `json:"request_time"`
	Deadline       time.Time              `json:"deadline"`
	Metadata       map[string]interface{} `json:"metadata"`
}

// ConflictResolutionPolicy defines how to resolve resource conflicts
type ConflictResolutionPolicy struct {
	Strategy          string  `json:"strategy"`           // priority, fair, round_robin
	PreemptionEnabled bool    `json:"preemption_enabled"` // Allow preemption of lower priority workloads
	GracePeriod       time.Duration `json:"grace_period"` // Grace period before preemption
	PriorityWeights   map[WorkloadPriority]float64 `json:"priority_weights"`
}

// SystemMetrics represents system-wide performance metrics
type SystemMetrics struct {
	sync.RWMutex
	
	Timestamp        time.Time `json:"timestamp"`
	CPUUsage         float64   `json:"cpu_usage"`          // Overall CPU usage
	MemoryUsage      uint64    `json:"memory_usage"`       // Total memory usage
	TotalMemory      uint64    `json:"total_memory"`       // Total system memory
	GPUUsage         map[int]float64 `json:"gpu_usage"`    // GPU ID -> usage
	StorageIO        StorageIOMetrics `json:"storage_io"`  // Storage I/O metrics
	NetworkIO        NetworkIOMetrics `json:"network_io"`  // Network I/O metrics
	ActiveProcesses  int       `json:"active_processes"`   // Number of active processes
	LoadAverage      [3]float64 `json:"load_average"`      // 1, 5, 15 minute load averages
}

// StorageIOMetrics represents storage I/O metrics
type StorageIOMetrics struct {
	ReadBytes    uint64  `json:"read_bytes"`
	WriteBytes   uint64  `json:"write_bytes"`
	ReadOps      uint64  `json:"read_ops"`
	WriteOps     uint64  `json:"write_ops"`
	UtilPercent  float64 `json:"util_percent"`
}

// NetworkIOMetrics represents network I/O metrics
type NetworkIOMetrics struct {
	RxBytes   uint64 `json:"rx_bytes"`
	TxBytes   uint64 `json:"tx_bytes"`
	RxPackets uint64 `json:"rx_packets"`
	TxPackets uint64 `json:"tx_packets"`
}

// WorkloadMetrics represents metrics for a specific workload
type WorkloadMetrics struct {
	PID               int           `json:"pid"`
	StartTime         time.Time     `json:"start_time"`
	Runtime           time.Duration `json:"runtime"`
	CPUTime           time.Duration `json:"cpu_time"`
	PeakMemoryUsage   uint64        `json:"peak_memory_usage"`
	TotalBytesRead    uint64        `json:"total_bytes_read"`
	TotalBytesWritten uint64        `json:"total_bytes_written"`
	FileOperations    uint64        `json:"file_operations"`
	NetworkConnections int          `json:"network_connections"`
	ExitCode          int           `json:"exit_code"`
	ExitTime          time.Time     `json:"exit_time"`
}

// CoordinationEvent represents a coordination event
type CoordinationEvent struct {
	Type      string                 `json:"type"`       // resource_request, process_start, etc.
	PID       int                    `json:"pid"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// ProcessEvent represents a process event
type ProcessEvent struct {
	Type      string                 `json:"type"`       // start, stop, fork, exec, etc.
	PID       int                    `json:"pid"`
	PPID      int                    `json:"ppid"`       // Parent PID
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// NewWorkloadCoordinator creates a new workload coordinator
func NewWorkloadCoordinator(enabled bool) *WorkloadCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	
	wc := &WorkloadCoordinator{
		enabled:             enabled,
		monitorInterval:     5 * time.Second,   // Monitor every 5 seconds
		heartbeatTimeout:    30 * time.Second,  // 30-second heartbeat timeout
		maxProcesses:        1000,              // Track up to 1000 processes
		
		processes:           make(map[int]*ProcessInfo),
		workloadGroups:      make(map[string][]*ProcessInfo),
		processHierarchy:    make(map[int][]int),
		resourcePools:       make(map[string]*ResourcePool),
		resourceAllocations: make(map[int]*ResourceAllocation),
		workloadMetrics:     make(map[int]*WorkloadMetrics),
		
		coordinationChannel: make(chan *CoordinationEvent, 1000),
		processEvents:       make(chan *ProcessEvent, 1000),
		signalChan:          make(chan os.Signal, 1),
		
		ctx:    ctx,
		cancel: cancel,
	}
	
	// Initialize system metrics
	wc.systemMetrics = &SystemMetrics{
		CPUUsage:    0.0,
		GPUUsage:    make(map[int]float64),
		LoadAverage: [3]float64{0, 0, 0},
	}
	
	// Initialize resource pools
	wc.initializeResourcePools()
	
	// Initialize conflict resolution policy
	wc.conflictResolution = &ConflictResolutionPolicy{
		Strategy:          "priority",
		PreemptionEnabled: true,
		GracePeriod:       30 * time.Second,
		PriorityWeights: map[WorkloadPriority]float64{
			PriorityLow:      0.1,
			PriorityNormal:   1.0,
			PriorityHigh:     2.0,
			PriorityUrgent:   5.0,
			PriorityCritical: 10.0,
		},
	}
	
	if enabled {
		// Set up signal handling
		signal.Notify(wc.signalChan, syscall.SIGINT, syscall.SIGTERM)
		
		// Start background tasks
		go wc.processMonitorLoop()
		go wc.coordinationEventLoop()
		go wc.systemMetricsLoop()
		go wc.resourceManagerLoop()
		
		glog.V(1).Infof("Workload coordinator started with monitoring interval %v", wc.monitorInterval)
	}
	
	return wc
}

// initializeResourcePools sets up default resource pools
func (wc *WorkloadCoordinator) initializeResourcePools() {
	// Memory resource pool
	wc.resourcePools["memory"] = &ResourcePool{
		ResourceType:      "memory",
		TotalCapacity:     16 * 1024 * 1024 * 1024, // 16GB default
		AvailableCapacity: 16 * 1024 * 1024 * 1024,
		Allocations:       make(map[int]uint64),
		WaitingQueue:      make([]*ResourceRequest, 0),
		Policy:           "Priority",
		ReservationTime:  10 * time.Minute,
	}
	
	// GPU resource pool
	wc.resourcePools["gpu"] = &ResourcePool{
		ResourceType:      "gpu",
		TotalCapacity:     8, // 8 GPUs default
		AvailableCapacity: 8,
		Allocations:       make(map[int]uint64),
		WaitingQueue:      make([]*ResourceRequest, 0),
		Policy:           "FIFO",
		ReservationTime:  1 * time.Hour,
	}
	
	// Storage I/O resource pool
	wc.resourcePools["storage_io"] = &ResourcePool{
		ResourceType:      "storage_io",
		TotalCapacity:     1000 * 1024 * 1024, // 1GB/s bandwidth
		AvailableCapacity: 1000 * 1024 * 1024,
		Allocations:       make(map[int]uint64),
		WaitingQueue:      make([]*ResourceRequest, 0),
		Policy:           "Fair",
		ReservationTime:  5 * time.Minute,
	}
}

// RegisterProcess registers a new process for coordination
func (wc *WorkloadCoordinator) RegisterProcess(pid int, workloadType WorkloadType, priority WorkloadPriority) error {
	wc.Lock()
	defer wc.Unlock()
	
	// Get process information
	processInfo, err := wc.getProcessInfo(pid)
	if err != nil {
		return fmt.Errorf("failed to get process info for PID %d: %w", pid, err)
	}
	
	processInfo.WorkloadType = workloadType
	processInfo.Priority = priority
	processInfo.LastHeartbeat = time.Now()
	
	wc.processes[pid] = processInfo
	wc.totalProcesses++
	
	// Create workload metrics
	wc.workloadMetrics[pid] = &WorkloadMetrics{
		PID:       pid,
		StartTime: processInfo.StartTime,
	}
	
	// Send process start event
	wc.processEvents <- &ProcessEvent{
		Type:      "process_registered",
		PID:       pid,
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"workload_type": workloadType,
			"priority":      priority,
		},
	}
	
	glog.V(2).Infof("Registered process: PID=%d, type=%v, priority=%v", pid, workloadType, priority)
	return nil
}

// getProcessInfo retrieves information about a process
func (wc *WorkloadCoordinator) getProcessInfo(pid int) (*ProcessInfo, error) {
	// In a real implementation, this would read from /proc/PID/ on Linux
	// For now, we'll create a basic process info structure
	
	processInfo := &ProcessInfo{
		PID:              pid,
		ProcessName:      fmt.Sprintf("process-%d", pid),
		CommandLine:      "python train.py",
		WorkingDirectory: "/tmp",
		Status:           "running",
		StartTime:        time.Now(),
		OpenFiles:        make(map[string]*FileDescriptor),
		RecentAccesses:   make([]FileAccess, 0),
		AccessPatterns:   make(map[string]AccessPattern),
		RequiredGPUs:     make([]int, 0),
		GPUUsage:         make(map[int]float64),
		Dependencies:     make([]int, 0),
	}
	
	return processInfo, nil
}

// RequestResources requests resources for a process
func (wc *WorkloadCoordinator) RequestResources(pid int, resourceType string, amount uint64, deadline time.Time) error {
	wc.Lock()
	defer wc.Unlock()
	
	process, exists := wc.processes[pid]
	if !exists {
		return fmt.Errorf("process %d not registered", pid)
	}
	
	request := &ResourceRequest{
		PID:          pid,
		ResourceType: resourceType,
		Amount:       amount,
		Priority:     process.Priority,
		RequestTime:  time.Now(),
		Deadline:     deadline,
		Metadata:     make(map[string]interface{}),
	}
	
	// Try to allocate resources immediately
	if allocated, err := wc.allocateResources(request); err == nil && allocated {
		glog.V(2).Infof("Allocated %d %s to process %d", amount, resourceType, pid)
		return nil
	}
	
	// Add to waiting queue if immediate allocation failed
	pool := wc.resourcePools[resourceType]
	if pool != nil {
		pool.Lock()
		pool.WaitingQueue = append(pool.WaitingQueue, request)
		pool.Unlock()
		
		glog.V(2).Infof("Added resource request to queue: PID=%d, type=%s, amount=%d", pid, resourceType, amount)
	}
	
	return nil
}

// allocateResources attempts to allocate resources for a request
func (wc *WorkloadCoordinator) allocateResources(request *ResourceRequest) (bool, error) {
	pool := wc.resourcePools[request.ResourceType]
	if pool == nil {
		return false, fmt.Errorf("unknown resource type: %s", request.ResourceType)
	}
	
	pool.Lock()
	defer pool.Unlock()
	
	// Check if resources are available
	if pool.AvailableCapacity < request.Amount {
		return false, nil
	}
	
	// Allocate resources
	pool.AvailableCapacity -= request.Amount
	pool.Allocations[request.PID] = request.Amount
	
	// Create resource allocation record
	allocation := &ResourceAllocation{
		PID:            request.PID,
		Allocations:    map[string]uint64{request.ResourceType: request.Amount},
		AllocationTime: time.Now(),
		ExpirationTime: time.Now().Add(pool.ReservationTime),
		Priority:       request.Priority,
		Renewable:      true,
	}
	
	wc.resourceAllocations[request.PID] = allocation
	
	return true, nil
}

// RecordFileAccess records a file access for process coordination
func (wc *WorkloadCoordinator) RecordFileAccess(pid int, filePath string, operation string, offset int64, size int, duration time.Duration) {
	wc.RLock()
	process := wc.processes[pid]
	wc.RUnlock()
	
	if process == nil {
		return
	}
	
	process.Lock()
	defer process.Unlock()
	
	// Record file access
	access := FileAccess{
		Timestamp: time.Now(),
		FilePath:  filePath,
		Operation: operation,
		Offset:    offset,
		Size:      size,
		Duration:  duration,
	}
	
	process.RecentAccesses = append(process.RecentAccesses, access)
	
	// Keep only recent accesses (last 1000)
	if len(process.RecentAccesses) > 1000 {
		process.RecentAccesses = process.RecentAccesses[len(process.RecentAccesses)-500:]
	}
	
	// Update access patterns
	wc.updateAccessPattern(process, filePath, operation, offset, size)
	
	// Update workload metrics
	if metrics, exists := wc.workloadMetrics[pid]; exists {
		metrics.FileOperations++
		if operation == "read" {
			metrics.TotalBytesRead += uint64(size)
		} else if operation == "write" {
			metrics.TotalBytesWritten += uint64(size)
		}
	}
}

// updateAccessPattern updates access patterns for a process
func (wc *WorkloadCoordinator) updateAccessPattern(process *ProcessInfo, filePath, operation string, offset int64, size int) {
	// Simple pattern detection - could be enhanced
	currentPattern := process.AccessPatterns[filePath]
	
	if operation == "read" {
		if size > 64*1024 {
			process.AccessPatterns[filePath] = SequentialAccess
		} else {
			process.AccessPatterns[filePath] = RandomAccess
		}
	}
	
	// Update if pattern has changed
	if currentPattern != process.AccessPatterns[filePath] {
		glog.V(4).Infof("Updated access pattern for %s: %v -> %v", filePath, currentPattern, process.AccessPatterns[filePath])
	}
}

// OptimizeWorkloadCoordination provides coordination recommendations
func (wc *WorkloadCoordinator) OptimizeWorkloadCoordination(pid int) *WorkloadCoordinationOptimization {
	wc.RLock()
	process := wc.processes[pid]
	systemMetrics := wc.systemMetrics
	wc.RUnlock()
	
	if process == nil {
		return &WorkloadCoordinationOptimization{
			ShouldThrottle: false,
			Priority:       PriorityNormal,
		}
	}
	
	process.RLock()
	defer process.RUnlock()
	systemMetrics.RLock()
	defer systemMetrics.RUnlock()
	
	optimization := &WorkloadCoordinationOptimization{
		PID:               pid,
		ShouldThrottle:    false,
		Priority:          process.Priority,
		RecommendedAction: "continue",
		Recommendations:   make([]string, 0),
	}
	
	// Check system load
	if systemMetrics.CPUUsage > 90.0 {
		optimization.ShouldThrottle = true
		optimization.RecommendedAction = "throttle"
		optimization.Recommendations = append(optimization.Recommendations, "High CPU usage detected - consider throttling")
	}
	
	// Check memory pressure
	memoryUsagePercent := float64(systemMetrics.MemoryUsage) / float64(systemMetrics.TotalMemory) * 100
	if memoryUsagePercent > 85.0 {
		optimization.Recommendations = append(optimization.Recommendations, "High memory usage - consider freeing cache")
	}
	
	// Check I/O patterns
	for filePath, pattern := range process.AccessPatterns {
		if pattern == RandomAccess {
			optimization.Recommendations = append(optimization.Recommendations, 
				fmt.Sprintf("Random access pattern detected for %s - consider data locality optimization", filePath))
		}
	}
	
	// Check for potential conflicts
	conflicts := wc.detectResourceConflicts(pid)
	if len(conflicts) > 0 {
		optimization.RecommendedAction = "yield"
		optimization.Recommendations = append(optimization.Recommendations, 
			fmt.Sprintf("Resource conflicts detected: %v", conflicts))
	}
	
	return optimization
}

// WorkloadCoordinationOptimization holds coordination optimization recommendations
type WorkloadCoordinationOptimization struct {
	PID               int                `json:"pid"`
	ShouldThrottle    bool               `json:"should_throttle"`
	Priority          WorkloadPriority   `json:"priority"`
	RecommendedAction string             `json:"recommended_action"` // continue, throttle, yield, migrate
	Recommendations   []string           `json:"recommendations"`
}

// detectResourceConflicts detects resource conflicts for a process
func (wc *WorkloadCoordinator) detectResourceConflicts(pid int) []string {
	conflicts := make([]string, 0)
	
	// Check for resource contention
	for resourceType, pool := range wc.resourcePools {
		pool.RLock()
		utilizationPercent := float64(pool.TotalCapacity-pool.AvailableCapacity) / float64(pool.TotalCapacity) * 100
		waitingCount := len(pool.WaitingQueue)
		pool.RUnlock()
		
		if utilizationPercent > 90.0 && waitingCount > 0 {
			conflicts = append(conflicts, fmt.Sprintf("%s_contention", resourceType))
		}
	}
	
	return conflicts
}

// Background task loops

func (wc *WorkloadCoordinator) processMonitorLoop() {
	ticker := time.NewTicker(wc.monitorInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-wc.ctx.Done():
			return
		case <-ticker.C:
			wc.monitorProcesses()
		case sig := <-wc.signalChan:
			glog.V(1).Infof("Received signal %v, shutting down workload coordinator", sig)
			wc.cancel()
			return
		}
	}
}

func (wc *WorkloadCoordinator) coordinationEventLoop() {
	for {
		select {
		case <-wc.ctx.Done():
			return
		case event := <-wc.coordinationChannel:
			wc.handleCoordinationEvent(event)
		case processEvent := <-wc.processEvents:
			wc.handleProcessEvent(processEvent)
		}
	}
}

func (wc *WorkloadCoordinator) systemMetricsLoop() {
	ticker := time.NewTicker(10 * time.Second) // Update system metrics every 10 seconds
	defer ticker.Stop()
	
	for {
		select {
		case <-wc.ctx.Done():
			return
		case <-ticker.C:
			wc.updateSystemMetrics()
		}
	}
}

func (wc *WorkloadCoordinator) resourceManagerLoop() {
	ticker := time.NewTicker(30 * time.Second) // Manage resources every 30 seconds
	defer ticker.Stop()
	
	for {
		select {
		case <-wc.ctx.Done():
			return
		case <-ticker.C:
			wc.manageResources()
		}
	}
}

// Background task implementations

func (wc *WorkloadCoordinator) monitorProcesses() {
	wc.Lock()
	defer wc.Unlock()
	
	now := time.Now()
	toRemove := make([]int, 0)
	
	for pid, process := range wc.processes {
		process.Lock()
		
		// Check if process is still alive
		if now.Sub(process.LastHeartbeat) > wc.heartbeatTimeout {
			toRemove = append(toRemove, pid)
		} else {
			// Update process metrics
			wc.updateProcessMetrics(pid, process)
		}
		
		process.Unlock()
	}
	
	// Remove dead processes
	for _, pid := range toRemove {
		wc.removeProcess(pid)
	}
	
	wc.activeWorkloads = int64(len(wc.processes))
}

func (wc *WorkloadCoordinator) updateProcessMetrics(pid int, process *ProcessInfo) {
	// In a real implementation, this would query system metrics
	// For now, we'll update with placeholder values
	
	if metrics, exists := wc.workloadMetrics[pid]; exists {
		metrics.Runtime = time.Since(metrics.StartTime)
		// Would update with real CPU time, memory usage, etc.
	}
}

func (wc *WorkloadCoordinator) removeProcess(pid int) {
	delete(wc.processes, pid)
	
	// Release allocated resources
	if allocation, exists := wc.resourceAllocations[pid]; exists {
		for resourceType, amount := range allocation.Allocations {
			if pool, exists := wc.resourcePools[resourceType]; exists {
				pool.Lock()
				pool.AvailableCapacity += amount
				delete(pool.Allocations, pid)
				pool.Unlock()
			}
		}
		delete(wc.resourceAllocations, pid)
	}
	
	glog.V(2).Infof("Removed dead process: PID=%d", pid)
}

func (wc *WorkloadCoordinator) handleCoordinationEvent(event *CoordinationEvent) {
	wc.coordinationEvents++
	
	switch event.Type {
	case "resource_request":
		// Handle resource request
		glog.V(3).Infof("Handling resource request from PID %d", event.PID)
	case "process_priority_change":
		// Handle priority change
		if newPriority, ok := event.Data["priority"].(WorkloadPriority); ok {
			wc.updateProcessPriority(event.PID, newPriority)
		}
	default:
		glog.V(4).Infof("Unknown coordination event type: %s", event.Type)
	}
}

func (wc *WorkloadCoordinator) handleProcessEvent(event *ProcessEvent) {
	switch event.Type {
	case "process_registered":
		glog.V(3).Infof("Process %d registered for coordination", event.PID)
	case "process_exit":
		wc.Lock()
		wc.removeProcess(event.PID)
		wc.Unlock()
	default:
		glog.V(4).Infof("Unknown process event type: %s", event.Type)
	}
}

func (wc *WorkloadCoordinator) updateSystemMetrics() {
	wc.systemMetrics.Lock()
	defer wc.systemMetrics.Unlock()
	
	wc.systemMetrics.Timestamp = time.Now()
	wc.systemMetrics.ActiveProcesses = len(wc.processes)
	
	// In a real implementation, would gather actual system metrics
	// For now, using placeholder values
	wc.systemMetrics.CPUUsage = 45.0 + float64(len(wc.processes))*2.0
	wc.systemMetrics.MemoryUsage = uint64(len(wc.processes)) * 100 * 1024 * 1024 // 100MB per process
}

func (wc *WorkloadCoordinator) manageResources() {
	wc.Lock()
	defer wc.Unlock()
	
	// Process waiting queues for each resource pool
	for resourceType, pool := range wc.resourcePools {
		pool.Lock()
		
		newQueue := make([]*ResourceRequest, 0)
		for _, request := range pool.WaitingQueue {
			// Try to allocate resources
			if allocated, _ := wc.allocateResources(request); !allocated {
				// Check if request has expired
				if time.Since(request.RequestTime) < 10*time.Minute {
					newQueue = append(newQueue, request)
				}
			}
		}
		
		pool.WaitingQueue = newQueue
		pool.Unlock()
		
		glog.V(4).Infof("Processed resource queue for %s: %d requests remaining", resourceType, len(newQueue))
	}
	
	// Check for expired resource allocations
	wc.checkExpiredAllocations()
}

func (wc *WorkloadCoordinator) checkExpiredAllocations() {
	now := time.Now()
	
	for pid, allocation := range wc.resourceAllocations {
		if now.After(allocation.ExpirationTime) {
			// Release expired allocations
			for resourceType, amount := range allocation.Allocations {
				if pool, exists := wc.resourcePools[resourceType]; exists {
					pool.Lock()
					pool.AvailableCapacity += amount
					delete(pool.Allocations, pid)
					pool.Unlock()
				}
			}
			delete(wc.resourceAllocations, pid)
			
			glog.V(2).Infof("Released expired resource allocation for PID %d", pid)
		}
	}
}

func (wc *WorkloadCoordinator) updateProcessPriority(pid int, newPriority WorkloadPriority) {
	wc.Lock()
	defer wc.Unlock()
	
	if process, exists := wc.processes[pid]; exists {
		process.Lock()
		oldPriority := process.Priority
		process.Priority = newPriority
		process.Unlock()
		
		glog.V(2).Infof("Updated process priority: PID=%d, %v -> %v", pid, oldPriority, newPriority)
	}
}

// GetCoordinationMetrics returns comprehensive coordination metrics
func (wc *WorkloadCoordinator) GetCoordinationMetrics() WorkloadCoordinationMetrics {
	wc.RLock()
	defer wc.RUnlock()
	
	metrics := WorkloadCoordinationMetrics{
		TotalProcesses:       wc.totalProcesses,
		ActiveWorkloads:      wc.activeWorkloads,
		CoordinationEvents:   wc.coordinationEvents,
		ResourceConflicts:    wc.resourceConflicts,
		WorkloadsByType:      make(map[WorkloadType]int64),
		WorkloadsByPriority:  make(map[WorkloadPriority]int64),
		ResourceUtilization:  make(map[string]float64),
	}
	
	// Count workloads by type and priority
	for _, process := range wc.processes {
		process.RLock()
		metrics.WorkloadsByType[process.WorkloadType]++
		metrics.WorkloadsByPriority[process.Priority]++
		process.RUnlock()
	}
	
	// Calculate resource utilization
	for resourceType, pool := range wc.resourcePools {
		pool.RLock()
		utilization := float64(pool.TotalCapacity-pool.AvailableCapacity) / float64(pool.TotalCapacity) * 100
		metrics.ResourceUtilization[resourceType] = utilization
		pool.RUnlock()
	}
	
	return metrics
}

// WorkloadCoordinationMetrics holds metrics for workload coordination
type WorkloadCoordinationMetrics struct {
	TotalProcesses      int64                              `json:"total_processes"`
	ActiveWorkloads     int64                              `json:"active_workloads"`
	CoordinationEvents  int64                              `json:"coordination_events"`
	ResourceConflicts   int64                              `json:"resource_conflicts"`
	WorkloadsByType     map[WorkloadType]int64             `json:"workloads_by_type"`
	WorkloadsByPriority map[WorkloadPriority]int64         `json:"workloads_by_priority"`
	ResourceUtilization map[string]float64                 `json:"resource_utilization"`
}

// Shutdown gracefully shuts down the workload coordinator
func (wc *WorkloadCoordinator) Shutdown() {
	if wc.cancel != nil {
		wc.cancel()
	}
	
	// Close channels
	close(wc.coordinationChannel)
	close(wc.processEvents)
	
	glog.V(1).Infof("Workload coordinator shutdown complete")
}

// String methods for enums

func (wt WorkloadType) String() string {
	switch wt {
	case WorkloadTypeTraining:
		return "Training"
	case WorkloadTypeInference:
		return "Inference"
	case WorkloadTypeDataPreprocessing:
		return "DataPreprocessing"
	case WorkloadTypeFeatureEngineering:
		return "FeatureEngineering"
	case WorkloadTypeModelValidation:
		return "ModelValidation"
	case WorkloadTypeHyperparameterTuning:
		return "HyperparameterTuning"
	case WorkloadTypeAutoML:
		return "AutoML"
	case WorkloadTypeModelServing:
		return "ModelServing"
	default:
		return "Unknown"
	}
}

func (wp WorkloadPriority) String() string {
	switch wp {
	case PriorityLow:
		return "Low"
	case PriorityNormal:
		return "Normal"
	case PriorityHigh:
		return "High"
	case PriorityUrgent:
		return "Urgent"
	case PriorityCritical:
		return "Critical"
	default:
		return "Normal"
	}
}
