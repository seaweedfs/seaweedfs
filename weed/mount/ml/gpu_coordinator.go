package ml

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// GPUMemoryInfo represents GPU memory information
type GPUMemoryInfo struct {
	DeviceID     int    `json:"device_id"`
	DeviceName   string `json:"device_name"`
	TotalMemory  uint64 `json:"total_memory"`  // Total memory in bytes
	UsedMemory   uint64 `json:"used_memory"`   // Used memory in bytes  
	FreeMemory   uint64 `json:"free_memory"`   // Free memory in bytes
	MemoryUtil   float64 `json:"memory_util"`  // Memory utilization percentage
	Temperature  int    `json:"temperature"`   // GPU temperature in Celsius
	PowerUsage   int    `json:"power_usage"`   // Power usage in watts
	UtilizationGPU int  `json:"util_gpu"`     // GPU utilization percentage
	ProcessCount int    `json:"process_count"` // Number of processes using GPU
}

// GPUProcessInfo represents a process using GPU
type GPUProcessInfo struct {
	PID         int    `json:"pid"`
	ProcessName string `json:"process_name"`
	MemoryUsage uint64 `json:"memory_usage"` // Memory used by process in bytes
	DeviceID    int    `json:"device_id"`
}

// GPUCoordinator manages GPU memory awareness and coordination with file I/O
type GPUCoordinator struct {
	sync.RWMutex
	
	// Configuration
	enabled                bool          // Whether GPU coordination is enabled
	monitorInterval        time.Duration // How often to poll GPU status
	memoryThreshold        float64       // Memory usage threshold to trigger coordination
	temperatureThreshold   int           // Temperature threshold in Celsius
	
	// GPU state
	gpus                   map[int]*GPUMemoryInfo    // GPU device info by ID
	processes              map[int]*GPUProcessInfo   // GPU processes by PID
	lastUpdate             time.Time                 // When GPU info was last updated
	
	// Coordination state
	activeWorkloads        map[string]*MLWorkload    // Active ML workloads
	pendingTransfers       map[string]*DataTransfer  // Pending data transfers
	coordinationRules      []*CoordinationRule       // Rules for GPU-storage coordination
	
	// Background monitoring
	ctx                    context.Context
	cancel                 context.CancelFunc
	
	// Metrics
	totalCoordinationEvents int64    // Total coordination events
	memoryPressureEvents    int64    // Events triggered by memory pressure
	temperatureLimitEvents  int64    // Events triggered by temperature limits
	coordinationMisses      int64    // Failed coordination attempts
}

// MLWorkload represents an active ML workload using GPU resources
type MLWorkload struct {
	sync.RWMutex
	
	WorkloadID       string    `json:"workload_id"`
	ProcessPID       int       `json:"process_pid"`
	GPUDevices       []int     `json:"gpu_devices"`    // GPU devices used
	MemoryFootprint  uint64    `json:"memory_footprint"` // Expected memory usage
	Priority         int       `json:"priority"`       // Workload priority (higher = more important)
	StartTime        time.Time `json:"start_time"`
	LastActivity     time.Time `json:"last_activity"`
	
	// Data access patterns
	DatasetFiles     []string  `json:"dataset_files"`  // Dataset files being accessed
	ModelFiles       []string  `json:"model_files"`    // Model files being accessed
	AccessPattern    string    `json:"access_pattern"` // Sequential, Random, etc.
	
	// Performance characteristics
	IOThroughput     float64   `json:"io_throughput"`  // MB/s
	BatchSize        int       `json:"batch_size"`
	EpochTime        time.Duration `json:"epoch_time"`
}

// DataTransfer represents a coordinated data transfer
type DataTransfer struct {
	TransferID      string    `json:"transfer_id"`
	SourcePath      string    `json:"source_path"`
	Size            uint64    `json:"size"`
	Priority        int       `json:"priority"`
	ScheduledTime   time.Time `json:"scheduled_time"`
	ExpectedDuration time.Duration `json:"expected_duration"`
	WorkloadID      string    `json:"workload_id"`
}

// CoordinationRule defines rules for coordinating GPU memory and storage I/O
type CoordinationRule struct {
	Name            string    `json:"name"`
	Condition       string    `json:"condition"`       // GPU memory > 80%, temp > 85, etc.
	Action          string    `json:"action"`          // reduce_prefetch, delay_transfer, etc.
	Parameters      map[string]interface{} `json:"parameters"`
	Priority        int       `json:"priority"`
	Enabled         bool      `json:"enabled"`
}

// NewGPUCoordinator creates a new GPU coordinator
func NewGPUCoordinator(enabled bool) *GPUCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	
	gc := &GPUCoordinator{
		enabled:             enabled,
		monitorInterval:     5 * time.Second,  // Poll every 5 seconds
		memoryThreshold:     80.0,             // 80% memory usage threshold
		temperatureThreshold: 85,              // 85°C temperature threshold
		
		gpus:                make(map[int]*GPUMemoryInfo),
		processes:           make(map[int]*GPUProcessInfo),
		activeWorkloads:     make(map[string]*MLWorkload),
		pendingTransfers:    make(map[string]*DataTransfer),
		coordinationRules:   make([]*CoordinationRule, 0),
		
		ctx:                 ctx,
		cancel:              cancel,
	}
	
	// Initialize default coordination rules
	gc.initializeDefaultRules()
	
	if enabled {
		// Start GPU monitoring
		go gc.monitorGPUs()
		glog.V(1).Infof("GPU coordinator started with monitoring interval %v", gc.monitorInterval)
	}
	
	return gc
}

// initializeDefaultRules sets up default coordination rules
func (gc *GPUCoordinator) initializeDefaultRules() {
	// Rule 1: Reduce prefetching when GPU memory is high
	gc.coordinationRules = append(gc.coordinationRules, &CoordinationRule{
		Name:       "reduce_prefetch_on_memory_pressure",
		Condition:  "gpu_memory > 85",
		Action:     "reduce_prefetch",
		Parameters: map[string]interface{}{"reduction_factor": 0.5},
		Priority:   10,
		Enabled:    true,
	})
	
	// Rule 2: Delay data transfers when GPU is very hot
	gc.coordinationRules = append(gc.coordinationRules, &CoordinationRule{
		Name:       "delay_transfer_on_temperature",
		Condition:  "gpu_temperature > 87",
		Action:     "delay_transfer",
		Parameters: map[string]interface{}{"delay_seconds": 30},
		Priority:   20,
		Enabled:    true,
	})
	
	// Rule 3: Prioritize model files over dataset files during memory pressure
	gc.coordinationRules = append(gc.coordinationRules, &CoordinationRule{
		Name:       "prioritize_model_files",
		Condition:  "gpu_memory > 80 AND file_type == 'model'",
		Action:     "increase_priority",
		Parameters: map[string]interface{}{"priority_boost": 50},
		Priority:   15,
		Enabled:    true,
	})
	
	// Rule 4: Use staging area for large transfers during active training
	gc.coordinationRules = append(gc.coordinationRules, &CoordinationRule{
		Name:       "stage_large_transfers",
		Condition:  "active_training AND transfer_size > 100MB",
		Action:     "stage_transfer",
		Parameters: map[string]interface{}{"staging_threshold": 100 * 1024 * 1024},
		Priority:   5,
		Enabled:    true,
	})
}

// monitorGPUs continuously monitors GPU status
func (gc *GPUCoordinator) monitorGPUs() {
	ticker := time.NewTicker(gc.monitorInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-gc.ctx.Done():
			return
		case <-ticker.C:
			if err := gc.updateGPUStatus(); err != nil {
				glog.V(3).Infof("Failed to update GPU status: %v", err)
			} else {
				gc.evaluateCoordinationRules()
			}
		}
	}
}

// updateGPUStatus queries current GPU status using nvidia-ml-py or nvidia-smi
func (gc *GPUCoordinator) updateGPUStatus() error {
	gc.Lock()
	defer gc.Unlock()
	
	// Try nvidia-smi first (most common)
	if gpuInfo, err := gc.queryNvidiaSMI(); err == nil {
		for deviceID, info := range gpuInfo {
			gc.gpus[deviceID] = info
		}
		gc.lastUpdate = time.Now()
		return nil
	}
	
	// Could also try ROCm for AMD GPUs, Intel GPU tools, etc.
	// For now, we'll focus on NVIDIA GPUs which are most common in ML
	
	return fmt.Errorf("no GPU monitoring method available")
}

// queryNvidiaSMI queries GPU information using nvidia-smi
func (gc *GPUCoordinator) queryNvidiaSMI() (map[int]*GPUMemoryInfo, error) {
	cmd := exec.Command("nvidia-smi", 
		"--query-gpu=index,name,memory.total,memory.used,memory.free,utilization.memory,temperature.gpu,power.draw,utilization.gpu",
		"--format=csv,noheader,nounits")
	
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi failed: %w", err)
	}
	
	return gc.parseNvidiaSMIOutput(string(output))
}

// parseNvidiaSMIOutput parses nvidia-smi CSV output
func (gc *GPUCoordinator) parseNvidiaSMIOutput(output string) (map[int]*GPUMemoryInfo, error) {
	gpus := make(map[int]*GPUMemoryInfo)
	lines := strings.Split(strings.TrimSpace(output), "\n")
	
	for _, line := range lines {
		fields := strings.Split(line, ",")
		if len(fields) < 9 {
			continue
		}
		
		// Parse fields
		deviceID, _ := strconv.Atoi(strings.TrimSpace(fields[0]))
		deviceName := strings.TrimSpace(fields[1])
		totalMem, _ := strconv.ParseUint(strings.TrimSpace(fields[2]), 10, 64)
		usedMem, _ := strconv.ParseUint(strings.TrimSpace(fields[3]), 10, 64)
		freeMem, _ := strconv.ParseUint(strings.TrimSpace(fields[4]), 10, 64)
		memUtil, _ := strconv.ParseFloat(strings.TrimSpace(fields[5]), 64)
		temp, _ := strconv.Atoi(strings.TrimSpace(fields[6]))
		power, _ := strconv.Atoi(strings.TrimSpace(fields[7]))
		gpuUtil, _ := strconv.Atoi(strings.TrimSpace(fields[8]))
		
		gpus[deviceID] = &GPUMemoryInfo{
			DeviceID:       deviceID,
			DeviceName:     deviceName,
			TotalMemory:    totalMem * 1024 * 1024, // Convert MB to bytes
			UsedMemory:     usedMem * 1024 * 1024,
			FreeMemory:     freeMem * 1024 * 1024,
			MemoryUtil:     memUtil,
			Temperature:    temp,
			PowerUsage:     power,
			UtilizationGPU: gpuUtil,
		}
	}
	
	return gpus, nil
}

// evaluateCoordinationRules evaluates all coordination rules and takes actions
func (gc *GPUCoordinator) evaluateCoordinationRules() {
	gc.RLock()
	defer gc.RUnlock()
	
	for _, rule := range gc.coordinationRules {
		if !rule.Enabled {
			continue
		}
		
		if gc.evaluateCondition(rule.Condition) {
			gc.executeAction(rule)
			gc.totalCoordinationEvents++
		}
	}
}

// evaluateCondition evaluates a rule condition against current GPU state
func (gc *GPUCoordinator) evaluateCondition(condition string) bool {
	// Simple condition evaluation - in production, this could use a proper expression parser
	for _, gpu := range gc.gpus {
		// Check memory pressure conditions
		if strings.Contains(condition, "gpu_memory >") {
			re := regexp.MustCompile(`gpu_memory > (\d+)`)
			if matches := re.FindStringSubmatch(condition); len(matches) > 1 {
				threshold, _ := strconv.ParseFloat(matches[1], 64)
				if gpu.MemoryUtil > threshold {
					gc.memoryPressureEvents++
					return true
				}
			}
		}
		
		// Check temperature conditions  
		if strings.Contains(condition, "gpu_temperature >") {
			re := regexp.MustCompile(`gpu_temperature > (\d+)`)
			if matches := re.FindStringSubmatch(condition); len(matches) > 1 {
				threshold, _ := strconv.Atoi(matches[1])
				if gpu.Temperature > threshold {
					gc.temperatureLimitEvents++
					return true
				}
			}
		}
	}
	
	return false
}

// executeAction executes a coordination action
func (gc *GPUCoordinator) executeAction(rule *CoordinationRule) {
	switch rule.Action {
	case "reduce_prefetch":
		gc.reducePrefetching(rule.Parameters)
	case "delay_transfer":
		gc.delayTransfers(rule.Parameters)  
	case "increase_priority":
		gc.increasePriority(rule.Parameters)
	case "stage_transfer":
		gc.stageTransfers(rule.Parameters)
	default:
		glog.V(3).Infof("Unknown coordination action: %s", rule.Action)
	}
	
	glog.V(2).Infof("Executed coordination rule: %s -> %s", rule.Name, rule.Action)
}

// reducePrefetching reduces prefetch activity to free up I/O bandwidth
func (gc *GPUCoordinator) reducePrefetching(params map[string]interface{}) {
	// This would integrate with the existing prefetch manager
	// to reduce prefetch queue size or worker count temporarily
	glog.V(3).Infof("Reducing prefetch activity due to GPU memory pressure")
}

// delayTransfers delays pending data transfers
func (gc *GPUCoordinator) delayTransfers(params map[string]interface{}) {
	if delaySeconds, ok := params["delay_seconds"].(float64); ok {
		delay := time.Duration(delaySeconds) * time.Second
		
		for transferID, transfer := range gc.pendingTransfers {
			transfer.ScheduledTime = transfer.ScheduledTime.Add(delay)
			glog.V(3).Infof("Delayed transfer %s by %v due to GPU temperature", transferID, delay)
		}
	}
}

// increasePriority increases priority for certain file types
func (gc *GPUCoordinator) increasePriority(params map[string]interface{}) {
	glog.V(3).Infof("Increasing priority for model files during memory pressure")
}

// stageTransfers uses staging area for large transfers
func (gc *GPUCoordinator) stageTransfers(params map[string]interface{}) {
	glog.V(3).Infof("Using staging area for large transfers during active training")
}

// RegisterWorkload registers a new ML workload
func (gc *GPUCoordinator) RegisterWorkload(workload *MLWorkload) {
	gc.Lock()
	defer gc.Unlock()
	
	gc.activeWorkloads[workload.WorkloadID] = workload
	glog.V(2).Infof("Registered GPU workload: %s on devices %v", workload.WorkloadID, workload.GPUDevices)
}

// UnregisterWorkload removes a workload
func (gc *GPUCoordinator) UnregisterWorkload(workloadID string) {
	gc.Lock()
	defer gc.Unlock()
	
	delete(gc.activeWorkloads, workloadID)
	glog.V(2).Infof("Unregistered GPU workload: %s", workloadID)
}

// ScheduleDataTransfer schedules a data transfer considering GPU state
func (gc *GPUCoordinator) ScheduleDataTransfer(transfer *DataTransfer) {
	gc.Lock()
	defer gc.Unlock()
	
	// Consider current GPU memory pressure and temperature
	schedulingDelay := time.Duration(0)
	
	for _, gpu := range gc.gpus {
		if gpu.MemoryUtil > gc.memoryThreshold {
			// Delay transfers when GPU memory is under pressure
			schedulingDelay = time.Duration(30) * time.Second
			break
		}
		
		if gpu.Temperature > gc.temperatureThreshold {
			// Delay transfers when GPU is running hot
			schedulingDelay = time.Duration(60) * time.Second
			break
		}
	}
	
	transfer.ScheduledTime = time.Now().Add(schedulingDelay)
	gc.pendingTransfers[transfer.TransferID] = transfer
	
	glog.V(2).Infof("Scheduled data transfer %s (size: %d bytes, delay: %v)", 
		transfer.TransferID, transfer.Size, schedulingDelay)
}

// GetGPUStatus returns current GPU status
func (gc *GPUCoordinator) GetGPUStatus() map[int]*GPUMemoryInfo {
	gc.RLock()
	defer gc.RUnlock()
	
	// Return a copy to avoid race conditions
	status := make(map[int]*GPUMemoryInfo)
	for id, info := range gc.gpus {
		statusCopy := *info
		status[id] = &statusCopy
	}
	
	return status
}

// GetCoordinationMetrics returns coordination metrics
func (gc *GPUCoordinator) GetCoordinationMetrics() GPUCoordinationMetrics {
	gc.RLock()
	defer gc.RUnlock()
	
	return GPUCoordinationMetrics{
		TotalGPUs:               len(gc.gpus),
		ActiveWorkloads:         len(gc.activeWorkloads),
		PendingTransfers:        len(gc.pendingTransfers),
		TotalCoordinationEvents: gc.totalCoordinationEvents,
		MemoryPressureEvents:    gc.memoryPressureEvents,
		TemperatureLimitEvents:  gc.temperatureLimitEvents,
		CoordinationMisses:      gc.coordinationMisses,
		LastGPUUpdate:          gc.lastUpdate,
	}
}

// GPUCoordinationMetrics holds metrics for GPU coordination
type GPUCoordinationMetrics struct {
	TotalGPUs               int       `json:"total_gpus"`
	ActiveWorkloads         int       `json:"active_workloads"`
	PendingTransfers        int       `json:"pending_transfers"`
	TotalCoordinationEvents int64     `json:"total_coordination_events"`
	MemoryPressureEvents    int64     `json:"memory_pressure_events"`
	TemperatureLimitEvents  int64     `json:"temperature_limit_events"`
	CoordinationMisses      int64     `json:"coordination_misses"`
	LastGPUUpdate          time.Time `json:"last_gpu_update"`
}

// ShouldReducePrefetch determines if prefetch should be reduced based on GPU state
func (gc *GPUCoordinator) ShouldReducePrefetch() (bool, float64) {
	gc.RLock()
	defer gc.RUnlock()
	
	if !gc.enabled {
		return false, 1.0
	}
	
	maxMemoryUtil := 0.0
	maxTemperature := 0
	
	for _, gpu := range gc.gpus {
		if gpu.MemoryUtil > maxMemoryUtil {
			maxMemoryUtil = gpu.MemoryUtil
		}
		if gpu.Temperature > maxTemperature {
			maxTemperature = gpu.Temperature
		}
	}
	
	// Reduce prefetch if GPU memory > 85% or temperature > 85°C
	if maxMemoryUtil > 85.0 || maxTemperature > 85 {
		// Reduction factor based on pressure level
		reductionFactor := 1.0
		if maxMemoryUtil > 90.0 {
			reductionFactor = 0.3 // Aggressive reduction
		} else if maxMemoryUtil > 85.0 {
			reductionFactor = 0.6 // Moderate reduction
		}
		
		return true, reductionFactor
	}
	
	return false, 1.0
}

// Shutdown gracefully shuts down the GPU coordinator
func (gc *GPUCoordinator) Shutdown() {
	if gc.cancel != nil {
		gc.cancel()
	}
	
	glog.V(1).Infof("GPU coordinator shutdown complete")
}

// Helper functions

func (gc *GPUCoordinator) IsEnabled() bool {
	gc.RLock()
	defer gc.RUnlock()
	return gc.enabled
}

func (gc *GPUCoordinator) SetEnabled(enabled bool) {
	gc.Lock()
	defer gc.Unlock()
	gc.enabled = enabled
}
