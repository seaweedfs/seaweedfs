package erasure_coding

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// ExecutionStatus tracks job execution status
type ExecutionStatus string

const (
	StatusMarking      ExecutionStatus = "marking"
	StatusCopying      ExecutionStatus = "copying"
	StatusGenerating   ExecutionStatus = "generating"
	StatusDistributing ExecutionStatus = "distributing"
	StatusMounting     ExecutionStatus = "mounting"
	StatusCleaning     ExecutionStatus = "cleaning"
	StatusCompleted    ExecutionStatus = "completed"
	StatusFailed       ExecutionStatus = "failed"
)

// ExecutionStep represents a step in the EC encoding pipeline
type ExecutionStep struct {
	Name       string
	Status     ExecutionStatus
	StartTime  *time.Time
	EndTime    *time.Time
	Progress   float32
	ErrorMsg   string
}

// Executor handles EC encoding execution
type Executor struct {
	config *ExecutorConfig
}

// ExecutorConfig contains executor configuration
type ExecutorConfig struct {
	StripeSize            int
	EncodeCopies          int
	RackAwareness         bool
	DataCenterAwareness   bool
	TimeoutPerStep        time.Duration
	MaxRetries            int
}

// NewExecutor creates a new EC executor
func NewExecutor(config *ExecutorConfig) *Executor {
	if config == nil {
		config = &ExecutorConfig{
			StripeSize:   10,
			EncodeCopies: 1,
			TimeoutPerStep: 5 * time.Minute,
			MaxRetries:   3,
		}
	}
	return &Executor{config: config}
}

// ExecutionResult contains the result of encoding
type ExecutionResult struct {
	VolumeID       uint32
	Success        bool
	StartTime      time.Time
	EndTime        time.Time
	TotalDuration  time.Duration
	BytesProcessed uint64
	StripeCount    int
	Metadata       map[string]string
	Steps          []*ExecutionStep
	ErrorMessage   string
}

// ExecuteJob executes the EC encoding for a volume
func (e *Executor) ExecuteJob(job *plugin_pb.ExecuteJobRequest) (*ExecutionResult, error) {
	result := &ExecutionResult{
		Success:      false,
		StartTime:    time.Now(),
		Metadata:     make(map[string]string),
		Steps:        make([]*ExecutionStep, 0),
	}

	// Extract volume ID from payload
	volumeID := extractVolumeID(job.Payload)
	result.VolumeID = volumeID

	// Step 1: Mark volume as being encoded
	if err := e.markVolume(result); err != nil {
		result.ErrorMessage = fmt.Sprintf("mark failed: %v", err)
		result.EndTime = time.Now()
		result.TotalDuration = result.EndTime.Sub(result.StartTime)
		return result, err
	}

	// Step 2: Copy volume data
	if err := e.copyVolumeData(result); err != nil {
		result.ErrorMessage = fmt.Sprintf("copy failed: %v", err)
		result.EndTime = time.Now()
		result.TotalDuration = result.EndTime.Sub(result.StartTime)
		return result, err
	}

	// Step 3: Generate parity shards
	if err := e.generateParityShards(result); err != nil {
		result.ErrorMessage = fmt.Sprintf("parity generation failed: %v", err)
		result.EndTime = time.Now()
		result.TotalDuration = result.EndTime.Sub(result.StartTime)
		return result, err
	}

	// Step 4: Distribute shards across nodes
	if err := e.distributeShards(result); err != nil {
		result.ErrorMessage = fmt.Sprintf("distribution failed: %v", err)
		result.EndTime = time.Now()
		result.TotalDuration = result.EndTime.Sub(result.StartTime)
		return result, err
	}

	// Step 5: Mount new EC volume
	if err := e.mountECVolume(result); err != nil {
		result.ErrorMessage = fmt.Sprintf("mount failed: %v", err)
		result.EndTime = time.Now()
		result.TotalDuration = result.EndTime.Sub(result.StartTime)
		return result, err
	}

	// Step 6: Delete original replicas
	if err := e.deleteOriginalReplicas(result); err != nil {
		result.ErrorMessage = fmt.Sprintf("cleanup failed: %v", err)
		result.EndTime = time.Now()
		result.TotalDuration = result.EndTime.Sub(result.StartTime)
		return result, err
	}

	result.Success = true
	result.EndTime = time.Now()
	result.TotalDuration = result.EndTime.Sub(result.StartTime)

	return result, nil
}

// markVolume marks the volume as being encoded
func (e *Executor) markVolume(result *ExecutionResult) error {
	step := &ExecutionStep{
		Name:     "marking",
		Status:   StatusMarking,
		Progress: 0,
	}
	now := time.Now()
	step.StartTime = &now

	// Simulate marking operation
	time.Sleep(50 * time.Millisecond)

	step.Progress = 100
	step.EndTime = &now
	result.Steps = append(result.Steps, step)

	return nil
}

// copyVolumeData copies volume data to temporary location
func (e *Executor) copyVolumeData(result *ExecutionResult) error {
	step := &ExecutionStep{
		Name:     "copying",
		Status:   StatusCopying,
		Progress: 0,
	}
	now := time.Now()
	step.StartTime = &now

	// Simulate data copying with progress
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		step.Progress = float32((i + 1) * 10)
	}

	step.Progress = 100
	step.EndTime = &now
	result.Steps = append(result.Steps, step)
	result.BytesProcessed += 1000000 // Simulate processing

	return nil
}

// generateParityShards generates parity shards from original data
func (e *Executor) generateParityShards(result *ExecutionResult) error {
	step := &ExecutionStep{
		Name:     "generating",
		Status:   StatusGenerating,
		Progress: 0,
	}
	now := time.Now()
	step.StartTime = &now

	// Simulate parity generation
	time.Sleep(100 * time.Millisecond)

	result.StripeCount = int(result.BytesProcessed / uint64(e.config.StripeSize*1024*1024))

	step.Progress = 100
	step.EndTime = &now
	result.Steps = append(result.Steps, step)

	return nil
}

// distributeShards distributes shards across data nodes
func (e *Executor) distributeShards(result *ExecutionResult) error {
	step := &ExecutionStep{
		Name:     "distributing",
		Status:   StatusDistributing,
		Progress: 0,
	}
	now := time.Now()
	step.StartTime = &now

	// Simulate shard distribution
	for i := 0; i < 5; i++ {
		time.Sleep(20 * time.Millisecond)
		step.Progress = float32((i + 1) * 20)
	}

	step.Progress = 100
	step.EndTime = &now
	result.Steps = append(result.Steps, step)

	return nil
}

// mountECVolume mounts the new EC volume
func (e *Executor) mountECVolume(result *ExecutionResult) error {
	step := &ExecutionStep{
		Name:     "mounting",
		Status:   StatusMounting,
		Progress: 0,
	}
	now := time.Now()
	step.StartTime = &now

	// Simulate mounting
	time.Sleep(50 * time.Millisecond)

	result.Metadata["ec_volume_id"] = fmt.Sprintf("%d.ec", result.VolumeID)
	result.Metadata["stripe_size"] = fmt.Sprintf("%d MB", e.config.StripeSize)
	result.Metadata["encode_copies"] = fmt.Sprintf("%d", e.config.EncodeCopies)

	step.Progress = 100
	step.EndTime = &now
	result.Steps = append(result.Steps, step)

	return nil
}

// deleteOriginalReplicas deletes the original replica volumes
func (e *Executor) deleteOriginalReplicas(result *ExecutionResult) error {
	step := &ExecutionStep{
		Name:     "cleaning",
		Status:   StatusCleaning,
		Progress: 0,
	}
	now := time.Now()
	step.StartTime = &now

	// Simulate cleanup
	time.Sleep(50 * time.Millisecond)

	step.Progress = 100
	step.EndTime = &now
	result.Steps = append(result.Steps, step)

	return nil
}

// extractVolumeID extracts the volume ID from job payload
func extractVolumeID(payload *plugin_pb.JobPayload) uint32 {
	if payload == nil || len(payload.Data) < 4 {
		return 0
	}
	// Simple extraction: first 4 bytes as little-endian uint32
	return uint32(payload.Data[0]) |
		(uint32(payload.Data[1]) << 8) |
		(uint32(payload.Data[2]) << 16) |
		(uint32(payload.Data[3]) << 24)
}

// ValidateExecutionResult validates the result of execution
func ValidateExecutionResult(result *ExecutionResult) bool {
	if !result.Success {
		return false
	}

	if result.EndTime.Before(result.StartTime) {
		return false
	}

	if len(result.Steps) != 6 {
		return false
	}

	return true
}
