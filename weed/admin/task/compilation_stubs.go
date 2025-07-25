package task

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Compilation stubs for missing types and functions

// Task is an alias for types.Task for backward compatibility
type Task = types.Task

// TaskType is an alias for types.TaskType for backward compatibility
type TaskType = types.TaskType

// TaskStatus is an alias for types.TaskStatus for backward compatibility
type TaskStatus = types.TaskStatus

// TaskPriority is an alias for types.TaskPriority for backward compatibility
type TaskPriority = types.TaskPriority

// Additional type aliases for compilation
var (
	TaskStatusCompleted = types.TaskStatusCompleted
	TaskStatusFailed    = types.TaskStatusFailed
)

// Worker represents a worker node
type Worker struct {
	ID           string
	Address      string
	Capabilities []string
	Status       string
	LastSeen     time.Time
}

// convertAdminToWorkerMessage converts AdminMessage to WorkerMessage for stream compatibility
func convertAdminToWorkerMessage(msg *worker_pb.AdminMessage) *worker_pb.WorkerMessage {
	// This is a workaround for the stream type mismatch
	// In a real implementation, this would need proper message conversion
	return &worker_pb.WorkerMessage{
		WorkerId:  msg.AdminId,
		Timestamp: msg.Timestamp,
		// Add basic message conversion logic here
	}
}

// WorkerRegistry stub methods
func (wr *WorkerRegistry) UpdateWorkerStatus(workerID string, status interface{}) {
	// Stub implementation
}

// AdminServer stub methods
func (as *AdminServer) AssignTaskToWorker(workerID string) *Task {
	// Stub implementation
	return nil
}

// DefaultAdminConfig returns default admin server configuration
func DefaultAdminConfig() *AdminConfig {
	return &AdminConfig{
		ScanInterval:          30 * time.Minute,
		WorkerTimeout:         5 * time.Minute,
		TaskTimeout:           10 * time.Minute,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    10,
	}
}

// SyncWithMasterData is a stub for the volume state manager
func (vsm *VolumeStateManager) SyncWithMasterData(volumes map[uint32]*VolumeInfo, ecShards map[uint32]map[int]*ShardInfo, serverCapacity map[string]*CapacityInfo) error {
	// Stub implementation - would normally sync the data
	return nil
}

// GetAllVolumeStates is a stub for the volume state manager
func (vsm *VolumeStateManager) GetAllVolumeStates() map[uint32]*VolumeState {
	// Stub implementation - return empty map
	return make(map[uint32]*VolumeState)
}

// DetectInconsistencies is a stub for the volume state manager
func (vsm *VolumeStateManager) DetectInconsistencies() []StateInconsistency {
	// Stub implementation - return empty slice
	return []StateInconsistency{}
}
