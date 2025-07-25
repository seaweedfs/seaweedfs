package task

import (
	"time"

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
