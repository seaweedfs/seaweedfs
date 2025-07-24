package task

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VolumeStateTracker tracks volume state changes and reconciles with master
type VolumeStateTracker struct {
	masterClient      *wdclient.MasterClient
	reconcileInterval time.Duration
	reservedVolumes   map[uint32]*VolumeReservation
	pendingChanges    map[uint32]*VolumeChange
	mutex             sync.RWMutex
}

// NewVolumeStateTracker creates a new volume state tracker
func NewVolumeStateTracker(masterClient *wdclient.MasterClient, reconcileInterval time.Duration) *VolumeStateTracker {
	return &VolumeStateTracker{
		masterClient:      masterClient,
		reconcileInterval: reconcileInterval,
		reservedVolumes:   make(map[uint32]*VolumeReservation),
		pendingChanges:    make(map[uint32]*VolumeChange),
	}
}

// ReserveVolume reserves a volume for a task
func (vst *VolumeStateTracker) ReserveVolume(volumeID uint32, taskID string) {
	vst.mutex.Lock()
	defer vst.mutex.Unlock()

	reservation := &VolumeReservation{
		VolumeID:      volumeID,
		TaskID:        taskID,
		ReservedAt:    time.Now(),
		ExpectedEnd:   time.Now().Add(15 * time.Minute), // Default 15 min estimate
		CapacityDelta: 0,                                // Will be updated based on task type
	}

	vst.reservedVolumes[volumeID] = reservation
	glog.V(2).Infof("Reserved volume %d for task %s", volumeID, taskID)
}

// ReleaseVolume releases a volume reservation
func (vst *VolumeStateTracker) ReleaseVolume(volumeID uint32, taskID string) {
	vst.mutex.Lock()
	defer vst.mutex.Unlock()

	if reservation, exists := vst.reservedVolumes[volumeID]; exists {
		if reservation.TaskID == taskID {
			delete(vst.reservedVolumes, volumeID)
			glog.V(2).Infof("Released volume %d reservation for task %s", volumeID, taskID)
		}
	}
}

// RecordVolumeChange records a completed volume change
func (vst *VolumeStateTracker) RecordVolumeChange(volumeID uint32, taskType types.TaskType, taskID string) {
	vst.mutex.Lock()
	defer vst.mutex.Unlock()

	changeType := ChangeTypeECEncoding
	if taskType == types.TaskTypeVacuum {
		changeType = ChangeTypeVacuumComplete
	}

	change := &VolumeChange{
		VolumeID:         volumeID,
		ChangeType:       changeType,
		TaskID:           taskID,
		CompletedAt:      time.Now(),
		ReportedToMaster: false,
	}

	vst.pendingChanges[volumeID] = change
	glog.V(1).Infof("Recorded volume change for volume %d: %s", volumeID, changeType)
}

// GetPendingChange returns pending change for a volume
func (vst *VolumeStateTracker) GetPendingChange(volumeID uint32) *VolumeChange {
	vst.mutex.RLock()
	defer vst.mutex.RUnlock()

	return vst.pendingChanges[volumeID]
}

// GetVolumeReservation returns reservation for a volume
func (vst *VolumeStateTracker) GetVolumeReservation(volumeID uint32) *VolumeReservation {
	vst.mutex.RLock()
	defer vst.mutex.RUnlock()

	return vst.reservedVolumes[volumeID]
}

// IsVolumeReserved checks if a volume is reserved
func (vst *VolumeStateTracker) IsVolumeReserved(volumeID uint32) bool {
	vst.mutex.RLock()
	defer vst.mutex.RUnlock()

	_, exists := vst.reservedVolumes[volumeID]
	return exists
}

// ReconcileWithMaster reconciles volume states with master server
func (vst *VolumeStateTracker) ReconcileWithMaster() {
	vst.mutex.Lock()
	defer vst.mutex.Unlock()

	// Report pending changes to master
	for volumeID, change := range vst.pendingChanges {
		if vst.reportChangeToMaster(change) {
			change.ReportedToMaster = true
			delete(vst.pendingChanges, volumeID)
			glog.V(1).Infof("Successfully reported volume change for volume %d to master", volumeID)
		}
	}

	// Clean up expired reservations
	vst.cleanupExpiredReservations()
}

// reportChangeToMaster reports a volume change to the master server
func (vst *VolumeStateTracker) reportChangeToMaster(change *VolumeChange) bool {
	// Note: In a real implementation, this would make actual API calls to master
	// For now, we'll simulate the reporting

	switch change.ChangeType {
	case ChangeTypeECEncoding:
		return vst.reportECCompletion(change)
	case ChangeTypeVacuumComplete:
		return vst.reportVacuumCompletion(change)
	}

	return false
}

// reportECCompletion reports EC completion to master
func (vst *VolumeStateTracker) reportECCompletion(change *VolumeChange) bool {
	// This would typically trigger the master to:
	// 1. Update volume state to reflect EC encoding
	// 2. Update capacity calculations
	// 3. Redistribute volume assignments

	glog.V(2).Infof("Reporting EC completion for volume %d", change.VolumeID)

	// Simulate master API call
	err := vst.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		// In real implementation, there would be a specific API call here
		// For now, we simulate success
		return nil
	})

	return err == nil
}

// reportVacuumCompletion reports vacuum completion to master
func (vst *VolumeStateTracker) reportVacuumCompletion(change *VolumeChange) bool {
	// This would typically trigger the master to:
	// 1. Update volume statistics
	// 2. Update capacity calculations
	// 3. Mark volume as recently vacuumed

	glog.V(2).Infof("Reporting vacuum completion for volume %d", change.VolumeID)

	// Simulate master API call
	err := vst.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		// In real implementation, there would be a specific API call here
		// For now, we simulate success
		return nil
	})

	return err == nil
}

// cleanupExpiredReservations removes expired volume reservations
func (vst *VolumeStateTracker) cleanupExpiredReservations() {
	now := time.Now()

	for volumeID, reservation := range vst.reservedVolumes {
		if now.After(reservation.ExpectedEnd) {
			delete(vst.reservedVolumes, volumeID)
			glog.Warningf("Cleaned up expired reservation for volume %d (task %s)", volumeID, reservation.TaskID)
		}
	}
}

// GetAdjustedCapacity returns adjusted capacity considering in-progress tasks
func (vst *VolumeStateTracker) GetAdjustedCapacity(volumeID uint32, baseCapacity int64) int64 {
	vst.mutex.RLock()
	defer vst.mutex.RUnlock()

	// Check for pending changes
	if change := vst.pendingChanges[volumeID]; change != nil {
		return change.NewCapacity
	}

	// Check for in-progress reservations
	if reservation := vst.reservedVolumes[volumeID]; reservation != nil {
		return baseCapacity + reservation.CapacityDelta
	}

	return baseCapacity
}

// GetStats returns statistics about volume state tracking
func (vst *VolumeStateTracker) GetStats() map[string]interface{} {
	vst.mutex.RLock()
	defer vst.mutex.RUnlock()

	stats := make(map[string]interface{})
	stats["reserved_volumes"] = len(vst.reservedVolumes)
	stats["pending_changes"] = len(vst.pendingChanges)

	changeTypeCounts := make(map[ChangeType]int)
	for _, change := range vst.pendingChanges {
		changeTypeCounts[change.ChangeType]++
	}
	stats["pending_by_type"] = changeTypeCounts

	return stats
}
