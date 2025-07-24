package task

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// ECDetector detects volumes that need erasure coding
type ECDetector struct {
	minUtilization float64
	minIdleTime    time.Duration
}

// NewECDetector creates a new EC detector
func NewECDetector() *ECDetector {
	return &ECDetector{
		minUtilization: 95.0,      // 95% full
		minIdleTime:    time.Hour, // 1 hour idle
	}
}

// DetectECCandidates finds volumes that need erasure coding
func (ed *ECDetector) DetectECCandidates(volumes []*VolumeInfo) ([]*VolumeCandidate, error) {
	var candidates []*VolumeCandidate

	for _, vol := range volumes {
		if ed.isECCandidate(vol) {
			candidate := &VolumeCandidate{
				VolumeID:   vol.ID,
				Server:     vol.Server,
				Collection: vol.Collection,
				TaskType:   types.TaskTypeErasureCoding,
				Priority:   ed.calculateECPriority(vol),
				Reason:     "Volume is full and idle, ready for erasure coding",
				DetectedAt: time.Now(),
				ScheduleAt: time.Now(),
				Parameters: map[string]interface{}{
					"utilization": vol.GetUtilization(),
					"idle_time":   vol.GetIdleTime().String(),
					"volume_size": vol.Size,
				},
			}
			candidates = append(candidates, candidate)
		}
	}

	glog.V(2).Infof("EC detector found %d candidates", len(candidates))
	return candidates, nil
}

// isECCandidate checks if a volume is suitable for EC
func (ed *ECDetector) isECCandidate(vol *VolumeInfo) bool {
	// Skip if read-only
	if vol.ReadOnly {
		return false
	}

	// Skip if already has remote storage (likely already EC'd)
	if vol.RemoteStorageKey != "" {
		return false
	}

	// Check utilization
	if vol.GetUtilization() < ed.minUtilization {
		return false
	}

	// Check idle time
	if vol.GetIdleTime() < ed.minIdleTime {
		return false
	}

	return true
}

// calculateECPriority calculates priority for EC tasks
func (ed *ECDetector) calculateECPriority(vol *VolumeInfo) types.TaskPriority {
	utilization := vol.GetUtilization()
	idleTime := vol.GetIdleTime()

	// Higher priority for fuller volumes that have been idle longer
	if utilization >= 98.0 && idleTime > 24*time.Hour {
		return types.TaskPriorityHigh
	}
	if utilization >= 96.0 && idleTime > 6*time.Hour {
		return types.TaskPriorityNormal
	}
	return types.TaskPriorityLow
}

// VacuumDetector detects volumes that need vacuum operations
type VacuumDetector struct {
	minGarbageRatio float64
	minDeleteCount  uint64
}

// NewVacuumDetector creates a new vacuum detector
func NewVacuumDetector() *VacuumDetector {
	return &VacuumDetector{
		minGarbageRatio: 0.3, // 30% garbage
		minDeleteCount:  100, // At least 100 deleted files
	}
}

// DetectVacuumCandidates finds volumes that need vacuum operations
func (vd *VacuumDetector) DetectVacuumCandidates(volumes []*VolumeInfo) ([]*VolumeCandidate, error) {
	var candidates []*VolumeCandidate

	for _, vol := range volumes {
		if vd.isVacuumCandidate(vol) {
			candidate := &VolumeCandidate{
				VolumeID:   vol.ID,
				Server:     vol.Server,
				Collection: vol.Collection,
				TaskType:   types.TaskTypeVacuum,
				Priority:   vd.calculateVacuumPriority(vol),
				Reason:     "Volume has high garbage ratio and needs vacuum",
				DetectedAt: time.Now(),
				ScheduleAt: time.Now(),
				Parameters: map[string]interface{}{
					"garbage_ratio":      vol.GetGarbageRatio(),
					"delete_count":       vol.DeleteCount,
					"deleted_byte_count": vol.DeletedByteCount,
				},
			}
			candidates = append(candidates, candidate)
		}
	}

	glog.V(2).Infof("Vacuum detector found %d candidates", len(candidates))
	return candidates, nil
}

// isVacuumCandidate checks if a volume needs vacuum
func (vd *VacuumDetector) isVacuumCandidate(vol *VolumeInfo) bool {
	// Skip if read-only
	if vol.ReadOnly {
		return false
	}

	// Check garbage ratio
	if vol.GetGarbageRatio() < vd.minGarbageRatio {
		return false
	}

	// Check delete count
	if vol.DeleteCount < vd.minDeleteCount {
		return false
	}

	return true
}

// calculateVacuumPriority calculates priority for vacuum tasks
func (vd *VacuumDetector) calculateVacuumPriority(vol *VolumeInfo) types.TaskPriority {
	garbageRatio := vol.GetGarbageRatio()

	// Higher priority for volumes with more garbage
	if garbageRatio >= 0.6 {
		return types.TaskPriorityHigh
	}
	if garbageRatio >= 0.4 {
		return types.TaskPriorityNormal
	}
	return types.TaskPriorityLow
}
