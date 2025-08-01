package vacuum

import (
	"sync"
	"time"
)

// VacuumMetrics contains vacuum-specific monitoring data
type VacuumMetrics struct {
	// Execution metrics
	VolumesVacuumed       int64     `json:"volumes_vacuumed"`
	TotalSpaceReclaimed   int64     `json:"total_space_reclaimed"`
	TotalFilesProcessed   int64     `json:"total_files_processed"`
	TotalGarbageCollected int64     `json:"total_garbage_collected"`
	LastVacuumTime        time.Time `json:"last_vacuum_time"`

	// Performance metrics
	AverageVacuumTime    int64   `json:"average_vacuum_time_seconds"`
	AverageGarbageRatio  float64 `json:"average_garbage_ratio"`
	SuccessfulOperations int64   `json:"successful_operations"`
	FailedOperations     int64   `json:"failed_operations"`

	// Current task metrics
	CurrentGarbageRatio  float64 `json:"current_garbage_ratio"`
	VolumesPendingVacuum int     `json:"volumes_pending_vacuum"`

	mutex sync.RWMutex
}

// NewVacuumMetrics creates a new vacuum metrics instance
func NewVacuumMetrics() *VacuumMetrics {
	return &VacuumMetrics{
		LastVacuumTime: time.Now(),
	}
}

// RecordVolumeVacuumed records a successful volume vacuum operation
func (m *VacuumMetrics) RecordVolumeVacuumed(spaceReclaimed int64, filesProcessed int64, garbageCollected int64, vacuumTime time.Duration, garbageRatio float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.VolumesVacuumed++
	m.TotalSpaceReclaimed += spaceReclaimed
	m.TotalFilesProcessed += filesProcessed
	m.TotalGarbageCollected += garbageCollected
	m.SuccessfulOperations++
	m.LastVacuumTime = time.Now()

	// Update average vacuum time
	if m.AverageVacuumTime == 0 {
		m.AverageVacuumTime = int64(vacuumTime.Seconds())
	} else {
		// Exponential moving average
		newTime := int64(vacuumTime.Seconds())
		m.AverageVacuumTime = (m.AverageVacuumTime*4 + newTime) / 5
	}

	// Update average garbage ratio
	if m.AverageGarbageRatio == 0 {
		m.AverageGarbageRatio = garbageRatio
	} else {
		// Exponential moving average
		m.AverageGarbageRatio = 0.8*m.AverageGarbageRatio + 0.2*garbageRatio
	}
}

// RecordFailure records a failed vacuum operation
func (m *VacuumMetrics) RecordFailure() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.FailedOperations++
}

// UpdateCurrentGarbageRatio updates the current volume's garbage ratio
func (m *VacuumMetrics) UpdateCurrentGarbageRatio(ratio float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.CurrentGarbageRatio = ratio
}

// SetVolumesPendingVacuum sets the number of volumes pending vacuum
func (m *VacuumMetrics) SetVolumesPendingVacuum(count int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.VolumesPendingVacuum = count
}

// GetMetrics returns a copy of the current metrics (without the mutex)
func (m *VacuumMetrics) GetMetrics() VacuumMetrics {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Create a copy without the mutex to avoid copying lock value
	return VacuumMetrics{
		VolumesVacuumed:       m.VolumesVacuumed,
		TotalSpaceReclaimed:   m.TotalSpaceReclaimed,
		TotalFilesProcessed:   m.TotalFilesProcessed,
		TotalGarbageCollected: m.TotalGarbageCollected,
		LastVacuumTime:        m.LastVacuumTime,
		AverageVacuumTime:     m.AverageVacuumTime,
		AverageGarbageRatio:   m.AverageGarbageRatio,
		SuccessfulOperations:  m.SuccessfulOperations,
		FailedOperations:      m.FailedOperations,
		CurrentGarbageRatio:   m.CurrentGarbageRatio,
		VolumesPendingVacuum:  m.VolumesPendingVacuum,
	}
}

// GetSuccessRate returns the success rate as a percentage
func (m *VacuumMetrics) GetSuccessRate() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	total := m.SuccessfulOperations + m.FailedOperations
	if total == 0 {
		return 100.0
	}
	return float64(m.SuccessfulOperations) / float64(total) * 100.0
}

// GetAverageSpaceReclaimed returns the average space reclaimed per volume
func (m *VacuumMetrics) GetAverageSpaceReclaimed() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.VolumesVacuumed == 0 {
		return 0
	}
	return float64(m.TotalSpaceReclaimed) / float64(m.VolumesVacuumed)
}

// Reset resets all metrics to zero
func (m *VacuumMetrics) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	*m = VacuumMetrics{
		LastVacuumTime: time.Now(),
	}
}

// Global metrics instance for vacuum tasks
var globalVacuumMetrics = NewVacuumMetrics()

// GetGlobalVacuumMetrics returns the global vacuum metrics instance
func GetGlobalVacuumMetrics() *VacuumMetrics {
	return globalVacuumMetrics
}
