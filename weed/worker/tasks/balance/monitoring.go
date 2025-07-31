package balance

import (
	"sync"
	"time"
)

// BalanceMetrics contains balance-specific monitoring data
type BalanceMetrics struct {
	// Execution metrics
	VolumesBalanced      int64     `json:"volumes_balanced"`
	TotalDataTransferred int64     `json:"total_data_transferred"`
	AverageImbalance     float64   `json:"average_imbalance"`
	LastBalanceTime      time.Time `json:"last_balance_time"`

	// Performance metrics
	AverageTransferSpeed float64 `json:"average_transfer_speed_mbps"`
	TotalExecutionTime   int64   `json:"total_execution_time_seconds"`
	SuccessfulOperations int64   `json:"successful_operations"`
	FailedOperations     int64   `json:"failed_operations"`

	// Current task metrics
	CurrentImbalanceScore float64 `json:"current_imbalance_score"`
	PlannedDestinations   int     `json:"planned_destinations"`

	mutex sync.RWMutex
}

// NewBalanceMetrics creates a new balance metrics instance
func NewBalanceMetrics() *BalanceMetrics {
	return &BalanceMetrics{
		LastBalanceTime: time.Now(),
	}
}

// RecordVolumeBalanced records a successful volume balance operation
func (m *BalanceMetrics) RecordVolumeBalanced(volumeSize int64, transferTime time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.VolumesBalanced++
	m.TotalDataTransferred += volumeSize
	m.SuccessfulOperations++
	m.LastBalanceTime = time.Now()
	m.TotalExecutionTime += int64(transferTime.Seconds())

	// Calculate average transfer speed (MB/s)
	if transferTime > 0 {
		speedMBps := float64(volumeSize) / (1024 * 1024) / transferTime.Seconds()
		if m.AverageTransferSpeed == 0 {
			m.AverageTransferSpeed = speedMBps
		} else {
			// Exponential moving average
			m.AverageTransferSpeed = 0.8*m.AverageTransferSpeed + 0.2*speedMBps
		}
	}
}

// RecordFailure records a failed balance operation
func (m *BalanceMetrics) RecordFailure() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.FailedOperations++
}

// UpdateImbalanceScore updates the current cluster imbalance score
func (m *BalanceMetrics) UpdateImbalanceScore(score float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.CurrentImbalanceScore = score

	// Update average imbalance with exponential moving average
	if m.AverageImbalance == 0 {
		m.AverageImbalance = score
	} else {
		m.AverageImbalance = 0.9*m.AverageImbalance + 0.1*score
	}
}

// SetPlannedDestinations sets the number of planned destinations
func (m *BalanceMetrics) SetPlannedDestinations(count int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.PlannedDestinations = count
}

// GetMetrics returns a copy of the current metrics (without the mutex)
func (m *BalanceMetrics) GetMetrics() BalanceMetrics {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Create a copy without the mutex to avoid copying lock value
	return BalanceMetrics{
		VolumesBalanced:       m.VolumesBalanced,
		TotalDataTransferred:  m.TotalDataTransferred,
		AverageImbalance:      m.AverageImbalance,
		LastBalanceTime:       m.LastBalanceTime,
		AverageTransferSpeed:  m.AverageTransferSpeed,
		TotalExecutionTime:    m.TotalExecutionTime,
		SuccessfulOperations:  m.SuccessfulOperations,
		FailedOperations:      m.FailedOperations,
		CurrentImbalanceScore: m.CurrentImbalanceScore,
		PlannedDestinations:   m.PlannedDestinations,
	}
}

// GetSuccessRate returns the success rate as a percentage
func (m *BalanceMetrics) GetSuccessRate() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	total := m.SuccessfulOperations + m.FailedOperations
	if total == 0 {
		return 100.0
	}
	return float64(m.SuccessfulOperations) / float64(total) * 100.0
}

// Reset resets all metrics to zero
func (m *BalanceMetrics) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	*m = BalanceMetrics{
		LastBalanceTime: time.Now(),
	}
}

// Global metrics instance for balance tasks
var globalBalanceMetrics = NewBalanceMetrics()

// GetGlobalBalanceMetrics returns the global balance metrics instance
func GetGlobalBalanceMetrics() *BalanceMetrics {
	return globalBalanceMetrics
}
