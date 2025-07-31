package erasure_coding

import (
	"sync"
	"time"
)

// ErasureCodingMetrics contains erasure coding-specific monitoring data
type ErasureCodingMetrics struct {
	// Execution metrics
	VolumesEncoded      int64     `json:"volumes_encoded"`
	TotalShardsCreated  int64     `json:"total_shards_created"`
	TotalDataProcessed  int64     `json:"total_data_processed"`
	TotalSourcesRemoved int64     `json:"total_sources_removed"`
	LastEncodingTime    time.Time `json:"last_encoding_time"`

	// Performance metrics
	AverageEncodingTime  int64 `json:"average_encoding_time_seconds"`
	AverageShardSize     int64 `json:"average_shard_size"`
	AverageDataShards    int   `json:"average_data_shards"`
	AverageParityShards  int   `json:"average_parity_shards"`
	SuccessfulOperations int64 `json:"successful_operations"`
	FailedOperations     int64 `json:"failed_operations"`

	// Distribution metrics
	ShardsPerDataCenter  map[string]int64 `json:"shards_per_datacenter"`
	ShardsPerRack        map[string]int64 `json:"shards_per_rack"`
	PlacementSuccessRate float64          `json:"placement_success_rate"`

	// Current task metrics
	CurrentVolumeSize      int64 `json:"current_volume_size"`
	CurrentShardCount      int   `json:"current_shard_count"`
	VolumesPendingEncoding int   `json:"volumes_pending_encoding"`

	mutex sync.RWMutex
}

// NewErasureCodingMetrics creates a new erasure coding metrics instance
func NewErasureCodingMetrics() *ErasureCodingMetrics {
	return &ErasureCodingMetrics{
		LastEncodingTime:    time.Now(),
		ShardsPerDataCenter: make(map[string]int64),
		ShardsPerRack:       make(map[string]int64),
	}
}

// RecordVolumeEncoded records a successful volume encoding operation
func (m *ErasureCodingMetrics) RecordVolumeEncoded(volumeSize int64, shardsCreated int, dataShards int, parityShards int, encodingTime time.Duration, sourceRemoved bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.VolumesEncoded++
	m.TotalShardsCreated += int64(shardsCreated)
	m.TotalDataProcessed += volumeSize
	m.SuccessfulOperations++
	m.LastEncodingTime = time.Now()

	if sourceRemoved {
		m.TotalSourcesRemoved++
	}

	// Update average encoding time
	if m.AverageEncodingTime == 0 {
		m.AverageEncodingTime = int64(encodingTime.Seconds())
	} else {
		// Exponential moving average
		newTime := int64(encodingTime.Seconds())
		m.AverageEncodingTime = (m.AverageEncodingTime*4 + newTime) / 5
	}

	// Update average shard size
	if shardsCreated > 0 {
		avgShardSize := volumeSize / int64(shardsCreated)
		if m.AverageShardSize == 0 {
			m.AverageShardSize = avgShardSize
		} else {
			m.AverageShardSize = (m.AverageShardSize*4 + avgShardSize) / 5
		}
	}

	// Update average data/parity shards
	if m.AverageDataShards == 0 {
		m.AverageDataShards = dataShards
		m.AverageParityShards = parityShards
	} else {
		m.AverageDataShards = (m.AverageDataShards*4 + dataShards) / 5
		m.AverageParityShards = (m.AverageParityShards*4 + parityShards) / 5
	}
}

// RecordFailure records a failed erasure coding operation
func (m *ErasureCodingMetrics) RecordFailure() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.FailedOperations++
}

// RecordShardPlacement records shard placement for distribution tracking
func (m *ErasureCodingMetrics) RecordShardPlacement(dataCenter string, rack string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.ShardsPerDataCenter[dataCenter]++
	rackKey := dataCenter + ":" + rack
	m.ShardsPerRack[rackKey]++
}

// UpdateCurrentVolumeInfo updates current volume processing information
func (m *ErasureCodingMetrics) UpdateCurrentVolumeInfo(volumeSize int64, shardCount int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.CurrentVolumeSize = volumeSize
	m.CurrentShardCount = shardCount
}

// SetVolumesPendingEncoding sets the number of volumes pending encoding
func (m *ErasureCodingMetrics) SetVolumesPendingEncoding(count int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.VolumesPendingEncoding = count
}

// UpdatePlacementSuccessRate updates the placement success rate
func (m *ErasureCodingMetrics) UpdatePlacementSuccessRate(rate float64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.PlacementSuccessRate == 0 {
		m.PlacementSuccessRate = rate
	} else {
		// Exponential moving average
		m.PlacementSuccessRate = 0.8*m.PlacementSuccessRate + 0.2*rate
	}
}

// GetMetrics returns a copy of the current metrics (without the mutex)
func (m *ErasureCodingMetrics) GetMetrics() ErasureCodingMetrics {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Create deep copy of maps
	shardsPerDC := make(map[string]int64)
	for k, v := range m.ShardsPerDataCenter {
		shardsPerDC[k] = v
	}

	shardsPerRack := make(map[string]int64)
	for k, v := range m.ShardsPerRack {
		shardsPerRack[k] = v
	}

	// Create a copy without the mutex to avoid copying lock value
	return ErasureCodingMetrics{
		VolumesEncoded:         m.VolumesEncoded,
		TotalShardsCreated:     m.TotalShardsCreated,
		TotalDataProcessed:     m.TotalDataProcessed,
		TotalSourcesRemoved:    m.TotalSourcesRemoved,
		LastEncodingTime:       m.LastEncodingTime,
		AverageEncodingTime:    m.AverageEncodingTime,
		AverageShardSize:       m.AverageShardSize,
		AverageDataShards:      m.AverageDataShards,
		AverageParityShards:    m.AverageParityShards,
		SuccessfulOperations:   m.SuccessfulOperations,
		FailedOperations:       m.FailedOperations,
		ShardsPerDataCenter:    shardsPerDC,
		ShardsPerRack:          shardsPerRack,
		PlacementSuccessRate:   m.PlacementSuccessRate,
		CurrentVolumeSize:      m.CurrentVolumeSize,
		CurrentShardCount:      m.CurrentShardCount,
		VolumesPendingEncoding: m.VolumesPendingEncoding,
	}
}

// GetSuccessRate returns the success rate as a percentage
func (m *ErasureCodingMetrics) GetSuccessRate() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	total := m.SuccessfulOperations + m.FailedOperations
	if total == 0 {
		return 100.0
	}
	return float64(m.SuccessfulOperations) / float64(total) * 100.0
}

// GetAverageDataProcessed returns the average data processed per volume
func (m *ErasureCodingMetrics) GetAverageDataProcessed() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.VolumesEncoded == 0 {
		return 0
	}
	return float64(m.TotalDataProcessed) / float64(m.VolumesEncoded)
}

// GetSourceRemovalRate returns the percentage of sources removed after encoding
func (m *ErasureCodingMetrics) GetSourceRemovalRate() float64 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if m.VolumesEncoded == 0 {
		return 0
	}
	return float64(m.TotalSourcesRemoved) / float64(m.VolumesEncoded) * 100.0
}

// Reset resets all metrics to zero
func (m *ErasureCodingMetrics) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	*m = ErasureCodingMetrics{
		LastEncodingTime:    time.Now(),
		ShardsPerDataCenter: make(map[string]int64),
		ShardsPerRack:       make(map[string]int64),
	}
}

// Global metrics instance for erasure coding tasks
var globalErasureCodingMetrics = NewErasureCodingMetrics()

// GetGlobalErasureCodingMetrics returns the global erasure coding metrics instance
func GetGlobalErasureCodingMetrics() *ErasureCodingMetrics {
	return globalErasureCodingMetrics
}
