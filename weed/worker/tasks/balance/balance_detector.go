package balance

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BalanceDetector implements TaskDetector for balance tasks
type BalanceDetector struct {
	enabled          bool
	threshold        float64 // Imbalance threshold (0.1 = 10%)
	minCheckInterval time.Duration
	minVolumeCount   int
	lastCheck        time.Time
}

// Compile-time interface assertions
var (
	_ types.TaskDetector = (*BalanceDetector)(nil)
)

// NewBalanceDetector creates a new balance detector
func NewBalanceDetector() *BalanceDetector {
	return &BalanceDetector{
		enabled:          true,
		threshold:        0.1, // 10% imbalance threshold
		minCheckInterval: 1 * time.Hour,
		minVolumeCount:   10, // Don't balance small clusters
		lastCheck:        time.Time{},
	}
}

// GetTaskType returns the task type
func (d *BalanceDetector) GetTaskType() types.TaskType {
	return types.TaskTypeBalance
}

// ScanForTasks checks if cluster balance is needed
func (d *BalanceDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
	if !d.enabled {
		return nil, nil
	}

	glog.V(2).Infof("Scanning for balance tasks...")

	// Don't check too frequently
	if time.Since(d.lastCheck) < d.minCheckInterval {
		return nil, nil
	}
	d.lastCheck = time.Now()

	// Skip if cluster is too small
	if len(volumeMetrics) < d.minVolumeCount {
		glog.V(2).Infof("Cluster too small for balance (%d volumes < %d minimum)", len(volumeMetrics), d.minVolumeCount)
		return nil, nil
	}

	// Analyze volume distribution across servers
	serverVolumeCounts := make(map[string]int)
	for _, metric := range volumeMetrics {
		serverVolumeCounts[metric.Server]++
	}

	if len(serverVolumeCounts) < 2 {
		glog.V(2).Infof("Not enough servers for balance (%d servers)", len(serverVolumeCounts))
		return nil, nil
	}

	// Calculate balance metrics
	totalVolumes := len(volumeMetrics)
	avgVolumesPerServer := float64(totalVolumes) / float64(len(serverVolumeCounts))

	maxVolumes := 0
	minVolumes := totalVolumes
	maxServer := ""
	minServer := ""

	for server, count := range serverVolumeCounts {
		if count > maxVolumes {
			maxVolumes = count
			maxServer = server
		}
		if count < minVolumes {
			minVolumes = count
			minServer = server
		}
	}

	// Check if imbalance exceeds threshold
	imbalanceRatio := float64(maxVolumes-minVolumes) / avgVolumesPerServer
	if imbalanceRatio <= d.threshold {
		glog.V(2).Infof("Cluster is balanced (imbalance ratio: %.2f <= %.2f)", imbalanceRatio, d.threshold)
		return nil, nil
	}

	// Create balance task
	reason := fmt.Sprintf("Cluster imbalance detected: %.1f%% (max: %d on %s, min: %d on %s, avg: %.1f)",
		imbalanceRatio*100, maxVolumes, maxServer, minVolumes, minServer, avgVolumesPerServer)

	task := &types.TaskDetectionResult{
		TaskType:   types.TaskTypeBalance,
		Priority:   types.TaskPriorityNormal,
		Reason:     reason,
		ScheduleAt: time.Now(),
		Parameters: map[string]interface{}{
			"imbalance_ratio":        imbalanceRatio,
			"threshold":              d.threshold,
			"max_volumes":            maxVolumes,
			"min_volumes":            minVolumes,
			"avg_volumes_per_server": avgVolumesPerServer,
			"max_server":             maxServer,
			"min_server":             minServer,
			"total_servers":          len(serverVolumeCounts),
		},
	}

	glog.V(1).Infof("🔄 Found balance task: %s", reason)
	return []*types.TaskDetectionResult{task}, nil
}

// ScanInterval returns how often to scan
func (d *BalanceDetector) ScanInterval() time.Duration {
	return d.minCheckInterval
}

// IsEnabled returns whether the detector is enabled
func (d *BalanceDetector) IsEnabled() bool {
	return d.enabled
}

// SetEnabled sets whether the detector is enabled
func (d *BalanceDetector) SetEnabled(enabled bool) {
	d.enabled = enabled
	glog.V(1).Infof("🔄 Balance detector enabled: %v", enabled)
}

// SetThreshold sets the imbalance threshold
func (d *BalanceDetector) SetThreshold(threshold float64) {
	d.threshold = threshold
	glog.V(1).Infof("🔄 Balance threshold set to: %.1f%%", threshold*100)
}

// SetMinCheckInterval sets the minimum time between balance checks
func (d *BalanceDetector) SetMinCheckInterval(interval time.Duration) {
	d.minCheckInterval = interval
	glog.V(1).Infof("🔄 Balance check interval set to: %v", interval)
}

// SetMinVolumeCount sets the minimum volume count for balance operations
func (d *BalanceDetector) SetMinVolumeCount(count int) {
	d.minVolumeCount = count
	glog.V(1).Infof("🔄 Balance minimum volume count set to: %d", count)
}

// GetThreshold returns the current imbalance threshold
func (d *BalanceDetector) GetThreshold() float64 {
	return d.threshold
}

// GetMinCheckInterval returns the minimum check interval
func (d *BalanceDetector) GetMinCheckInterval() time.Duration {
	return d.minCheckInterval
}

// GetMinVolumeCount returns the minimum volume count
func (d *BalanceDetector) GetMinVolumeCount() int {
	return d.minVolumeCount
}
