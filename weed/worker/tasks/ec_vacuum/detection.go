package ec_vacuum

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	wtypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detection identifies EC volumes that need vacuum operations
func Detection(metrics []*wtypes.VolumeHealthMetrics, info *wtypes.ClusterInfo, config base.TaskConfig) ([]*wtypes.TaskDetectionResult, error) {
	ecVacuumConfig, ok := config.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config type for EC vacuum detection")
	}

	if !ecVacuumConfig.Enabled {
		return nil, nil
	}

	glog.V(2).Infof("EC vacuum detection: checking %d volume metrics", len(metrics))

	var results []*wtypes.TaskDetectionResult
	now := time.Now()

	// Get topology info for EC shard analysis
	if info.ActiveTopology == nil {
		glog.V(1).Infof("EC vacuum detection: no topology info available")
		return results, nil
	}

	// Collect EC volume information from topology
	ecVolumeInfo := collectEcVolumeInfo(info.ActiveTopology)
	glog.V(2).Infof("EC vacuum detection: found %d EC volumes in topology", len(ecVolumeInfo))

	for volumeID, ecInfo := range ecVolumeInfo {
		// Apply filters
		if !shouldVacuumEcVolume(ecInfo, ecVacuumConfig, now) {
			continue
		}

		// Calculate deletion ratio
		deletionRatio := calculateDeletionRatio(ecInfo)
		if deletionRatio < ecVacuumConfig.DeletionThreshold {
			glog.V(3).Infof("EC volume %d deletion ratio %.3f below threshold %.3f",
				volumeID, deletionRatio, ecVacuumConfig.DeletionThreshold)
			continue
		}

		// Generate task ID for ActiveTopology integration
		taskID := fmt.Sprintf("ec_vacuum_vol_%d_%d", volumeID, now.Unix())

		result := &wtypes.TaskDetectionResult{
			TaskID:     taskID,
			TaskType:   wtypes.TaskType("ec_vacuum"),
			VolumeID:   volumeID,
			Server:     ecInfo.PrimaryNode,
			Collection: ecInfo.Collection,
			Priority:   wtypes.TaskPriorityLow, // EC vacuum is not urgent
			Reason: fmt.Sprintf("EC volume needs vacuum: deletion_ratio=%.1f%% (>%.1f%%), age=%.1fh (>%dh), size=%.1fMB (>%dMB)",
				deletionRatio*100, ecVacuumConfig.DeletionThreshold*100,
				ecInfo.Age.Hours(), ecVacuumConfig.MinVolumeAgeHours,
				float64(ecInfo.Size)/(1024*1024), ecVacuumConfig.MinSizeMB),
			ScheduleAt: now,
		}

		// Add to topology's pending tasks for capacity management (simplified for now)
		if info.ActiveTopology != nil {
			glog.V(3).Infof("EC vacuum detection: would add pending task %s to topology for volume %d", taskID, volumeID)
			// Note: Simplified for now - in production would properly integrate with ActiveTopology
		}

		results = append(results, result)

		glog.V(1).Infof("EC vacuum detection: queued volume %d for vacuum (deletion_ratio=%.1f%%, size=%.1fMB)",
			volumeID, deletionRatio*100, float64(ecInfo.Size)/(1024*1024))
	}

	glog.V(1).Infof("EC vacuum detection: found %d EC volumes needing vacuum", len(results))
	return results, nil
}

// EcVolumeInfo contains information about an EC volume
type EcVolumeInfo struct {
	VolumeID     uint32
	Collection   string
	Size         uint64
	CreatedAt    time.Time
	Age          time.Duration
	PrimaryNode  string
	ShardNodes   map[pb.ServerAddress]erasure_coding.ShardBits
	DeletionInfo DeletionInfo
}

// DeletionInfo contains deletion statistics for an EC volume
type DeletionInfo struct {
	TotalEntries   int64
	DeletedEntries int64
	DeletionRatio  float64
}

// collectEcVolumeInfo extracts EC volume information from active topology
func collectEcVolumeInfo(activeTopology interface{}) map[uint32]*EcVolumeInfo {
	ecVolumes := make(map[uint32]*EcVolumeInfo)

	// Simplified implementation for demonstration
	// In production, this would query the topology for actual EC volume information
	// For now, return empty map since we don't have direct access to topology data
	glog.V(3).Infof("EC vacuum detection: topology analysis not implemented, returning empty volume list")

	return ecVolumes
}

// shouldVacuumEcVolume determines if an EC volume should be considered for vacuum
func shouldVacuumEcVolume(ecInfo *EcVolumeInfo, config *Config, now time.Time) bool {
	// Check minimum age
	if ecInfo.Age < time.Duration(config.MinVolumeAgeHours)*time.Hour {
		glog.V(3).Infof("EC volume %d too young: age=%.1fh < %dh",
			ecInfo.VolumeID, ecInfo.Age.Hours(), config.MinVolumeAgeHours)
		return false
	}

	// Check minimum size
	sizeMB := float64(ecInfo.Size) / (1024 * 1024)
	if sizeMB < float64(config.MinSizeMB) {
		glog.V(3).Infof("EC volume %d too small: size=%.1fMB < %dMB",
			ecInfo.VolumeID, sizeMB, config.MinSizeMB)
		return false
	}

	// Check collection filter
	if config.CollectionFilter != "" && !strings.Contains(ecInfo.Collection, config.CollectionFilter) {
		glog.V(3).Infof("EC volume %d collection %s doesn't match filter %s",
			ecInfo.VolumeID, ecInfo.Collection, config.CollectionFilter)
		return false
	}

	// Check if we have enough shards for vacuum operation
	totalShards := 0
	for _, shardBits := range ecInfo.ShardNodes {
		totalShards += shardBits.ShardIdCount()
	}

	if totalShards < erasure_coding.DataShardsCount {
		glog.V(3).Infof("EC volume %d insufficient shards for vacuum: have=%d, need=%d",
			ecInfo.VolumeID, totalShards, erasure_coding.DataShardsCount)
		return false
	}

	return true
}

// calculateDeletionRatio calculates the deletion ratio for an EC volume
func calculateDeletionRatio(ecInfo *EcVolumeInfo) float64 {
	if ecInfo.DeletionInfo.TotalEntries == 0 {
		// If no deletion info available, estimate based on shard distribution
		// Volumes with uneven shard distribution might indicate deletion
		return estimateDeletionFromShardDistribution(ecInfo)
	}

	return ecInfo.DeletionInfo.DeletionRatio
}

// estimateDeletionInfo provides a simplified estimation of deletion info
func estimateDeletionInfo(volumeSize uint64) DeletionInfo {
	// Simplified estimation - in reality would parse ecj files
	// For demonstration, assume some deletion exists if the volume is old enough
	estimatedTotal := int64(volumeSize / 1024) // Rough estimate of entries
	estimatedDeleted := estimatedTotal / 10    // Assume 10% deletions as baseline

	deletionRatio := 0.0
	if estimatedTotal > 0 {
		deletionRatio = float64(estimatedDeleted) / float64(estimatedTotal)
	}

	return DeletionInfo{
		TotalEntries:   estimatedTotal,
		DeletedEntries: estimatedDeleted,
		DeletionRatio:  deletionRatio,
	}
}

// estimateDeletionFromShardDistribution estimates deletion ratio from shard distribution patterns
func estimateDeletionFromShardDistribution(ecInfo *EcVolumeInfo) float64 {
	// Simplified heuristic: if shards are not evenly distributed,
	// it might indicate the volume has been through some operations
	// In a real implementation, would analyze ecj files directly

	nodeCount := len(ecInfo.ShardNodes)
	if nodeCount == 0 {
		return 0.0
	}

	// If all shards are on one node, it might indicate consolidation due to deletions
	for _, shardBits := range ecInfo.ShardNodes {
		if shardBits.ShardIdCount() >= erasure_coding.TotalShardsCount {
			return 0.4 // Higher deletion ratio for consolidated volumes
		}
	}

	// Default conservative estimate
	return 0.1
}
