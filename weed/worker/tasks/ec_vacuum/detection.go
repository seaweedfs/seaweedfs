package ec_vacuum

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
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

	// Collect EC volume information from metrics
	ecVolumeInfo := collectEcVolumeInfo(metrics, info)
	glog.V(2).Infof("EC vacuum detection: found %d EC volumes in metrics", len(ecVolumeInfo))

	for volumeID, ecInfo := range ecVolumeInfo {
		// Calculate deletion ratio first for logging
		deletionRatio := calculateDeletionRatio(ecInfo)

		// Apply filters and track why volumes don't qualify
		if !shouldVacuumEcVolume(ecInfo, ecVacuumConfig, now) {
			continue
		}

		if deletionRatio < ecVacuumConfig.DeletionThreshold {
			glog.V(3).Infof("EC volume %d deletion ratio %.3f below threshold %.3f",
				volumeID, deletionRatio, ecVacuumConfig.DeletionThreshold)
			continue
		}

		// Generate task ID for ActiveTopology integration
		taskID := fmt.Sprintf("ec_vacuum_vol_%d_%d", volumeID, now.Unix())

		// Create task sources from shard information
		var sources []*worker_pb.TaskSource
		for serverAddr, shardBits := range ecInfo.ShardNodes {
			shardIds := make([]uint32, 0, shardBits.ShardIdCount())
			for i := 0; i < erasure_coding.TotalShardsCount; i++ {
				if shardBits.HasShardId(erasure_coding.ShardId(i)) {
					shardIds = append(shardIds, uint32(i))
				}
			}
			if len(shardIds) > 0 {
				sources = append(sources, &worker_pb.TaskSource{
					Node:          string(serverAddr),
					VolumeId:      volumeID,
					ShardIds:      shardIds,
					EstimatedSize: ecInfo.Size / uint64(len(ecInfo.ShardNodes)), // Rough estimate per server
				})
			}
		}

		// Create TypedParams for EC vacuum task
		typedParams := &worker_pb.TaskParams{
			TaskId:     taskID,
			VolumeId:   volumeID,
			Collection: ecInfo.Collection,
			VolumeSize: ecInfo.Size,
			Sources:    sources,
			TaskParams: &worker_pb.TaskParams_VacuumParams{
				VacuumParams: &worker_pb.VacuumTaskParams{
					GarbageThreshold: deletionRatio,
					ForceVacuum:      false,
					BatchSize:        1000,        // Default batch size
					WorkingDir:       "/data/ec_vacuum",  // Default base directory - worker may use BaseWorkingDir/ec_vacuum instead
					VerifyChecksum:   true,        // Enable checksum verification for safety
				},
			},
		}

		result := &wtypes.TaskDetectionResult{
			TaskID:      taskID,
			TaskType:    wtypes.TaskType("ec_vacuum"),
			VolumeID:    volumeID,
			Server:      ecInfo.PrimaryNode,
			Collection:  ecInfo.Collection,
			Priority:    wtypes.TaskPriorityLow, // EC vacuum is not urgent
			Reason: fmt.Sprintf("EC volume needs vacuum: deletion_ratio=%.1f%% (>%.1f%%), age=%.1fh (>%.1fh), size=%.1fMB (>%dMB)",
				deletionRatio*100, ecVacuumConfig.DeletionThreshold*100,
				ecInfo.Age.Hours(), (time.Duration(ecVacuumConfig.MinVolumeAgeSeconds) * time.Second).Hours(),
				float64(ecInfo.Size)/(1024*1024), ecVacuumConfig.MinSizeMB),
			TypedParams: typedParams,
			ScheduleAt:  now,
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

	// Show detailed criteria for volumes that didn't qualify (similar to erasure coding detection)
	if len(results) == 0 && len(ecVolumeInfo) > 0 {
		glog.V(1).Infof("EC vacuum detection: No tasks created for %d volumes", len(ecVolumeInfo))

		// Show details for first few EC volumes
		count := 0
		for volumeID, ecInfo := range ecVolumeInfo {
			if count >= 3 { // Limit to first 3 volumes to avoid spam
				break
			}

			deletionRatio := calculateDeletionRatio(ecInfo)
			sizeMB := float64(ecInfo.Size) / (1024 * 1024)
			deletedMB := deletionRatio * sizeMB
			ageRequired := time.Duration(ecVacuumConfig.MinVolumeAgeSeconds) * time.Second

			// Check shard availability
			totalShards := 0
			for _, shardBits := range ecInfo.ShardNodes {
				totalShards += shardBits.ShardIdCount()
			}

			glog.Infof("EC VACUUM: Volume %d: deleted=%.1fMB, ratio=%.1f%% (need ≥%.1f%%), age=%s (need ≥%s), size=%.1fMB (need ≥%dMB), shards=%d (need ≥%d)",
				volumeID, deletedMB, deletionRatio*100, ecVacuumConfig.DeletionThreshold*100,
				ecInfo.Age.Truncate(time.Minute), ageRequired.Truncate(time.Minute),
				sizeMB, ecVacuumConfig.MinSizeMB, totalShards, erasure_coding.DataShardsCount)
			count++
		}
	}

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

// collectEcVolumeInfo extracts EC volume information from volume health metrics and topology
func collectEcVolumeInfo(metrics []*wtypes.VolumeHealthMetrics, info *wtypes.ClusterInfo) map[uint32]*EcVolumeInfo {
	ecVolumes := make(map[uint32]*EcVolumeInfo)

	for _, metric := range metrics {
		// Only process EC volumes
		if !metric.IsECVolume {
			continue
		}

		// Calculate deletion ratio from health metrics
		deletionRatio := 0.0
		if metric.Size > 0 {
			deletionRatio = float64(metric.DeletedBytes) / float64(metric.Size)
		}

		// Create EC volume info from metrics
		ecVolumes[metric.VolumeID] = &EcVolumeInfo{
			VolumeID:    metric.VolumeID,
			Collection:  metric.Collection,
			Size:        metric.Size,
			CreatedAt:   time.Now().Add(-metric.Age),
			Age:         metric.Age,
			PrimaryNode: metric.Server,
			ShardNodes:  make(map[pb.ServerAddress]erasure_coding.ShardBits), // Will be populated if needed
			DeletionInfo: DeletionInfo{
				TotalEntries:   int64(metric.Size / 1024), // Rough estimate
				DeletedEntries: int64(metric.DeletedBytes / 1024),
				DeletionRatio:  deletionRatio,
			},
		}

		glog.V(2).Infof("EC vacuum detection: found EC volume %d, size=%dMB, deleted=%dMB, ratio=%.1f%%",
			metric.VolumeID, metric.Size/(1024*1024), metric.DeletedBytes/(1024*1024), deletionRatio*100)
	}

	// Populate shard information from cluster topology
	if info.ActiveTopology != nil {
		populateShardInfo(ecVolumes, info.ActiveTopology)
	}

	glog.V(1).Infof("EC vacuum detection: found %d EC volumes from %d metrics", len(ecVolumes), len(metrics))
	return ecVolumes
}

// populateShardInfo populates the ShardNodes information from cluster topology
func populateShardInfo(ecVolumes map[uint32]*EcVolumeInfo, activeTopology *topology.ActiveTopology) {
	if activeTopology == nil {
		return
	}

	// Get topology information
	topologyInfo := activeTopology.GetTopologyInfo()
	if topologyInfo == nil {
		return
	}

	// Iterate through topology to find EC shard information
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, diskInfo := range node.DiskInfos {
					// Check each EC shard on this disk
					for _, ecShardInfo := range diskInfo.EcShardInfos {
						volumeID := ecShardInfo.Id

						// Only process volumes we're tracking
						if ecVolumeInfo, exists := ecVolumes[volumeID]; exists {
							// Initialize ShardNodes map if needed
							if ecVolumeInfo.ShardNodes == nil {
								ecVolumeInfo.ShardNodes = make(map[pb.ServerAddress]erasure_coding.ShardBits)
							}

							// Add shards from this node
							serverAddr := pb.ServerAddress(node.Id)
							if _, exists := ecVolumeInfo.ShardNodes[serverAddr]; !exists {
								ecVolumeInfo.ShardNodes[serverAddr] = erasure_coding.ShardBits(0)
							}

							// Add all shards on this disk for this volume
							for i := 0; i < len(ecShardInfo.ShardSizes); i++ {
								ecVolumeInfo.ShardNodes[serverAddr] = ecVolumeInfo.ShardNodes[serverAddr].AddShardId(erasure_coding.ShardId(i))
							}

							glog.V(3).Infof("EC volume %d: found %d shards on server %s",
								volumeID, len(ecShardInfo.ShardSizes), node.Id)
						}
					}
				}
			}
		}
	}

	// Log shard distribution summary
	for volumeID, ecInfo := range ecVolumes {
		totalShards := 0
		for _, shardBits := range ecInfo.ShardNodes {
			totalShards += shardBits.ShardIdCount()
		}
		glog.V(2).Infof("EC volume %d: total shards=%d across %d servers",
			volumeID, totalShards, len(ecInfo.ShardNodes))
	}
}

// shouldVacuumEcVolume determines if an EC volume should be considered for vacuum
func shouldVacuumEcVolume(ecInfo *EcVolumeInfo, config *Config, now time.Time) bool {
	// Check minimum age
	minAge := time.Duration(config.MinVolumeAgeSeconds) * time.Second
	if ecInfo.Age < minAge {
		glog.V(3).Infof("EC volume %d too young: age=%.1fh < %.1fh",
			ecInfo.VolumeID, ecInfo.Age.Hours(), minAge.Hours())
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
