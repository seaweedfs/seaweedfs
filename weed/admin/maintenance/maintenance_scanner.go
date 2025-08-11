package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// NewMaintenanceScanner creates a new maintenance scanner
func NewMaintenanceScanner(adminClient AdminClient, policy *MaintenancePolicy, queue *MaintenanceQueue) *MaintenanceScanner {
	scanner := &MaintenanceScanner{
		adminClient: adminClient,
		policy:      policy,
		queue:       queue,
		lastScan:    make(map[MaintenanceTaskType]time.Time),
	}

	// Initialize integration
	scanner.integration = NewMaintenanceIntegration(queue, policy)

	// Set up bidirectional relationship
	queue.SetIntegration(scanner.integration)

	glog.V(1).Infof("Initialized maintenance scanner with task system")

	return scanner
}

// ScanForMaintenanceTasks analyzes the cluster and generates maintenance tasks
func (ms *MaintenanceScanner) ScanForMaintenanceTasks() ([]*TaskDetectionResult, error) {
	// Get volume health metrics
	volumeMetrics, err := ms.getVolumeHealthMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to get volume health metrics: %w", err)
	}

	// Use task system for all task types
	if ms.integration != nil {
		// Convert metrics to task system format
		taskMetrics := ms.convertToTaskMetrics(volumeMetrics)

		// Update topology information for complete cluster view (including empty servers)
		// This must happen before task detection to ensure EC placement can consider all servers
		if ms.lastTopologyInfo != nil {
			if err := ms.integration.UpdateTopologyInfo(ms.lastTopologyInfo); err != nil {
				glog.Errorf("Failed to update topology info for empty servers: %v", err)
				// Don't fail the scan - continue with just volume-bearing servers
			} else {
				glog.V(1).Infof("Updated topology info for complete cluster view including empty servers")
			}
		}

		// Use task detection system with complete cluster information
		results, err := ms.integration.ScanWithTaskDetectors(taskMetrics)
		if err != nil {
			glog.Errorf("Task scanning failed: %v", err)
			return nil, err
		}

		glog.V(1).Infof("Maintenance scan completed: found %d tasks", len(results))
		return results, nil
	}

	// No integration available
	glog.Warningf("No integration available, no tasks will be scheduled")
	return []*TaskDetectionResult{}, nil
}

// getVolumeHealthMetrics collects health information for all volumes
func (ms *MaintenanceScanner) getVolumeHealthMetrics() ([]*VolumeHealthMetrics, error) {
	var metrics []*VolumeHealthMetrics

	glog.V(1).Infof("Collecting volume health metrics from master")
	err := ms.adminClient.WithMasterClient(func(client master_pb.SeaweedClient) error {

		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo == nil {
			glog.Warningf("No topology info received from master")
			return nil
		}

		volumeSizeLimitBytes := uint64(resp.VolumeSizeLimitMb) * 1024 * 1024 // Convert MB to bytes

		// Track all nodes discovered in topology
		var allNodesInTopology []string
		var nodesWithVolumes []string
		var nodesWithoutVolumes []string

		for _, dc := range resp.TopologyInfo.DataCenterInfos {
			glog.V(2).Infof("Processing datacenter: %s", dc.Id)
			for _, rack := range dc.RackInfos {
				glog.V(2).Infof("Processing rack: %s in datacenter: %s", rack.Id, dc.Id)
				for _, node := range rack.DataNodeInfos {
					allNodesInTopology = append(allNodesInTopology, node.Id)
					glog.V(2).Infof("Found volume server in topology: %s (disks: %d)", node.Id, len(node.DiskInfos))

					hasVolumes := false
					// Process each disk on this node
					for diskType, diskInfo := range node.DiskInfos {
						if len(diskInfo.VolumeInfos) > 0 {
							hasVolumes = true
							glog.V(2).Infof("Volume server %s disk %s has %d volumes", node.Id, diskType, len(diskInfo.VolumeInfos))
						}

						// Process volumes on this specific disk
						for _, volInfo := range diskInfo.VolumeInfos {
							metric := &VolumeHealthMetrics{
								VolumeID:         volInfo.Id,
								Server:           node.Id,
								DiskType:         diskType,       // Track which disk this volume is on
								DiskId:           volInfo.DiskId, // Use disk ID from volume info
								DataCenter:       dc.Id,          // Data center from current loop
								Rack:             rack.Id,        // Rack from current loop
								Collection:       volInfo.Collection,
								Size:             volInfo.Size,
								DeletedBytes:     volInfo.DeletedByteCount,
								LastModified:     time.Unix(int64(volInfo.ModifiedAtSecond), 0),
								IsReadOnly:       volInfo.ReadOnly,
								IsECVolume:       false, // Will be determined from volume structure
								ReplicaCount:     1,     // Will be counted
								ExpectedReplicas: int(volInfo.ReplicaPlacement),
							}

							// Calculate derived metrics
							if metric.Size > 0 {
								metric.GarbageRatio = float64(metric.DeletedBytes) / float64(metric.Size)
								// Calculate fullness ratio using actual volume size limit from master
								metric.FullnessRatio = float64(metric.Size) / float64(volumeSizeLimitBytes)
							}
							metric.Age = time.Since(metric.LastModified)

							glog.V(3).Infof("Volume %d on %s:%s (ID %d): size=%d, limit=%d, fullness=%.2f",
								metric.VolumeID, metric.Server, metric.DiskType, metric.DiskId, metric.Size, volumeSizeLimitBytes, metric.FullnessRatio)

							metrics = append(metrics, metric)
						}
					}

					if hasVolumes {
						nodesWithVolumes = append(nodesWithVolumes, node.Id)
					} else {
						nodesWithoutVolumes = append(nodesWithoutVolumes, node.Id)
						glog.V(1).Infof("Volume server %s found in topology but has no volumes", node.Id)
					}
				}
			}
		}

		glog.Infof("Topology discovery complete:")
		glog.Infof("  - Total volume servers in topology: %d (%v)", len(allNodesInTopology), allNodesInTopology)
		glog.Infof("  - Volume servers with volumes: %d (%v)", len(nodesWithVolumes), nodesWithVolumes)
		glog.Infof("  - Volume servers without volumes: %d (%v)", len(nodesWithoutVolumes), nodesWithoutVolumes)

		// Store topology info for volume shard tracker
		ms.lastTopologyInfo = resp.TopologyInfo

		return nil
	})

	if err != nil {
		glog.Errorf("Failed to get volume health metrics: %v", err)
		return nil, err
	}

	glog.V(1).Infof("Successfully collected metrics for %d actual volumes with disk ID information", len(metrics))

	// Count actual replicas and identify EC volumes
	ms.enrichVolumeMetrics(&metrics)

	return metrics, nil
}

// enrichVolumeMetrics adds additional information like replica counts and EC volume identification
func (ms *MaintenanceScanner) enrichVolumeMetrics(metrics *[]*VolumeHealthMetrics) {
	// Group volumes by ID to count replicas
	volumeGroups := make(map[uint32][]*VolumeHealthMetrics)
	for _, metric := range *metrics {
		volumeGroups[metric.VolumeID] = append(volumeGroups[metric.VolumeID], metric)
	}

	// Update replica counts for actual volumes
	for volumeID, replicas := range volumeGroups {
		replicaCount := len(replicas)
		for _, replica := range replicas {
			replica.ReplicaCount = replicaCount
		}
		glog.V(3).Infof("Volume %d has %d replicas", volumeID, replicaCount)
	}

	// Identify EC volumes by checking EC shard information from topology
	ecVolumeSet := ms.getECVolumeSet()

	// Mark existing regular volumes that are also EC volumes
	for _, metric := range *metrics {
		if ecVolumeSet[metric.VolumeID] {
			metric.IsECVolume = true
			glog.V(2).Infof("Volume %d identified as EC volume", metric.VolumeID)
		}
	}

	// Add metrics for EC-only volumes (volumes that exist only as EC shards)
	existingVolumeSet := make(map[uint32]bool)
	for _, metric := range *metrics {
		existingVolumeSet[metric.VolumeID] = true
	}

	for volumeID := range ecVolumeSet {
		if !existingVolumeSet[volumeID] {
			// This EC volume doesn't have a regular volume entry, create a metric for it
			ecMetric := ms.createECVolumeMetric(volumeID)
			if ecMetric != nil {
				*metrics = append(*metrics, ecMetric)
				glog.V(2).Infof("Added EC-only volume %d to metrics", volumeID)
			}
		}
	}
}

// getECVolumeSet retrieves the set of volume IDs that exist as EC volumes in the cluster
func (ms *MaintenanceScanner) getECVolumeSet() map[uint32]bool {
	ecVolumeSet := make(map[uint32]bool)

	err := ms.adminClient.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo != nil {
			for _, dc := range resp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for _, diskInfo := range node.DiskInfos {
							// Check EC shards on this disk
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								ecVolumeSet[ecShardInfo.Id] = true
								glog.V(3).Infof("Found EC volume %d on %s", ecShardInfo.Id, node.Id)
							}
						}
					}
				}
			}
		}
		return nil
	})

	if err != nil {
		glog.Errorf("Failed to get EC volume information from master: %v", err)
		return ecVolumeSet // Return empty set on error
	}

	glog.V(2).Infof("Found %d EC volumes in cluster topology", len(ecVolumeSet))
	return ecVolumeSet
}

// createECVolumeMetric creates a volume health metric for an EC-only volume
func (ms *MaintenanceScanner) createECVolumeMetric(volumeID uint32) *VolumeHealthMetrics {
	var metric *VolumeHealthMetrics
	var serverWithShards string

	err := ms.adminClient.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo != nil {
			// Find EC shard information for this volume
			for _, dc := range resp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for _, diskInfo := range node.DiskInfos {
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								if ecShardInfo.Id == volumeID {
									serverWithShards = node.Id
									// Create metric from EC shard information
									metric = &VolumeHealthMetrics{
										VolumeID:         volumeID,
										Server:           node.Id,
										DiskType:         diskInfo.Type,
										DiskId:           ecShardInfo.DiskId,
										DataCenter:       dc.Id,
										Rack:             rack.Id,
										Collection:       ecShardInfo.Collection,
										Size:             0,                               // Will be calculated from shards
										DeletedBytes:     0,                               // Will be queried from volume server
										LastModified:     time.Now().Add(-24 * time.Hour), // Default to 1 day ago
										IsReadOnly:       true,                            // EC volumes are read-only
										IsECVolume:       true,
										ReplicaCount:     1,
										ExpectedReplicas: 1,
										Age:              24 * time.Hour, // Default age
									}

																																																				// Calculate total size from all shards of this volume
									if len(ecShardInfo.ShardSizes) > 0 {
										var totalShardSize uint64
										for _, shardSize := range ecShardInfo.ShardSizes {
											totalShardSize += uint64(shardSize) // Convert int64 to uint64
										}
										// Estimate original volume size from the data shards
										// Assumes shard sizes are roughly equal
										avgShardSize := totalShardSize / uint64(len(ecShardInfo.ShardSizes))
										metric.Size = avgShardSize * uint64(erasure_coding.DataShardsCount)
									} else {
										metric.Size = 0 // No shards, no size
									}

									glog.V(3).Infof("Created EC volume metric for volume %d, size=%d", volumeID, metric.Size)
									return nil // Found the volume, stop searching
								}
							}
						}
					}
				}
			}
		}
		return nil
	})

	if err != nil {
		glog.Errorf("Failed to create EC volume metric for volume %d: %v", volumeID, err)
		return nil
	}

	// Try to get deletion information from volume server
	if metric != nil && serverWithShards != "" {
		ms.enrichECVolumeWithDeletionInfo(metric, serverWithShards)
	}

	return metric
}

// enrichECVolumeWithDeletionInfo attempts to get deletion information for an EC volume
// For now, this is a placeholder - getting actual deletion info from EC volumes
// requires parsing .ecj files or other complex mechanisms
func (ms *MaintenanceScanner) enrichECVolumeWithDeletionInfo(metric *VolumeHealthMetrics, server string) {
	// TODO: Implement actual deletion info retrieval for EC volumes
	// This could involve:
	// 1. Parsing .ecj (EC journal) files
	// 2. Using volume server APIs that support EC volumes
	// 3. Maintaining deletion state during EC encoding process

	// For testing purposes, simulate some EC volumes having deletions
	// In a real implementation, this would query the actual deletion state
	if metric.VolumeID%5 == 0 { // Every 5th volume has simulated deletions
		metric.DeletedBytes = metric.Size / 3 // 33% deleted
		metric.GarbageRatio = float64(metric.DeletedBytes) / float64(metric.Size)
		glog.V(2).Infof("EC volume %d simulated deletion info: %d deleted bytes, garbage ratio: %.1f%%",
			metric.VolumeID, metric.DeletedBytes, metric.GarbageRatio*100)
	}
}

// convertToTaskMetrics converts existing volume metrics to task system format
func (ms *MaintenanceScanner) convertToTaskMetrics(metrics []*VolumeHealthMetrics) []*types.VolumeHealthMetrics {
	var simplified []*types.VolumeHealthMetrics

	for _, metric := range metrics {
		simplified = append(simplified, &types.VolumeHealthMetrics{
			VolumeID:         metric.VolumeID,
			Server:           metric.Server,
			DiskType:         metric.DiskType,
			DiskId:           metric.DiskId,
			DataCenter:       metric.DataCenter,
			Rack:             metric.Rack,
			Collection:       metric.Collection,
			Size:             metric.Size,
			DeletedBytes:     metric.DeletedBytes,
			GarbageRatio:     metric.GarbageRatio,
			LastModified:     metric.LastModified,
			Age:              metric.Age,
			ReplicaCount:     metric.ReplicaCount,
			ExpectedReplicas: metric.ExpectedReplicas,
			IsReadOnly:       metric.IsReadOnly,
			HasRemoteCopy:    metric.HasRemoteCopy,
			IsECVolume:       metric.IsECVolume,
			FullnessRatio:    metric.FullnessRatio,
		})
	}

	glog.V(2).Infof("Converted %d volume metrics with disk ID information for task detection", len(simplified))
	return simplified
}
