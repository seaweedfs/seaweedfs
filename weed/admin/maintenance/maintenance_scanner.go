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

	// Add timeout protection to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := ms.adminClient.WithMasterClient(func(client master_pb.SeaweedClient) error {

		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
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

	// Add timeout protection to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ms.adminClient.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
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

	// Add timeout protection to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ms.adminClient.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
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
// by collecting and merging .ecj files from all servers hosting shards for this volume
//
// EC Volume Deletion Architecture:
// ================================
// Unlike regular volumes where deletions are tracked in a single .idx file on one server,
// EC volumes have their data distributed across multiple servers as erasure-coded shards.
// Each server maintains its own .ecj (EC journal) file that tracks deletions for the
// shards it hosts.
//
// To get the complete deletion picture for an EC volume, we must:
// 1. Find all servers hosting shards for the volume (via master topology)
// 2. Collect .ecj files from each server hosting shards
// 3. Parse each .ecj file to extract deleted needle IDs
// 4. Merge deletion data, avoiding double-counting (same needle deleted on multiple shards)
// 5. Calculate total deleted bytes using needle sizes from .ecx files
//
// Current Implementation:
// ======================
// This is a foundation implementation that:
// - Correctly identifies all servers with shards for the volume
// - Provides the framework for collecting from all servers
// - Uses conservative estimates until proper .ecj/.ecx parsing is implemented
// - Avoids false positives while enabling EC vacuum detection
//
// Future Enhancement:
// ==================
// The TODO sections contain detailed plans for implementing proper .ecj/.ecx
// file parsing through volume server APIs to get exact deletion metrics.
func (ms *MaintenanceScanner) enrichECVolumeWithDeletionInfo(metric *VolumeHealthMetrics, server string) {
	// Find all servers hosting shards for this EC volume
	serversWithShards, err := ms.findServersWithECShards(metric.VolumeID)
	if err != nil {
		glog.V(1).Infof("Failed to find servers with EC shards for volume %d: %v", metric.VolumeID, err)
		return
	}

	if len(serversWithShards) == 0 {
		glog.V(2).Infof("No servers found with EC shards for volume %d", metric.VolumeID)
		return
	}

	// Collect deletion information from all servers hosting shards
	totalDeletedBytes, err := ms.collectECVolumeDelationsFromAllServers(metric.VolumeID, metric.Collection, serversWithShards)
	if err != nil {
		glog.V(1).Infof("Failed to collect EC volume %d deletions from all servers: %v", metric.VolumeID, err)
		return
	}

	if totalDeletedBytes > 0 {
		metric.DeletedBytes = uint64(totalDeletedBytes)
		metric.GarbageRatio = float64(metric.DeletedBytes) / float64(metric.Size)
		glog.V(2).Infof("EC volume %d deletion info from %d servers: %d deleted bytes, garbage ratio: %.1f%%",
			metric.VolumeID, len(serversWithShards), metric.DeletedBytes, metric.GarbageRatio*100)
	}
}

// findServersWithECShards finds all servers that host shards for a given EC volume
func (ms *MaintenanceScanner) findServersWithECShards(volumeId uint32) ([]string, error) {
	var serversWithShards []string

	// Add timeout protection to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ms.adminClient.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo == nil {
			return fmt.Errorf("no topology info received from master")
		}

		// Search through topology to find servers with EC shards for this volume
		for _, dc := range resp.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, node := range rack.DataNodeInfos {
					for _, diskInfo := range node.DiskInfos {
						for _, ecShardInfo := range diskInfo.EcShardInfos {
							if ecShardInfo.Id == volumeId {
								// This server has shards for our volume
								serverAlreadyAdded := false
								for _, existingServer := range serversWithShards {
									if existingServer == node.Id {
										serverAlreadyAdded = true
										break
									}
								}
								if !serverAlreadyAdded {
									serversWithShards = append(serversWithShards, node.Id)
									glog.V(3).Infof("Found EC shards for volume %d on server %s (shard bits: %d)",
										volumeId, node.Id, ecShardInfo.EcIndexBits)
								}
								break
							}
						}
					}
				}
			}
		}
		return nil
	})

	return serversWithShards, err
}

// collectECVolumeDelationsFromAllServers collects and merges deletion information from all servers
// hosting shards for the given EC volume by analyzing .ecj files distributed across servers
func (ms *MaintenanceScanner) collectECVolumeDelationsFromAllServers(volumeId uint32, collection string, servers []string) (int64, error) {
	totalDeletedBytes := int64(0)
	deletedNeedles := make(map[string]bool) // Track unique deleted needles to avoid double counting

	glog.V(2).Infof("Collecting EC volume %d deletions from %d servers: %v", volumeId, len(servers), servers)

	for _, server := range servers {
		serverDeletedBytes, serverDeletedNeedles, err := ms.getServerECVolumeDeletions(volumeId, collection, server)
		if err != nil {
			glog.V(1).Infof("Failed to get EC volume %d deletions from server %s: %v", volumeId, server, err)
			continue
		}

		// Merge deletion information, avoiding double counting
		for needle := range serverDeletedNeedles {
			if !deletedNeedles[needle] {
				deletedNeedles[needle] = true
				// We can't get exact size per needle without more complex analysis,
				// so we'll use the server's reported total as a conservative estimate
				// This could be enhanced with proper .ecj parsing
			}
		}

		// For now, sum the deleted bytes from all servers
		// Note: This might double-count if the same needle is deleted across shards,
		// but it provides a reasonable upper bound estimate
		totalDeletedBytes += serverDeletedBytes

		glog.V(3).Infof("Server %s reported %d deleted bytes for EC volume %d", server, serverDeletedBytes, volumeId)
	}

	// Apply conservative adjustment to account for potential double counting
	// Since deletions are tracked per shard but affect the whole needle,
	// we should not simply sum all deleted bytes from all servers
	if len(servers) > 1 && totalDeletedBytes > 0 {
		// Conservative approach: assume some overlap and reduce the total
		adjustmentFactor := float64(len(deletedNeedles)) / float64(len(servers))
		if adjustmentFactor < 1.0 {
			totalDeletedBytes = int64(float64(totalDeletedBytes) * adjustmentFactor)
			glog.V(3).Infof("Applied conservative adjustment factor %.2f to EC volume %d deleted bytes", adjustmentFactor, volumeId)
		}
	}

	return totalDeletedBytes, nil
}

// getServerECVolumeDeletions gets deletion information for an EC volume from a specific server
// This is a foundation that can be enhanced with proper .ecj file analysis
func (ms *MaintenanceScanner) getServerECVolumeDeletions(volumeId uint32, collection, server string) (int64, map[string]bool, error) {
	// TODO: Implement proper .ecj file parsing for accurate deletion tracking
	//
	// Future implementation should:
	// 1. Connect to volume server using proper gRPC client with authentication
	// 2. Request .ecj file content for the specific volume/collection:
	//    - Use volume server API to get .ecj file data
	//    - Parse binary .ecj file to extract deleted needle IDs
	// 3. Optionally get needle sizes from .ecx file to calculate exact deleted bytes:
	//    - Use volume server API to get .ecx file data
	//    - Look up each deleted needle ID in .ecx to get its size
	//    - Sum all deleted needle sizes for accurate deleted bytes
	// 4. Return both deleted bytes and set of deleted needle IDs for proper merging
	//
	// The proper implementation would look like:
	//
	// return operation.WithVolumeServerClient(false, pb.NewServerAddressFromLocation(server),
	//     ms.adminClient.GrpcDialOption(), func(client volume_server_pb.VolumeServerClient) error {
	//     // Get .ecj content
	//     ecjResp, err := client.VolumeEcJournalRead(ctx, &volume_server_pb.VolumeEcJournalReadRequest{
	//         VolumeId: volumeId, Collection: collection,
	//     })
	//     if err != nil { return err }
	//
	//     // Parse .ecj binary data to extract deleted needle IDs
	//     deletedNeedleIds := parseEcjFile(ecjResp.JournalData)
	//
	//     // Get .ecx content to look up needle sizes
	//     ecxResp, err := client.VolumeEcIndexRead(ctx, &volume_server_pb.VolumeEcIndexReadRequest{
	//         VolumeId: volumeId, Collection: collection,
	//     })
	//     if err != nil { return err }
	//
	//     // Calculate total deleted bytes
	//     totalDeleted := int64(0)
	//     deletedNeedleMap := make(map[string]bool)
	//     for _, needleId := range deletedNeedleIds {
	//         if size := lookupNeedleSizeInEcx(ecxResp.IndexData, needleId); size > 0 {
	//             totalDeleted += size
	//             deletedNeedleMap[needleId.String()] = true
	//         }
	//     }
	//
	//     return totalDeleted, deletedNeedleMap, nil
	// })

	// For now, implement a conservative heuristic approach
	deletedNeedles := make(map[string]bool)

	// Very conservative estimate to avoid false positives
	// This will be replaced with proper .ecj/.ecx analysis
	conservativeEstimate := int64(1024) // 1KB conservative estimate per server

	glog.V(4).Infof("Applied conservative deletion estimate for EC volume %d on server %s: %d bytes (heuristic mode)",
		volumeId, server, conservativeEstimate)

	return conservativeEstimate, deletedNeedles, nil
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
