package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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
	// Get volume health metrics directly in task-system format, along with topology info
	taskMetrics, topologyInfo, err := ms.getVolumeHealthMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to get volume health metrics: %w", err)
	}

	// Use task system for all task types
	if ms.integration != nil {
		// Update topology information for complete cluster view (including empty servers)
		// This must happen before task detection to ensure EC placement can consider all servers
		if topologyInfo != nil {
			if err := ms.integration.UpdateTopologyInfo(topologyInfo); err != nil {
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

// getVolumeHealthMetrics collects health information for all volumes.
// Returns metrics in task-system format directly (no intermediate copy) and
// the topology info for updating the active topology.
func (ms *MaintenanceScanner) getVolumeHealthMetrics() ([]*types.VolumeHealthMetrics, *master_pb.TopologyInfo, error) {
	var metrics []*types.VolumeHealthMetrics
	var topologyInfo *master_pb.TopologyInfo

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

		// Track node counts for summary logging (avoid accumulating full ID slices)
		var totalNodes, nodesWithVolumes, nodesWithoutVolumes int

		for _, dc := range resp.TopologyInfo.DataCenterInfos {
			glog.V(3).Infof("Processing datacenter: %s", dc.Id)
			for _, rack := range dc.RackInfos {
				glog.V(3).Infof("Processing rack: %s in datacenter: %s", rack.Id, dc.Id)
				for _, node := range rack.DataNodeInfos {
					totalNodes++
					glog.V(3).Infof("Found volume server in topology: %s (disks: %d)", node.Id, len(node.DiskInfos))

					hasVolumes := false
					// Process each disk on this node
					for diskType, diskInfo := range node.DiskInfos {
						if len(diskInfo.VolumeInfos) > 0 {
							hasVolumes = true
							glog.V(3).Infof("Volume server %s disk %s has %d volumes", node.Id, diskType, len(diskInfo.VolumeInfos))
						}

						// Process volumes on this specific disk
						for _, volInfo := range diskInfo.VolumeInfos {
							metric := &types.VolumeHealthMetrics{
								VolumeID:         volInfo.Id,
								Server:           node.Id,
								ServerAddress:    node.Address,
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

							glog.V(4).Infof("Volume %d on %s:%s (ID %d): size=%d, limit=%d, fullness=%.2f",
								metric.VolumeID, metric.Server, metric.DiskType, metric.DiskId, metric.Size, volumeSizeLimitBytes, metric.FullnessRatio)

							metrics = append(metrics, metric)
						}
					}

					if hasVolumes {
						nodesWithVolumes++
					} else {
						nodesWithoutVolumes++
						glog.V(1).Infof("Volume server %s found in topology but has no volumes", node.Id)
					}
				}
			}
		}

		glog.Infof("Topology discovery: %d volume servers (%d with volumes, %d without)",
			totalNodes, nodesWithVolumes, nodesWithoutVolumes)

		// Return topology info as a local value (not retained on the scanner struct)
		topologyInfo = resp.TopologyInfo

		return nil
	})

	if err != nil {
		glog.Errorf("Failed to get volume health metrics: %v", err)
		return nil, nil, err
	}

	glog.V(1).Infof("Successfully collected metrics for %d actual volumes with disk ID information", len(metrics))

	// Count actual replicas and identify EC volumes
	ms.enrichVolumeMetrics(metrics)

	return metrics, topologyInfo, nil
}

// enrichVolumeMetrics adds additional information like replica counts
func (ms *MaintenanceScanner) enrichVolumeMetrics(metrics []*types.VolumeHealthMetrics) {
	// Group volumes by ID to count replicas
	volumeGroups := make(map[uint32][]*types.VolumeHealthMetrics)
	for _, metric := range metrics {
		volumeGroups[metric.VolumeID] = append(volumeGroups[metric.VolumeID], metric)
	}

	// Update replica counts for actual volumes
	for volumeID, replicas := range volumeGroups {
		replicaCount := len(replicas)
		for _, replica := range replicas {
			replica.ReplicaCount = replicaCount
		}
		glog.V(4).Infof("Volume %d has %d replicas", volumeID, replicaCount)
	}

	// TODO: Identify EC volumes by checking volume structure
	// This would require querying volume servers for EC shard information
}
