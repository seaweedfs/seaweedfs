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
	// Get volume health metrics
	volumeMetrics, err := ms.getVolumeHealthMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to get volume health metrics: %v", err)
	}

	// Use task system for all task types
	if ms.integration != nil {
		// Convert metrics to task system format
		taskMetrics := ms.convertToTaskMetrics(volumeMetrics)

		// Use task detection system
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

	err := ms.adminClient.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo == nil {
			return nil
		}

		for _, dc := range resp.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, node := range rack.DataNodeInfos {
					for _, diskInfo := range node.DiskInfos {
						for _, volInfo := range diskInfo.VolumeInfos {
							metric := &VolumeHealthMetrics{
								VolumeID:         volInfo.Id,
								Server:           node.Id,
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
								// Calculate fullness ratio (would need volume size limit)
								// metric.FullnessRatio = float64(metric.Size) / float64(volumeSizeLimit)
							}
							metric.Age = time.Since(metric.LastModified)

							metrics = append(metrics, metric)
						}
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Count actual replicas and identify EC volumes
	ms.enrichVolumeMetrics(metrics)

	return metrics, nil
}

// enrichVolumeMetrics adds additional information like replica counts
func (ms *MaintenanceScanner) enrichVolumeMetrics(metrics []*VolumeHealthMetrics) {
	// Group volumes by ID to count replicas
	volumeGroups := make(map[uint32][]*VolumeHealthMetrics)
	for _, metric := range metrics {
		volumeGroups[metric.VolumeID] = append(volumeGroups[metric.VolumeID], metric)
	}

	// Update replica counts
	for _, group := range volumeGroups {
		actualReplicas := len(group)
		for _, metric := range group {
			metric.ReplicaCount = actualReplicas
		}
	}
}

// convertToTaskMetrics converts existing volume metrics to task system format
func (ms *MaintenanceScanner) convertToTaskMetrics(metrics []*VolumeHealthMetrics) []*types.VolumeHealthMetrics {
	var simplified []*types.VolumeHealthMetrics

	for _, metric := range metrics {
		simplified = append(simplified, &types.VolumeHealthMetrics{
			VolumeID:         metric.VolumeID,
			Server:           metric.Server,
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

	return simplified
}
