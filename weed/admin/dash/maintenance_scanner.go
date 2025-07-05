package dash

import (
	"context"
	"crypto/rand"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// NewMaintenanceScanner creates a new maintenance scanner
func NewMaintenanceScanner(adminServer *AdminServer, policy *MaintenancePolicy, queue *MaintenanceQueue) *MaintenanceScanner {
	scanner := &MaintenanceScanner{
		adminServer: adminServer,
		policy:      policy,
		queue:       queue,
		lastScan:    make(map[MaintenanceTaskType]time.Time),
	}

	// Initialize simplified integration
	scanner.simplifiedIntegration = NewSimplifiedMaintenanceIntegration(queue, policy)

	// Set up bidirectional relationship
	queue.SetSimplifiedIntegration(scanner.simplifiedIntegration)

	glog.V(1).Infof("Initialized maintenance scanner with simplified task system")

	return scanner
}

// ScanForMaintenanceTasks analyzes the cluster and generates maintenance tasks
func (ms *MaintenanceScanner) ScanForMaintenanceTasks() ([]*TaskDetectionResult, error) {
	now := time.Now()

	// Get volume health metrics
	volumeMetrics, err := ms.getVolumeHealthMetrics()
	if err != nil {
		return nil, fmt.Errorf("failed to get volume health metrics: %v", err)
	}

	// Use simplified system for registered task types
	if ms.simplifiedIntegration != nil {
		// Convert metrics to simplified format
		simplifiedMetrics := ms.convertToSimplifiedMetrics(volumeMetrics)

		// Use simplified detection system
		simplifiedResults, err := ms.simplifiedIntegration.ScanWithSimplifiedTasks(simplifiedMetrics)
		if err != nil {
			glog.Errorf("Simplified task scanning failed, falling back to hardcoded: %v", err)
		} else {
			glog.V(1).Infof("Simplified maintenance scan completed: found %d tasks", len(simplifiedResults))
			return simplifiedResults, nil
		}
	}

	// Fallback to hardcoded logic for non-simplified tasks or if simplified system fails
	var results []*TaskDetectionResult

	// Check each maintenance type (fallback logic)
	if ms.policy.VacuumEnabled && ms.shouldScan(TaskTypeVacuum, now) {
		vacuumTasks := ms.scanForVacuumTasks(volumeMetrics, now)
		results = append(results, vacuumTasks...)
		ms.lastScan[TaskTypeVacuum] = now
	}

	if ms.policy.ECEnabled && ms.shouldScan(TaskTypeErasureCoding, now) {
		ecTasks := ms.scanForECTasks(volumeMetrics, now)
		results = append(results, ecTasks...)
		ms.lastScan[TaskTypeErasureCoding] = now
	}

	if ms.policy.RemoteUploadEnabled && ms.shouldScan(TaskTypeRemoteUpload, now) {
		remoteTasks := ms.scanForRemoteUploadTasks(volumeMetrics, now)
		results = append(results, remoteTasks...)
		ms.lastScan[TaskTypeRemoteUpload] = now
	}

	if ms.policy.ReplicationFixEnabled && ms.shouldScan(TaskTypeFixReplication, now) {
		replicationTasks := ms.scanForReplicationTasks(volumeMetrics, now)
		results = append(results, replicationTasks...)
		ms.lastScan[TaskTypeFixReplication] = now
	}

	if ms.policy.BalanceEnabled && ms.shouldScan(TaskTypeBalance, now) {
		balanceTasks := ms.scanForBalanceTasks(now)
		results = append(results, balanceTasks...)
		ms.lastScan[TaskTypeBalance] = now
	}

	// Always check for cluster replication tasks (they come from message queue)
	if ms.shouldScan(TaskTypeClusterReplication, now) {
		clusterReplicationTasks := ms.scanForClusterReplicationTasks(now)
		results = append(results, clusterReplicationTasks...)
		ms.lastScan[TaskTypeClusterReplication] = now
	}

	glog.V(1).Infof("Maintenance scan completed: found %d tasks", len(results))
	return results, nil
}

// shouldScan determines if a scan should be performed for a specific task type
func (ms *MaintenanceScanner) shouldScan(taskType MaintenanceTaskType, now time.Time) bool {
	lastScan, exists := ms.lastScan[taskType]
	if !exists {
		return true // First scan
	}

	var interval time.Duration
	switch taskType {
	case TaskTypeVacuum:
		interval = time.Duration(ms.policy.VacuumMinInterval) * time.Hour
	case TaskTypeErasureCoding:
		interval = 2 * time.Hour // Check EC more frequently
	case TaskTypeRemoteUpload:
		interval = 4 * time.Hour
	case TaskTypeFixReplication:
		interval = time.Duration(ms.policy.ReplicationCheckInterval) * time.Hour
	case TaskTypeBalance:
		interval = time.Duration(ms.policy.BalanceCheckInterval) * time.Hour
	case TaskTypeClusterReplication:
		interval = 1 * time.Hour // Check cluster replication tasks more frequently
	default:
		interval = time.Hour
	}

	return now.Sub(lastScan) >= interval
}

// getVolumeHealthMetrics collects health information for all volumes
func (ms *MaintenanceScanner) getVolumeHealthMetrics() ([]*VolumeHealthMetrics, error) {
	var metrics []*VolumeHealthMetrics

	err := ms.adminServer.WithMasterClient(func(client master_pb.SeaweedClient) error {
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

// convertToSimplifiedMetrics converts existing volume metrics to simplified format
func (ms *MaintenanceScanner) convertToSimplifiedMetrics(metrics []*VolumeHealthMetrics) []*types.VolumeHealthMetrics {
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

// scanForVacuumTasks identifies volumes that need vacuum operations
func (ms *MaintenanceScanner) scanForVacuumTasks(metrics []*VolumeHealthMetrics, now time.Time) []*TaskDetectionResult {
	var tasks []*TaskDetectionResult

	// Get current running vacuum tasks to avoid duplicates
	runningVacuumCount := ms.queue.GetRunningTaskCount(TaskTypeVacuum)
	if runningVacuumCount >= ms.policy.VacuumMaxConcurrent {
		return tasks // Already at max capacity
	}

	for _, metric := range metrics {
		// Check if volume needs vacuum
		if metric.GarbageRatio >= ms.policy.VacuumGarbageRatio {
			// Check if vacuum was done recently
			if ms.wasTaskRecentlyCompleted(TaskTypeVacuum, metric.VolumeID, metric.Server, now) {
				continue
			}

			priority := PriorityNormal
			if metric.GarbageRatio > 0.7 {
				priority = PriorityHigh
			}
			if metric.GarbageRatio > 0.9 {
				priority = PriorityCritical
			}

			task := &TaskDetectionResult{
				TaskType:   TaskTypeVacuum,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   priority,
				Reason:     fmt.Sprintf("Garbage ratio %.1f%% exceeds threshold %.1f%%", metric.GarbageRatio*100, ms.policy.VacuumGarbageRatio*100),
				Parameters: map[string]interface{}{
					"garbage_ratio": metric.GarbageRatio,
					"threshold":     ms.policy.VacuumGarbageRatio,
				},
				ScheduleAt: now,
			}
			tasks = append(tasks, task)

			// Limit concurrent vacuum operations
			if len(tasks)+runningVacuumCount >= ms.policy.VacuumMaxConcurrent {
				break
			}
		}
	}

	return tasks
}

// scanForECTasks identifies volumes that should be converted to erasure coding
func (ms *MaintenanceScanner) scanForECTasks(metrics []*VolumeHealthMetrics, now time.Time) []*TaskDetectionResult {
	var tasks []*TaskDetectionResult

	runningECCount := ms.queue.GetRunningTaskCount(TaskTypeErasureCoding)
	if runningECCount >= ms.policy.ECMaxConcurrent {
		return tasks
	}

	ageThreshold := time.Duration(ms.policy.ECVolumeAgeHours) * time.Hour

	for _, metric := range metrics {
		// Skip if already EC volume
		if metric.IsECVolume {
			continue
		}

		// Check age and fullness criteria
		if metric.Age >= ageThreshold && metric.FullnessRatio >= ms.policy.ECFullnessRatio {
			// Check if volume is read-only (safe for EC conversion)
			if !metric.IsReadOnly {
				continue
			}

			task := &TaskDetectionResult{
				TaskType:   TaskTypeErasureCoding,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   PriorityLow, // EC is not urgent
				Reason:     fmt.Sprintf("Volume is %v old and %.1f%% full", metric.Age.Truncate(time.Hour), metric.FullnessRatio*100),
				Parameters: map[string]interface{}{
					"age_hours":      int(metric.Age.Hours()),
					"fullness_ratio": metric.FullnessRatio,
				},
				ScheduleAt: now,
			}
			tasks = append(tasks, task)

			if len(tasks)+runningECCount >= ms.policy.ECMaxConcurrent {
				break
			}
		}
	}

	return tasks
}

// scanForRemoteUploadTasks identifies volumes for remote storage upload
func (ms *MaintenanceScanner) scanForRemoteUploadTasks(metrics []*VolumeHealthMetrics, now time.Time) []*TaskDetectionResult {
	var tasks []*TaskDetectionResult

	runningUploadCount := ms.queue.GetRunningTaskCount(TaskTypeRemoteUpload)
	if runningUploadCount >= ms.policy.RemoteUploadMaxConcurrent {
		return tasks
	}

	ageThreshold := time.Duration(ms.policy.RemoteUploadAgeHours) * time.Hour
	pattern := ms.policy.RemoteUploadPattern

	for _, metric := range metrics {
		// Skip if already has remote copy
		if metric.HasRemoteCopy {
			continue
		}

		// Check age criteria
		if metric.Age < ageThreshold {
			continue
		}

		// Check collection pattern if specified
		if pattern != "" && !ms.matchesPattern(metric.Collection, pattern) {
			continue
		}

		// Prefer read-only volumes for remote upload
		priority := PriorityLow
		if metric.IsReadOnly {
			priority = PriorityNormal
		}

		task := &TaskDetectionResult{
			TaskType:   TaskTypeRemoteUpload,
			VolumeID:   metric.VolumeID,
			Server:     metric.Server,
			Collection: metric.Collection,
			Priority:   priority,
			Reason:     fmt.Sprintf("Volume is %v old, eligible for remote upload", metric.Age.Truncate(time.Hour)),
			Parameters: map[string]interface{}{
				"age_hours": int(metric.Age.Hours()),
			},
			ScheduleAt: now,
		}
		tasks = append(tasks, task)

		if len(tasks)+runningUploadCount >= ms.policy.RemoteUploadMaxConcurrent {
			break
		}
	}

	return tasks
}

// scanForReplicationTasks identifies volumes with replication issues
func (ms *MaintenanceScanner) scanForReplicationTasks(metrics []*VolumeHealthMetrics, now time.Time) []*TaskDetectionResult {
	var tasks []*TaskDetectionResult

	runningReplicationCount := ms.queue.GetRunningTaskCount(TaskTypeFixReplication)
	if runningReplicationCount >= ms.policy.ReplicationMaxConcurrent {
		return tasks
	}

	for _, metric := range metrics {
		// Check if replication count matches expected
		if metric.ReplicaCount != metric.ExpectedReplicas {
			priority := PriorityHigh
			reason := ""

			if metric.ReplicaCount < metric.ExpectedReplicas {
				reason = fmt.Sprintf("Under-replicated: %d replicas, expected %d", metric.ReplicaCount, metric.ExpectedReplicas)
				if metric.ReplicaCount == 1 && metric.ExpectedReplicas > 1 {
					priority = PriorityCritical // Single point of failure
				}
			} else {
				reason = fmt.Sprintf("Over-replicated: %d replicas, expected %d", metric.ReplicaCount, metric.ExpectedReplicas)
				priority = PriorityNormal // Less urgent
			}

			task := &TaskDetectionResult{
				TaskType:   TaskTypeFixReplication,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   priority,
				Reason:     reason,
				Parameters: map[string]interface{}{
					"actual_replicas":   metric.ReplicaCount,
					"expected_replicas": metric.ExpectedReplicas,
				},
				ScheduleAt: now,
			}
			tasks = append(tasks, task)

			if len(tasks)+runningReplicationCount >= ms.policy.ReplicationMaxConcurrent {
				break
			}
		}
	}

	return tasks
}

// scanForBalanceTasks identifies cluster imbalance issues
func (ms *MaintenanceScanner) scanForBalanceTasks(now time.Time) []*TaskDetectionResult {
	var tasks []*TaskDetectionResult

	runningBalanceCount := ms.queue.GetRunningTaskCount(TaskTypeBalance)
	if runningBalanceCount >= ms.policy.BalanceMaxConcurrent {
		return tasks
	}

	// Get cluster topology for balance analysis
	topology, err := ms.adminServer.GetClusterTopology()
	if err != nil {
		glog.Errorf("Failed to get cluster topology for balance check: %v", err)
		return tasks
	}

	// Analyze volume distribution imbalance
	if ms.isClusterImbalanced(topology) {
		task := &TaskDetectionResult{
			TaskType: TaskTypeBalance,
			Priority: PriorityNormal,
			Reason:   fmt.Sprintf("Cluster imbalance exceeds %.1f%% threshold", ms.policy.BalanceThreshold*100),
			Parameters: map[string]interface{}{
				"threshold": ms.policy.BalanceThreshold,
			},
			ScheduleAt: now,
		}
		tasks = append(tasks, task)
	}

	return tasks
}

// isClusterImbalanced checks if the cluster has volume distribution imbalance
func (ms *MaintenanceScanner) isClusterImbalanced(topology *ClusterTopology) bool {
	if len(topology.VolumeServers) < 2 {
		return false // Can't balance with less than 2 servers
	}

	totalVolumes := 0
	maxVolumes := 0
	minVolumes := int(^uint(0) >> 1) // Max int

	for _, server := range topology.VolumeServers {
		totalVolumes += server.Volumes
		if server.Volumes > maxVolumes {
			maxVolumes = server.Volumes
		}
		if server.Volumes < minVolumes {
			minVolumes = server.Volumes
		}
	}

	if totalVolumes == 0 {
		return false
	}

	avgVolumes := float64(totalVolumes) / float64(len(topology.VolumeServers))
	imbalanceRatio := float64(maxVolumes-minVolumes) / avgVolumes

	return imbalanceRatio > ms.policy.BalanceThreshold
}

// wasTaskRecentlyCompleted checks if a task was recently completed to avoid duplicates
func (ms *MaintenanceScanner) wasTaskRecentlyCompleted(taskType MaintenanceTaskType, volumeID uint32, server string, now time.Time) bool {
	// Check with the queue for recently completed tasks
	return ms.queue.WasTaskRecentlyCompleted(taskType, volumeID, server, now)
}

// matchesPattern checks if a collection name matches a pattern
func (ms *MaintenanceScanner) matchesPattern(collection, pattern string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}

	// Convert shell-style pattern to regex
	regexPattern := strings.ReplaceAll(pattern, "*", ".*")
	regexPattern = "^" + regexPattern + "$"

	matched, err := regexp.MatchString(regexPattern, collection)
	if err != nil {
		glog.Errorf("Invalid pattern '%s': %v", pattern, err)
		return false
	}

	return matched
}

// generateTaskID creates a unique task ID
func generateTaskID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// scanForClusterReplicationTasks identifies files needing replication to remote clusters
func (ms *MaintenanceScanner) scanForClusterReplicationTasks(now time.Time) []*TaskDetectionResult {
	var tasks []*TaskDetectionResult

	runningReplicationCount := ms.queue.GetRunningTaskCount(TaskTypeClusterReplication)
	maxConcurrent := 3 // Default for cluster replication

	if runningReplicationCount >= maxConcurrent {
		return tasks
	}

	// Get replication requests from message queue
	replicationRequests, err := ms.getReplicationRequestsFromMQ()
	if err != nil {
		glog.Errorf("Failed to get replication requests from message queue: %v", err)
		return tasks
	}

	for _, request := range replicationRequests {
		// Check if we've reached concurrent limit
		if len(tasks)+runningReplicationCount >= maxConcurrent {
			break
		}

		// Determine priority based on request metadata
		priority := PriorityNormal
		if request.ReplicationMode == "sync" {
			priority = PriorityHigh
		} else if request.ReplicationMode == "backup" {
			priority = PriorityLow
		}

		task := &TaskDetectionResult{
			TaskType: TaskTypeClusterReplication,
			Priority: priority,
			Reason:   fmt.Sprintf("Replicate %s to cluster %s", request.SourcePath, request.TargetCluster),
			Parameters: map[string]interface{}{
				"source_path":      request.SourcePath,
				"target_cluster":   request.TargetCluster,
				"target_path":      request.TargetPath,
				"replication_mode": request.ReplicationMode,
				"file_size":        request.FileSize,
				"checksum":         request.Checksum,
				"metadata":         request.Metadata,
			},
			ScheduleAt: now,
		}
		tasks = append(tasks, task)
	}

	return tasks
}

// getReplicationRequestsFromMQ reads replication requests from SeaweedFS message queue
func (ms *MaintenanceScanner) getReplicationRequestsFromMQ() ([]*ClusterReplicationTask, error) {
	var requests []*ClusterReplicationTask

	// This would integrate with SeaweedFS message queue system
	// For now, simulate reading from a hypothetical replication topic

	// In a real implementation, this would:
	// 1. Connect to SeaweedFS MQ broker
	// 2. Subscribe to "cluster_replication" topic
	// 3. Read pending replication messages
	// 4. Parse them into ClusterReplicationTask structs

	// Example implementation would look like:
	/*
		err := ms.adminServer.WithMQClient(func(mqClient mq_pb.SeaweedMessagingClient) error {
			stream, err := mqClient.Subscribe(context.Background(), &mq_pb.SubscribeRequest{
				Message: &mq_pb.SubscribeRequest_Init{
					Init: &mq_pb.SubscribeRequest_InitMessage{
						Topic:     "cluster_replication",
						Partition: 0,
					},
				},
			})
			if err != nil {
				return err
			}

			// Read available messages
			for {
				resp, err := stream.Recv()
				if err != nil {
					break // No more messages
				}

				// Parse message into ClusterReplicationTask
				var task ClusterReplicationTask
				if err := json.Unmarshal(resp.Message.Data, &task); err == nil {
					requests = append(requests, &task)
				}
			}

			return nil
		})
	*/

	// For demonstration, return empty list
	return requests, nil
}
