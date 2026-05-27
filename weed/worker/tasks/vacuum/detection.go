package vacuum

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/util"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detection implements the detection logic for vacuum tasks
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	vacuumConfig := config.(*Config)
	minVolumeAge := time.Duration(vacuumConfig.MinVolumeAgeSeconds) * time.Second

	type volumeBucket struct {
		replicas []*types.VolumeHealthMetrics
	}
	buckets := make(map[uint32]*volumeBucket)
	order := make([]uint32, 0)
	for _, m := range metrics {
		b, ok := buckets[m.VolumeID]
		if !ok {
			b = &volumeBucket{}
			buckets[m.VolumeID] = b
			order = append(order, m.VolumeID)
		}
		b.replicas = append(b.replicas, m)
	}

	var results []*types.TaskDetectionResult
	skippedDueToGarbage := 0
	skippedDueToAge := 0
	debugCount := 0

	for _, vid := range order {
		replicas := buckets[vid].replicas

		// Pick the replica with the highest garbage ratio that also satisfies
		// the minimum-age gate as the "primary" used for priority + reason.
		// Master built-in vacuum filters per-replica again inside
		// batchVacuumVolumeCheck, so it's fine if only some replicas are
		// above threshold here — the worker's checkVacuumEligibility will
		// filter again at execute time.
		primary := pickPrimaryReplica(replicas, vacuumConfig.GarbageThreshold, minVolumeAge)
		if primary == nil {
			recordSkipReasons(replicas, vacuumConfig.GarbageThreshold, minVolumeAge, &skippedDueToGarbage, &skippedDueToAge, &debugCount)
			continue
		}

		priority := types.TaskPriorityNormal
		if primary.GarbageRatio > 0.6 {
			priority = types.TaskPriorityHigh
		}

		taskID := fmt.Sprintf("vacuum_vol_%d_%d", vid, time.Now().Unix())

		result := &types.TaskDetectionResult{
			TaskID:     taskID,
			TaskType:   types.TaskTypeVacuum,
			VolumeID:   vid,
			Server:     primary.Server,
			Collection: primary.Collection,
			Priority:   priority,
			Reason:     "Volume has excessive garbage requiring vacuum",
			ScheduleAt: time.Now(),
		}

		// Check if ANY task already exists in ActiveTopology for this volume
		if clusterInfo != nil && clusterInfo.ActiveTopology != nil {
			if clusterInfo.ActiveTopology.HasAnyTask(vid) {
				glog.V(2).Infof("VACUUM: Skipping volume %d, task already exists in ActiveTopology", vid)
				continue
			}
		}

		result.TypedParams = createVacuumTaskParams(result, replicas, vacuumConfig, clusterInfo)
		if result.TypedParams != nil {
			results = append(results, result)
		}
	}

	if len(results) == 0 && len(metrics) > 0 {
		totalVolumes := len(metrics)
		glog.V(1).Infof("VACUUM: No tasks created for %d volume replicas across %d volumes. Threshold=%.2f%%, MinAge=%s. Skipped: %d (garbage<threshold), %d (age<minimum)",
			totalVolumes, len(buckets), vacuumConfig.GarbageThreshold*100, minVolumeAge, skippedDueToGarbage, skippedDueToAge)

		for i, metric := range metrics {
			if i >= 3 {
				break
			}
			glog.V(1).Infof("VACUUM: Volume %d on %s: garbage=%.2f%% (need ≥%.2f%%), age=%s (need ≥%s)",
				metric.VolumeID, metric.Server, metric.GarbageRatio*100, vacuumConfig.GarbageThreshold*100,
				metric.Age.Truncate(time.Minute), minVolumeAge.Truncate(time.Minute))
		}
	}

	return results, nil
}

// pickPrimaryReplica returns the eligible replica with the highest garbage
// ratio. Returns nil if no replica satisfies both gates.
func pickPrimaryReplica(replicas []*types.VolumeHealthMetrics, garbageThreshold float64, minAge time.Duration) *types.VolumeHealthMetrics {
	var best *types.VolumeHealthMetrics
	for _, m := range replicas {
		if m.GarbageRatio < garbageThreshold {
			continue
		}
		if m.Age < minAge {
			continue
		}
		if best == nil || m.GarbageRatio > best.GarbageRatio {
			best = m
		}
	}
	return best
}

// recordSkipReasons tallies skip counters for debug logging when no
// replica of a volume qualified for vacuum.
func recordSkipReasons(replicas []*types.VolumeHealthMetrics, garbageThreshold float64, minAge time.Duration, garbageSkip, ageSkip, debugCount *int) {
	for _, m := range replicas {
		if *debugCount >= 5 {
			return
		}
		if m.GarbageRatio < garbageThreshold {
			*garbageSkip++
		}
		if m.Age < minAge {
			*ageSkip++
		}
		*debugCount++
	}
}

// createVacuumTaskParams creates typed parameters for a vacuum task whose
// Sources list contains every replica of the volume. Worker-side
// performVacuum iterates over Sources and vacuums each replica.
func createVacuumTaskParams(task *types.TaskDetectionResult, replicas []*types.VolumeHealthMetrics, vacuumConfig *Config, clusterInfo *types.ClusterInfo) *worker_pb.TaskParams {
	garbageThreshold := 0.3
	verifyChecksum := true
	batchSize := int32(1000)
	workingDir := ""

	if vacuumConfig != nil {
		garbageThreshold = vacuumConfig.GarbageThreshold
	}

	if clusterInfo == nil || clusterInfo.ActiveTopology == nil {
		glog.Errorf("Topology not available for vacuum task on volume %d, skipping", task.VolumeID)
		return nil
	}

	sources := make([]*worker_pb.TaskSource, 0, len(replicas))
	seen := make(map[string]struct{}, len(replicas))
	for _, m := range replicas {
		address, err := util.ResolveServerAddress(m.Server, clusterInfo.ActiveTopology)
		if err != nil {
			glog.Warningf("Failed to resolve address for server %s for vacuum task on volume %d, dropping replica: %v", m.Server, task.VolumeID, err)
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		sources = append(sources, &worker_pb.TaskSource{
			Node:          address,
			VolumeId:      task.VolumeID,
			EstimatedSize: m.Size,
			DataCenter:    m.DataCenter,
			Rack:          m.Rack,
		})
	}

	if len(sources) == 0 {
		glog.Errorf("No resolvable replicas for vacuum task on volume %d, skipping", task.VolumeID)
		return nil
	}

	// Use the primary replica (matches task.Server) for the canonical size
	// recorded in TaskParams. Master built-in keeps a single volumeSizeLimit
	// here so timeouts are stable across replicas.
	canonical := primaryReplicaByServer(replicas, task.Server)
	var canonicalSize uint64
	if canonical != nil {
		canonicalSize = canonical.Size
	}

	return &worker_pb.TaskParams{
		TaskId:     task.TaskID,
		VolumeId:   task.VolumeID,
		Collection: task.Collection,
		VolumeSize: canonicalSize,
		Sources:    sources,
		TaskParams: &worker_pb.TaskParams_VacuumParams{
			VacuumParams: &worker_pb.VacuumTaskParams{
				GarbageThreshold: garbageThreshold,
				ForceVacuum:      false,
				BatchSize:        batchSize,
				WorkingDir:       workingDir,
				VerifyChecksum:   verifyChecksum,
			},
		},
	}
}

func primaryReplicaByServer(replicas []*types.VolumeHealthMetrics, server string) *types.VolumeHealthMetrics {
	for _, m := range replicas {
		if m.Server == server {
			return m
		}
	}
	if len(replicas) > 0 {
		return replicas[0]
	}
	return nil
}
