package delete_empty

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// superBlockSize is the minimum size of a volume file (just the header).
// A volume at exactly this size has no needle data — it is empty.
const superBlockSize = 8

// Detection implements the detection logic for compaction tasks.
// When DeleteEmptyEnabled is set it selects volumes whose on-disk size equals
// the superblock header (i.e. FileCount == 0) and that have not been modified
// within the configured quiet period, to avoid deleting freshly allocated volumes.
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	cfg := config.(*Config)

	if !cfg.DeleteEmptyEnabled {
		glog.V(2).Infof("COMPACTION: delete-empty-volumes is disabled, skipping")
		return nil, nil
	}

	quietFor := time.Duration(cfg.QuietForSeconds) * time.Second
	now := time.Now()

	var results []*types.TaskDetectionResult

	for _, metric := range metrics {
		if metric.Size > superBlockSize {
			continue
		}
		if metric.LastModified.IsZero() {
			continue
		}
		if now.Sub(metric.LastModified) < quietFor {
			glog.V(3).Infof("COMPACTION: skipping volume %d on %s — modified %v ago (quiet period: %v)",
				metric.VolumeID, metric.Server, now.Sub(metric.LastModified).Truncate(time.Minute), quietFor)
			continue
		}

		// Build a per-replica task ID so each replica of an empty volume
		// gets its own compaction task without ID collisions.
		taskID := fmt.Sprintf("compaction_%d_%s_%d", metric.VolumeID, metric.ServerAddress, now.Unix())

		if clusterInfo != nil && clusterInfo.ActiveTopology != nil {
			if clusterInfo.ActiveTopology.HasAnyTask(metric.VolumeID) {
				glog.V(2).Infof("COMPACTION: skipping volume %d on %s, task already exists in ActiveTopology", metric.VolumeID, metric.ServerAddress)
				continue
			}
		}

		result := &types.TaskDetectionResult{
			TaskID:     taskID,
			TaskType:   types.TaskTypeCompaction,
			VolumeID:   metric.VolumeID,
			Server:     metric.Server,
			Collection: metric.Collection,
			Priority:   types.TaskPriorityLow,
			Reason:     "Volume is empty and has been quiet for the configured period",
			ScheduleAt: now,
			TypedParams: &worker_pb.TaskParams{
				TaskId:     taskID,
				VolumeId:   metric.VolumeID,
				Collection: metric.Collection,
				VolumeSize: metric.Size,
				Sources: []*worker_pb.TaskSource{
					{
						Node:          metric.ServerAddress,
						VolumeId:      metric.VolumeID,
						EstimatedSize: metric.Size,
						DataCenter:    metric.DataCenter,
						Rack:          metric.Rack,
					},
				},
			},
		}

		glog.V(1).Infof("COMPACTION: detected empty volume %d on %s (collection: %q, last modified: %v ago)",
			metric.VolumeID, metric.Server, metric.Collection, now.Sub(metric.LastModified).Truncate(time.Minute))

		results = append(results, result)
	}

	if len(results) == 0 && len(metrics) > 0 {
		glog.V(1).Infof("COMPACTION: no empty volumes found among %d volumes (quiet period: %v)", len(metrics), quietFor)
	}

	return results, nil
}
