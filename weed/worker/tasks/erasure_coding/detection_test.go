package erasure_coding

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECPlacementPlannerApplyReservations(t *testing.T) {
	activeTopology := buildActiveTopology(t, 1, []string{"hdd"}, 10, 0)

	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	key := ecDiskKey("10.0.0.1:8080", 0)
	candidate, ok := planner.candidateByKey[key]
	require.True(t, ok)
	assert.Equal(t, 10, candidate.FreeSlots)
	assert.Equal(t, 0, candidate.ShardCount)
	assert.Equal(t, 0, candidate.LoadCount)

	shardImpact := topology.CalculateECShardStorageImpact(1, 1)
	destinations := make([]topology.TaskDestinationSpec, 10)
	for i := 0; i < 10; i++ {
		destinations[i] = topology.TaskDestinationSpec{
			ServerID:      "10.0.0.1:8080",
			DiskID:        0,
			StorageImpact: &shardImpact,
		}
	}

	planner.applyTaskReservations(1024, nil, destinations)

	candidate = planner.candidateByKey[key]
	assert.Equal(t, 9, candidate.FreeSlots, "10 shard slots should reduce available volume slots by 1")
	assert.Equal(t, 10, candidate.ShardCount)
	assert.Equal(t, 1, candidate.LoadCount, "load should only be incremented once per disk")
}

func TestPlanECDestinationsUsesPlanner(t *testing.T) {
	activeTopology := buildActiveTopology(t, 7, []string{"hdd", "ssd"}, 100, 0)
	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	metric := &types.VolumeHealthMetrics{
		VolumeID:   1,
		Server:     "10.0.0.1:8080",
		Size:       100 * 1024 * 1024,
		Collection: "",
	}

	plan, err := planECDestinations(planner, metric, NewDefaultConfig())
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, erasure_coding.TotalShardsCount, len(plan.Plans))
}

func TestDetectionContextCancellation(t *testing.T) {
	activeTopology := buildActiveTopology(t, 5, []string{"hdd", "ssd"}, 50, 0)
	clusterInfo := &types.ClusterInfo{ActiveTopology: activeTopology}
	metrics := buildVolumeMetricsForIDs(50)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := Detection(ctx, metrics, clusterInfo, NewDefaultConfig(), 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDetectionMaxResultsHonorsLimit(t *testing.T) {
	activeTopology := buildActiveTopology(t, 4, []string{"hdd"}, 20, 0)
	clusterInfo := &types.ClusterInfo{ActiveTopology: activeTopology}
	metrics := buildVolumeMetricsForIDs(3)

	results, hasMore, err := Detection(context.Background(), metrics, clusterInfo, NewDefaultConfig(), 1)
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.True(t, hasMore)
}

func TestPlanECDestinationsFailsWithInsufficientCapacity(t *testing.T) {
	activeTopology := buildActiveTopology(t, 1, []string{"hdd"}, 1, 1)
	planner := newECPlacementPlanner(activeTopology, nil)
	require.NotNil(t, planner)

	metric := &types.VolumeHealthMetrics{
		VolumeID:   2,
		Server:     "10.0.0.1:8080",
		Size:       10 * 1024 * 1024,
		Collection: "",
	}

	_, err := planECDestinations(planner, metric, NewDefaultConfig())
	require.Error(t, err)
}

func buildVolumeMetricsForIDs(count int) []*types.VolumeHealthMetrics {
	metrics := make([]*types.VolumeHealthMetrics, 0, count)
	now := time.Now()
	for id := 1; id <= count; id++ {
		metrics = append(metrics, &types.VolumeHealthMetrics{
			VolumeID:      uint32(id),
			Server:        "10.0.0.1:8080",
			Size:          200 * 1024 * 1024,
			Collection:    "",
			FullnessRatio: 0.9,
			LastModified:  now.Add(-time.Hour),
			Age:           10 * time.Minute,
		})
	}
	return metrics
}

func buildActiveTopology(t *testing.T, nodeCount int, diskTypes []string, maxVolumeCount, usedVolumeCount int64) *topology.ActiveTopology {
	t.Helper()
	activeTopology := topology.NewActiveTopology(10)

	nodes := make([]*master_pb.DataNodeInfo, 0, nodeCount)
	for i := 1; i <= nodeCount; i++ {
		diskInfos := make(map[string]*master_pb.DiskInfo)
		for diskIndex, diskType := range diskTypes {
			used := usedVolumeCount
			if used > maxVolumeCount {
				used = maxVolumeCount
			}
			volumeInfos := make([]*master_pb.VolumeInformationMessage, 0, 200)
			for vid := 1; vid <= 200; vid++ {
				volumeInfos = append(volumeInfos, &master_pb.VolumeInformationMessage{
					Id:         uint32(vid),
					Collection: "",
					DiskId:     uint32(diskIndex),
				})
			}
			diskInfos[diskType] = &master_pb.DiskInfo{
				DiskId:         uint32(diskIndex),
				VolumeCount:    used,
				MaxVolumeCount: maxVolumeCount,
				VolumeInfos:    volumeInfos,
			}
		}

		nodes = append(nodes, &master_pb.DataNodeInfo{
			Id:        fmt.Sprintf("10.0.0.%d:8080", i),
			DiskInfos: diskInfos,
		})
	}

	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id:            "rack1",
						DataNodeInfos: nodes,
					},
				},
			},
		},
	}

	require.NoError(t, activeTopology.UpdateTopology(topologyInfo))
	return activeTopology
}
