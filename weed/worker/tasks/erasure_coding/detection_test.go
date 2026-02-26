package erasure_coding

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECPlacementPlannerApplyReservations(t *testing.T) {
	activeTopology := buildActiveTopology(t, 1, []string{"hdd"}, 10)

	planner := newECPlacementPlanner(activeTopology)
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
	activeTopology := buildActiveTopology(t, 7, []string{"hdd", "ssd"}, 100)
	planner := newECPlacementPlanner(activeTopology)
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
	assert.Equal(t, erasure_coding.TotalShardsCount, plan.TotalShards)
}

func buildActiveTopology(t *testing.T, nodeCount int, diskTypes []string, maxVolumeCount int64) *topology.ActiveTopology {
	t.Helper()
	activeTopology := topology.NewActiveTopology(10)

	nodes := make([]*master_pb.DataNodeInfo, 0, nodeCount)
	for i := 1; i <= nodeCount; i++ {
		diskInfos := make(map[string]*master_pb.DiskInfo)
		for diskIndex, diskType := range diskTypes {
			diskInfos[diskType] = &master_pb.DiskInfo{
				DiskId:         uint32(diskIndex),
				VolumeCount:    0,
				MaxVolumeCount: maxVolumeCount,
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
