package ec_repair

import (
	"fmt"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/require"
)

type planTestNode struct {
	id       string
	address  string
	diskType string
	diskID   uint32
	shards   map[uint32]int64
}

func TestDetectReturnsExtraShardCandidates(t *testing.T) {
	nodes := []planTestNode{
		{
			id:       "nodeA",
			address:  "127.0.0.1:9100",
			diskType: "hdd",
			diskID:   0,
			shards:   makeShardMap(0, 9, 100),
		},
		{
			id:       "nodeB",
			address:  "127.0.0.1:9101",
			diskType: "hdd",
			diskID:   0,
			shards: map[uint32]int64{
				0:  90, // mismatched size
				2: 100,
				3: 100,
				4: 100,
				5: 100,
				6: 100,
			},
		},
	}

	topo := buildPlanTopology(nodes)
	candidates, hasMore, err := Detect(topo, "", 0)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, candidates, 1)

	candidate := candidates[0]
	require.Greater(t, candidate.ExtraShards, 0)
	require.Greater(t, candidate.MismatchedShards, 0)
}

func TestBuildRepairPlanRequiresEnoughShards(t *testing.T) {
	nodes := []planTestNode{
		{id: "nodeA", address: "n1", diskType: "hdd", diskID: 0, shards: makeShardMap(0, 4, 100)},
	}
	topo := buildPlanTopology(nodes)

	_, err := BuildRepairPlan(topo, nil, 300, "", "hdd")
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("need at least %d", erasure_coding.DataShardsCount))
}

func TestBuildRepairPlanIncludesTargetsAndDeletes(t *testing.T) {
	nodes := []planTestNode{
		{id: "nodeA", address: "n1", diskType: "hdd", diskID: 0, shards: makeShardMap(0, 9, 100)},
		{id: "nodeB", address: "n2", diskType: "hdd", diskID: 0, shards: map[uint32]int64{11: 100}},
	}
	topo := buildPlanTopology(nodes)

	activeTopo := buildActiveTopology(t, []string{"n3", "n4", "n5"})
	plan, err := BuildRepairPlan(topo, activeTopo, 500, "collection", "hdd")
	require.NoError(t, err)
	require.NotEmpty(t, plan.MissingShards)
	require.Contains(t, plan.MissingShards, uint32(10))
	require.NotEmpty(t, plan.Targets)
	require.NotEmpty(t, plan.DeleteByNode)
}

func makeShardMap(from, to int, size int64) map[uint32]int64 {
	shards := make(map[uint32]int64)
	for i := from; i <= to; i++ {
		shards[uint32(i)] = size
	}
	return shards
}

func buildPlanTopology(nodes []planTestNode) *master_pb.TopologyInfo {
	dataNodes := make([]*master_pb.DataNodeInfo, 0, len(nodes))
	for _, node := range nodes {
		diskInfo := &master_pb.DiskInfo{
			DiskId:         node.diskID,
			MaxVolumeCount: 100,
			VolumeCount:    10,
		}
		if len(node.shards) > 0 {
			diskInfo.EcShardInfos = []*master_pb.VolumeEcShardInformationMessage{
				buildEcShardInfo(uint32(999), fmt.Sprintf("%s-coll", node.id), node.diskType, node.diskID, node.shards),
			}
		}
		dataNodes = append(dataNodes, &master_pb.DataNodeInfo{
			Id:        node.id,
			Address:   node.address,
			DiskInfos: map[string]*master_pb.DiskInfo{node.diskType: diskInfo},
		})
	}
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id:            "rack1",
						DataNodeInfos: dataNodes,
					},
				},
			},
		},
	}
}

func buildActiveTopology(t *testing.T, nodeIDs []string) *topology.ActiveTopology {
	t.Helper()
	info := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
					},
				},
			},
		},
	}
	rack := info.DataCenterInfos[0].RackInfos[0]
	for _, id := range nodeIDs {
		rack.DataNodeInfos = append(rack.DataNodeInfos, &master_pb.DataNodeInfo{
			Id:      id,
			Address: id,
			DiskInfos: map[string]*master_pb.DiskInfo{
				"hdd": {
					DiskId:         0,
					MaxVolumeCount: 200,
					VolumeCount:    50,
				},
			},
		})
	}

	active := topology.NewActiveTopology(recentTaskWindowSize)
	require.NoError(t, active.UpdateTopology(info))
	return active
}

func buildEcShardInfo(volumeID uint32, collection, diskType string, diskID uint32, shardSizes map[uint32]int64) *master_pb.VolumeEcShardInformationMessage {
	shardIDs := make([]int, 0, len(shardSizes))
	for shardID := range shardSizes {
		shardIDs = append(shardIDs, int(shardID))
	}
	sort.Ints(shardIDs)

	var bits uint32
	sizes := make([]int64, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		bits |= (1 << shardID)
		sizes = append(sizes, shardSizes[uint32(shardID)])
	}
	return &master_pb.VolumeEcShardInformationMessage{
		Id:          volumeID,
		Collection:  collection,
		EcIndexBits: bits,
		DiskType:    diskType,
		DiskId:      diskID,
		ShardSizes:  sizes,
	}
}
