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

const (
	planTestVolumeID     = uint32(999)
	planTestCollection   = "test-collection"
	recentTaskWindowSize = 10
)

type planTestNode struct {
	id       string
	address  string
	diskType string
	diskID   uint32
	shards   map[uint32]int64
}

type multiVolumeSpec struct {
	nodeID     string
	address    string
	diskType   string
	diskID     uint32
	volumeID   uint32
	collection string
	shards     map[uint32]int64
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
				0: 90, // mismatched size
				2: 100,
				3: 100,
				4: 100,
				5: 100,
				6: 100,
			},
		},
	}

	topo := buildPlanTopology(nodes, planTestVolumeID, planTestCollection)
	candidates, hasMore, err := Detect(topo, "", 0)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, candidates, 1)

	candidate := candidates[0]
	require.Greater(t, candidate.ExtraShards, 0)
	require.Greater(t, candidate.MismatchedShards, 0)
}

func TestDetectHonorsMaxResults(t *testing.T) {
	specs := []multiVolumeSpec{
		{
			nodeID:     "volume100-node1",
			address:    "127.0.0.1:9200",
			diskType:   "hdd",
			diskID:     0,
			volumeID:   100,
			collection: planTestCollection,
			shards:     makeShardMap(0, 9, 100),
		},
		{
			nodeID:     "volume100-node2",
			address:    "127.0.0.1:9201",
			diskType:   "hdd",
			diskID:     1,
			volumeID:   100,
			collection: planTestCollection,
			shards: map[uint32]int64{
				0: 50,
			},
		},
		{
			nodeID:     "volume101-node1",
			address:    "127.0.0.1:9300",
			diskType:   "hdd",
			diskID:     0,
			volumeID:   101,
			collection: planTestCollection,
			shards:     makeShardMap(0, 9, 100),
		},
		{
			nodeID:     "volume101-node2",
			address:    "127.0.0.1:9301",
			diskType:   "hdd",
			diskID:     1,
			volumeID:   101,
			collection: planTestCollection,
			shards: map[uint32]int64{
				1: 90,
			},
		},
		{
			nodeID:     "volume102-node1",
			address:    "127.0.0.1:9400",
			diskType:   "hdd",
			diskID:     0,
			volumeID:   102,
			collection: planTestCollection,
			shards:     makeShardMap(0, 9, 100),
		},
		{
			nodeID:     "volume102-node2",
			address:    "127.0.0.1:9401",
			diskType:   "hdd",
			diskID:     1,
			volumeID:   102,
			collection: planTestCollection,
			shards: map[uint32]int64{
				2: 50,
			},
		},
	}

	topo := buildMultiVolumeTopology(specs)

	candidates, hasMore, err := Detect(topo, "", 2)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, candidates, 2)
}

func TestBuildRepairPlanRequiresEnoughShards(t *testing.T) {
	nodes := []planTestNode{
		{id: "nodeA", address: "n1", diskType: "hdd", diskID: 0, shards: makeShardMap(0, 4, 100)},
	}
	topo := buildPlanTopology(nodes, planTestVolumeID, planTestCollection)

	_, err := BuildRepairPlan(topo, nil, planTestVolumeID, planTestCollection, "hdd")
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("need at least %d", erasure_coding.DataShardsCount))
}

func TestBuildRepairPlanIncludesTargetsAndDeletes(t *testing.T) {
	nodes := []planTestNode{
		{id: "nodeA", address: "n1", diskType: "hdd", diskID: 0, shards: makeShardMap(0, 9, 100)},
		{id: "nodeB", address: "n2", diskType: "hdd", diskID: 0, shards: map[uint32]int64{0: 50}},
	}
	topo := buildPlanTopology(nodes, planTestVolumeID, planTestCollection)

	activeTopo := buildActiveTopology(t, []string{"n3", "n4", "n5", "n6"})
	plan, err := BuildRepairPlan(topo, activeTopo, planTestVolumeID, planTestCollection, "hdd")
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

func buildPlanTopology(nodes []planTestNode, volumeID uint32, collection string) *master_pb.TopologyInfo {
	dataNodes := make([]*master_pb.DataNodeInfo, 0, len(nodes))
	for _, node := range nodes {
		diskInfo := &master_pb.DiskInfo{
			DiskId:         node.diskID,
			MaxVolumeCount: 100,
			VolumeCount:    10,
		}
		if len(node.shards) > 0 {
			diskInfo.EcShardInfos = []*master_pb.VolumeEcShardInformationMessage{
				buildEcShardInfo(volumeID, collection, node.diskType, node.diskID, node.shards),
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

func buildMultiVolumeTopology(specs []multiVolumeSpec) *master_pb.TopologyInfo {
	dataNodes := make([]*master_pb.DataNodeInfo, 0, len(specs))
	for _, spec := range specs {
		diskInfo := &master_pb.DiskInfo{
			DiskId:         spec.diskID,
			MaxVolumeCount: 100,
			VolumeCount:    10,
		}
		if len(spec.shards) > 0 {
			diskInfo.EcShardInfos = []*master_pb.VolumeEcShardInformationMessage{
				buildEcShardInfo(spec.volumeID, spec.collection, spec.diskType, spec.diskID, spec.shards),
			}
		}
		dataNodes = append(dataNodes, &master_pb.DataNodeInfo{
			Id:        spec.nodeID,
			Address:   spec.address,
			DiskInfos: map[string]*master_pb.DiskInfo{spec.diskType: diskInfo},
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
