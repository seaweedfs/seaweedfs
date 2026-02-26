package ec_repair_test

import (
	"context"
	"sort"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type nodeSpec struct {
	id       string
	address  string
	diskType string
	diskID   uint32
	ecShards []*master_pb.VolumeEcShardInformationMessage
}

func TestEcRepairDetectionFindsCandidates(t *testing.T) {
	const (
		volumeID   = uint32(100)
		collection = "ec-test"
		diskType   = "hdd"
	)

	node1 := nodeSpec{
		id:       "node1",
		address:  "127.0.0.1:11001",
		diskType: diskType,
		diskID:   0,
		ecShards: []*master_pb.VolumeEcShardInformationMessage{
			buildEcShardInfo(volumeID, collection, diskType, 0, map[uint32]int64{
				0: 100,
				1: 100,
				2: 100,
				3: 100,
				4: 100,
			}),
		},
	}

	node2 := nodeSpec{
		id:       "node2",
		address:  "127.0.0.1:11002",
		diskType: diskType,
		diskID:   0,
		ecShards: []*master_pb.VolumeEcShardInformationMessage{
			buildEcShardInfo(volumeID, collection, diskType, 0, map[uint32]int64{
				0: 50,
				5: 100,
				6: 100,
				7: 100,
				8: 100,
				9: 100,
			}),
		},
	}

	topo := buildTopology([]nodeSpec{node1, node2})
	response := &master_pb.VolumeListResponse{TopologyInfo: topo}
	master := pluginworkers.NewMasterServer(t, response)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := pluginworker.NewEcRepairHandler(dialOption)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{GrpcDialOption: dialOption},
		Handlers:      []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("ec_repair")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	proposals, err := harness.Plugin().RunDetection(ctx, "ec_repair", &plugin_pb.ClusterContext{
		MasterGrpcAddresses: []string{master.Address()},
	}, 10)
	require.NoError(t, err)
	require.NotEmpty(t, proposals)
	require.Equal(t, "ec_repair", proposals[0].JobType)
}

func buildTopology(nodes []nodeSpec) *master_pb.TopologyInfo {
	dataNodes := make([]*master_pb.DataNodeInfo, 0, len(nodes))
	for _, node := range nodes {
		diskInfo := &master_pb.DiskInfo{
			DiskId:         node.diskID,
			MaxVolumeCount: 100,
			VolumeCount:    10,
			EcShardInfos:   node.ecShards,
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
