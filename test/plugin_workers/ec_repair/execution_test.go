package ec_repair_test

import (
	"context"
	"fmt"
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

type execNodeSpec struct {
	id       string
	address  string
	diskType string
	diskID   uint32
	ecShards []*master_pb.VolumeEcShardInformationMessage
}

func TestEcRepairExecutionRepairsShards(t *testing.T) {
	const (
		volumeID   = uint32(200)
		collection = "ec-repair"
		diskType   = "hdd"
	)

	volumeServers := []*pluginworkers.VolumeServer{
		pluginworkers.NewVolumeServer(t, ""),
		pluginworkers.NewVolumeServer(t, ""),
		pluginworkers.NewVolumeServer(t, ""),
		pluginworkers.NewVolumeServer(t, ""),
	}

	node1 := execNodeSpec{
		id:       "node1",
		address:  volumeServers[0].Address(),
		diskType: diskType,
		diskID:   0,
		ecShards: []*master_pb.VolumeEcShardInformationMessage{
			buildEcShardInfoExec(volumeID, collection, diskType, 0, map[uint32]int64{
				0: 100,
				1: 100,
				2: 100,
				3: 100,
				4: 100,
			}),
		},
	}

	node2 := execNodeSpec{
		id:       "node2",
		address:  volumeServers[1].Address(),
		diskType: diskType,
		diskID:   0,
		ecShards: []*master_pb.VolumeEcShardInformationMessage{
			buildEcShardInfoExec(volumeID, collection, diskType, 0, map[uint32]int64{
				0: 50,
				5: 100,
				6: 100,
				7: 100,
				8: 100,
				9: 100,
			}),
		},
	}

	node3 := execNodeSpec{
		id:       "node3",
		address:  volumeServers[2].Address(),
		diskType: diskType,
		diskID:   0,
	}

	node4 := execNodeSpec{
		id:       "node4",
		address:  volumeServers[3].Address(),
		diskType: diskType,
		diskID:   0,
	}

	topo := buildExecTopology([]execNodeSpec{node1, node2, node3, node4})
	response := &master_pb.VolumeListResponse{TopologyInfo: topo}
	master := pluginworkers.NewMasterServer(t, response)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := pluginworker.NewEcRepairHandler(dialOption)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{GrpcDialOption: dialOption},
		Handlers:      []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("ec_repair")

	job := &plugin_pb.JobSpec{
		JobId:   fmt.Sprintf("ec-repair-%d", volumeID),
		JobType: "ec_repair",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: collection},
			},
			"disk_type": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: diskType},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := harness.Plugin().ExecuteJob(ctx, job, &plugin_pb.ClusterContext{
		MasterGrpcAddresses: []string{master.Address()},
	}, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success)

	rebuildCalls := 0
	copyCalls := 0
	deleteCalls := 0
	unmountCalls := 0
	for _, vs := range volumeServers {
		rebuildCalls += len(vs.RebuildRequests())
		copyCalls += len(vs.CopyRequests())
		deleteCalls += len(vs.EcDeleteRequests())
		unmountCalls += len(vs.UnmountRequests())
	}

	require.Greater(t, rebuildCalls, 0)
	require.Greater(t, copyCalls, 0)
	require.Greater(t, deleteCalls, 0)
	require.Greater(t, unmountCalls, 0)
}

func buildExecTopology(nodes []execNodeSpec) *master_pb.TopologyInfo {
	dataNodes := make([]*master_pb.DataNodeInfo, 0, len(nodes))
	for _, node := range nodes {
		diskInfo := &master_pb.DiskInfo{
			DiskId:         node.diskID,
			MaxVolumeCount: 200,
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

func buildEcShardInfoExec(volumeID uint32, collection, diskType string, diskID uint32, shardSizes map[uint32]int64) *master_pb.VolumeEcShardInformationMessage {
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
