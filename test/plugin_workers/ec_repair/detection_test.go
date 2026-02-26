package ec_repair_test

import (
	"context"
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
