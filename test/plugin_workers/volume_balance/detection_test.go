package volume_balance_test

import (
	"context"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestVolumeBalanceDetectionIntegration(t *testing.T) {
	response := buildBalanceVolumeListResponse(t)
	master := pluginworkers.NewMasterServer(t, response)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := pluginworker.NewVolumeBalanceHandler(dialOption)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{
			GrpcDialOption: dialOption,
		},
		Handlers: []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("volume_balance")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	proposals, err := harness.Plugin().RunDetection(ctx, "volume_balance", &plugin_pb.ClusterContext{
		MasterGrpcAddresses: []string{master.Address()},
	}, 10)
	require.NoError(t, err)
	require.Len(t, proposals, 1)

	proposal := proposals[0]
	require.Equal(t, "volume_balance", proposal.JobType)
	paramsValue := proposal.Parameters["task_params_pb"]
	require.NotNil(t, paramsValue)

	params := &worker_pb.TaskParams{}
	require.NoError(t, proto.Unmarshal(paramsValue.GetBytesValue(), params))
	require.NotEmpty(t, params.Sources)
	require.NotEmpty(t, params.Targets)
}

func buildBalanceVolumeListResponse(t *testing.T) *master_pb.VolumeListResponse {
	t.Helper()

	volumeSizeLimitMB := uint64(100)
	volumeModifiedAt := time.Now().Add(-2 * time.Hour).Unix()

	overloadedVolumes := make([]*master_pb.VolumeInformationMessage, 0, 10)
	for i := 0; i < 10; i++ {
		volumeID := uint32(1000 + i)
		overloadedVolumes = append(overloadedVolumes, &master_pb.VolumeInformationMessage{
			Id:               volumeID,
			Collection:       "balance",
			DiskId:           0,
			Size:             20 * 1024 * 1024,
			DeletedByteCount: 0,
			ModifiedAtSecond: volumeModifiedAt,
			ReplicaPlacement: 1,
			ReadOnly:         false,
		})
	}

	underloadedVolumes := []*master_pb.VolumeInformationMessage{
		{
			Id:               2000,
			Collection:       "balance",
			DiskId:           0,
			Size:             20 * 1024 * 1024,
			DeletedByteCount: 0,
			ModifiedAtSecond: volumeModifiedAt,
			ReplicaPlacement: 1,
			ReadOnly:         false,
		},
	}

	overloadedDisk := &master_pb.DiskInfo{
		DiskId:         0,
		MaxVolumeCount: 100,
		VolumeCount:    int64(len(overloadedVolumes)),
		VolumeInfos:    overloadedVolumes,
	}

	underloadedDisk := &master_pb.DiskInfo{
		DiskId:         0,
		MaxVolumeCount: 100,
		VolumeCount:    int64(len(underloadedVolumes)),
		VolumeInfos:    underloadedVolumes,
	}

	overloadedNode := &master_pb.DataNodeInfo{
		Id:        "10.0.0.1:8080",
		Address:   "10.0.0.1:8080",
		DiskInfos: map[string]*master_pb.DiskInfo{"hdd": overloadedDisk},
	}

	underloadedNode := &master_pb.DataNodeInfo{
		Id:        "10.0.0.2:8080",
		Address:   "10.0.0.2:8080",
		DiskInfos: map[string]*master_pb.DiskInfo{"hdd": underloadedDisk},
	}

	rack := &master_pb.RackInfo{
		Id:            "rack-1",
		DataNodeInfos: []*master_pb.DataNodeInfo{overloadedNode, underloadedNode},
	}

	return &master_pb.VolumeListResponse{
		VolumeSizeLimitMb: volumeSizeLimitMB,
		TopologyInfo: &master_pb.TopologyInfo{
			DataCenterInfos: []*master_pb.DataCenterInfo{
				{
					Id:        "dc-1",
					RackInfos: []*master_pb.RackInfo{rack},
				},
			},
		},
	}
}
