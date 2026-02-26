package vacuum_test

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

func TestVacuumDetectionIntegration(t *testing.T) {
	volumeID := uint32(101)
	source := pluginworkers.NewVolumeServer(t, "")

	response := buildVacuumVolumeListResponse(t, source.Address(), volumeID, 0.6)
	master := pluginworkers.NewMasterServer(t, response)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := pluginworker.NewVacuumHandler(dialOption, 1)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{
			GrpcDialOption: dialOption,
		},
		Handlers: []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("vacuum")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	proposals, err := harness.Plugin().RunDetection(ctx, "vacuum", &plugin_pb.ClusterContext{
		MasterGrpcAddresses: []string{master.Address()},
	}, 10)
	require.NoError(t, err)
	require.NotEmpty(t, proposals)

	proposal := proposals[0]
	require.Equal(t, "vacuum", proposal.JobType)
	paramsValue := proposal.Parameters["task_params_pb"]
	require.NotNil(t, paramsValue)

	params := &worker_pb.TaskParams{}
	require.NoError(t, proto.Unmarshal(paramsValue.GetBytesValue(), params))
	require.NotEmpty(t, params.Sources)
	require.NotNil(t, params.GetVacuumParams())
}

func buildVacuumVolumeListResponse(t *testing.T, serverAddress string, volumeID uint32, garbageRatio float64) *master_pb.VolumeListResponse {
	t.Helper()

	volumeSizeLimitMB := uint64(100)
	volumeSize := uint64(90) * 1024 * 1024
	deletedBytes := uint64(float64(volumeSize) * garbageRatio)
	volumeModifiedAt := time.Now().Add(-48 * time.Hour).Unix()

	diskInfo := &master_pb.DiskInfo{
		DiskId:         0,
		MaxVolumeCount: 100,
		VolumeCount:    1,
		VolumeInfos: []*master_pb.VolumeInformationMessage{
			{
				Id:               volumeID,
				Collection:       "vac-test",
				DiskId:           0,
				Size:             volumeSize,
				DeletedByteCount: deletedBytes,
				ModifiedAtSecond: volumeModifiedAt,
				ReplicaPlacement: 1,
				ReadOnly:         false,
			},
		},
	}

	node := &master_pb.DataNodeInfo{
		Id:        serverAddress,
		Address:   serverAddress,
		DiskInfos: map[string]*master_pb.DiskInfo{"hdd": diskInfo},
	}

	return &master_pb.VolumeListResponse{
		VolumeSizeLimitMb: volumeSizeLimitMB,
		TopologyInfo: &master_pb.TopologyInfo{
			DataCenterInfos: []*master_pb.DataCenterInfo{
				{
					Id: "dc-1",
					RackInfos: []*master_pb.RackInfo{
						{
							Id:            "rack-1",
							DataNodeInfos: []*master_pb.DataNodeInfo{node},
						},
					},
				},
			},
		},
	}
}
