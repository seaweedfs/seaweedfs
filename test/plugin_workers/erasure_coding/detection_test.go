package erasure_coding_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	ecstorage "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type topologySpec struct {
	name         string
	dataCenters  int
	racksPerDC   int
	nodesPerRack int
}

func TestErasureCodingDetectionAcrossTopologies(t *testing.T) {
	cases := []topologySpec{
		{
			name:         "single-dc-multi-rack",
			dataCenters:  1,
			racksPerDC:   2,
			nodesPerRack: 7,
		},
		{
			name:         "multi-dc",
			dataCenters:  2,
			racksPerDC:   1,
			nodesPerRack: 7,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			volumeID := uint32(7)
			response := buildVolumeListResponse(t, tc, volumeID)
			master := pluginworkers.NewMasterServer(t, response)

			dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
			handler := pluginworker.NewErasureCodingHandler(dialOption, t.TempDir())
			harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
				WorkerOptions: pluginworker.WorkerOptions{
					GrpcDialOption: dialOption,
				},
				Handlers: []pluginworker.JobHandler{handler},
			})
			harness.WaitForJobType("erasure_coding")

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			proposals, err := harness.Plugin().RunDetection(ctx, "erasure_coding", &plugin_pb.ClusterContext{
				MasterGrpcAddresses: []string{master.Address()},
			}, 10)
			require.NoError(t, err)
			require.NotEmpty(t, proposals)

			proposal := proposals[0]
			require.Equal(t, "erasure_coding", proposal.JobType)
			paramsValue := proposal.Parameters["task_params_pb"]
			require.NotNil(t, paramsValue)

			params := &worker_pb.TaskParams{}
			require.NoError(t, proto.Unmarshal(paramsValue.GetBytesValue(), params))
			require.NotEmpty(t, params.Sources)
			require.Len(t, params.Targets, ecstorage.TotalShardsCount)
		})
	}
}

func buildVolumeListResponse(t *testing.T, spec topologySpec, volumeID uint32) *master_pb.VolumeListResponse {
	t.Helper()

	volumeSizeLimitMB := uint64(100)
	volumeSize := uint64(90) * 1024 * 1024
	volumeModifiedAt := time.Now().Add(-10 * time.Minute).Unix()

	var dataCenters []*master_pb.DataCenterInfo
	nodeIndex := 0
	volumePlaced := false

	for dc := 0; dc < spec.dataCenters; dc++ {
		var racks []*master_pb.RackInfo
		for rack := 0; rack < spec.racksPerDC; rack++ {
			var nodes []*master_pb.DataNodeInfo
			for n := 0; n < spec.nodesPerRack; n++ {
				nodeIndex++
				address := fmt.Sprintf("127.0.0.1:%d", 20000+nodeIndex)

				diskInfo := &master_pb.DiskInfo{
					DiskId:         0,
					MaxVolumeCount: 100,
					VolumeCount:    0,
					VolumeInfos:    []*master_pb.VolumeInformationMessage{},
				}

				if !volumePlaced {
					diskInfo.VolumeCount = 1
					diskInfo.VolumeInfos = append(diskInfo.VolumeInfos, &master_pb.VolumeInformationMessage{
						Id:               volumeID,
						Collection:       "ec-test",
						DiskId:           0,
						Size:             volumeSize,
						DeletedByteCount: 0,
						ModifiedAtSecond: volumeModifiedAt,
						ReplicaPlacement: 1,
						ReadOnly:         false,
					})
					volumePlaced = true
				}

				nodes = append(nodes, &master_pb.DataNodeInfo{
					Id:        address,
					Address:   address,
					DiskInfos: map[string]*master_pb.DiskInfo{"hdd": diskInfo},
				})
			}

			racks = append(racks, &master_pb.RackInfo{
				Id:            fmt.Sprintf("rack-%d", rack+1),
				DataNodeInfos: nodes,
			})
		}

		dataCenters = append(dataCenters, &master_pb.DataCenterInfo{
			Id:        fmt.Sprintf("dc-%d", dc+1),
			RackInfos: racks,
		})
	}

	return &master_pb.VolumeListResponse{
		VolumeSizeLimitMb: volumeSizeLimitMB,
		TopologyInfo: &master_pb.TopologyInfo{
			DataCenterInfos: dataCenters,
		},
	}
}
