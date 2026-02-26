package erasure_coding_test

import (
	"context"
	"fmt"
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

func TestErasureCodingDetectionLargeTopology(t *testing.T) {
	const (
		rackCount       = 100
		serverCount     = 1000
		volumesPerNode  = 300
		volumeSizeLimit = uint64(100)
	)

	if serverCount%rackCount != 0 {
		t.Fatalf("serverCount (%d) must be divisible by rackCount (%d)", serverCount, rackCount)
	}

	nodesPerRack := serverCount / rackCount
	eligibleSize := uint64(90) * 1024 * 1024
	ineligibleSize := uint64(10) * 1024 * 1024
	modifiedAt := time.Now().Add(-10 * time.Minute).Unix()

	volumeID := uint32(1)
	dataCenters := make([]*master_pb.DataCenterInfo, 0, 1)

	racks := make([]*master_pb.RackInfo, 0, rackCount)
	for rack := 0; rack < rackCount; rack++ {
		nodes := make([]*master_pb.DataNodeInfo, 0, nodesPerRack)
		for node := 0; node < nodesPerRack; node++ {
			address := fmt.Sprintf("10.0.%d.%d:8080", rack, node+1)
			volumes := make([]*master_pb.VolumeInformationMessage, 0, volumesPerNode)
			for v := 0; v < volumesPerNode; v++ {
				size := ineligibleSize
				if volumeID%2 == 0 {
					size = eligibleSize
				}
				volumes = append(volumes, &master_pb.VolumeInformationMessage{
					Id:               volumeID,
					Collection:       "ec-bulk",
					DiskId:           0,
					Size:             size,
					DeletedByteCount: 0,
					ModifiedAtSecond: modifiedAt,
					ReplicaPlacement: 1,
					ReadOnly:         false,
				})
				volumeID++
			}

			diskInfo := &master_pb.DiskInfo{
				DiskId:         0,
				MaxVolumeCount: int64(volumesPerNode + 10),
				VolumeCount:    int64(volumesPerNode),
				VolumeInfos:    volumes,
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
		Id:        "dc-1",
		RackInfos: racks,
	})

	response := &master_pb.VolumeListResponse{
		VolumeSizeLimitMb: volumeSizeLimit,
		TopologyInfo: &master_pb.TopologyInfo{
			DataCenterInfos: dataCenters,
		},
	}

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

	totalVolumes := serverCount * volumesPerNode
	expectedEligible := totalVolumes / 2

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	start := time.Now()
	proposals, err := harness.Plugin().RunDetection(ctx, "erasure_coding", &plugin_pb.ClusterContext{
		MasterGrpcAddresses: []string{master.Address()},
	}, 0)
	duration := time.Since(start)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(proposals), 10, "should detect at least some proposals")
	t.Logf("large topology detection completed in %s (proposals=%d, eligible=%d)", duration, len(proposals), expectedEligible)
	if len(proposals) < expectedEligible {
		t.Logf("large topology detection stopped early: %d proposals vs %d eligible", len(proposals), expectedEligible)
	}
}
