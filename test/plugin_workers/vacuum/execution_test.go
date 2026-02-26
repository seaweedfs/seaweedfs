package vacuum_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestVacuumExecutionIntegration(t *testing.T) {
	volumeID := uint32(202)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := pluginworker.NewVacuumHandler(dialOption, 1)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{
			GrpcDialOption: dialOption,
		},
		Handlers: []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("vacuum")

	source := pluginworkers.NewVolumeServer(t, "")
	source.SetVacuumGarbageRatio(0.6)

	job := &plugin_pb.JobSpec{
		JobId:   fmt.Sprintf("vacuum-job-%d", volumeID),
		JobType: "vacuum",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)},
			},
			"server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: source.Address()},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "vac-test"},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := harness.Plugin().ExecuteJob(ctx, job, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success)

	checkCalls, compactCalls, commitCalls, cleanupCalls := source.VacuumStats()
	require.GreaterOrEqual(t, checkCalls, 2)
	require.GreaterOrEqual(t, compactCalls, 1)
	require.GreaterOrEqual(t, commitCalls, 1)
	require.GreaterOrEqual(t, cleanupCalls, 1)
}
