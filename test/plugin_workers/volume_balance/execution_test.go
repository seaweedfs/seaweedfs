package volume_balance_test

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

func TestVolumeBalanceExecutionIntegration(t *testing.T) {
	volumeID := uint32(303)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := pluginworker.NewVolumeBalanceHandler(dialOption)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{
			GrpcDialOption: dialOption,
		},
		Handlers: []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("volume_balance")

	source := pluginworkers.NewVolumeServer(t, "")
	target := pluginworkers.NewVolumeServer(t, "")

	job := &plugin_pb.JobSpec{
		JobId:   fmt.Sprintf("balance-job-%d", volumeID),
		JobType: "volume_balance",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "balance"},
			},
			"source_server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: source.Address()},
			},
			"target_server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: target.Address()},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := harness.Plugin().ExecuteJob(ctx, job, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success)

	require.GreaterOrEqual(t, source.MarkReadonlyCount(), 1)
	require.GreaterOrEqual(t, len(source.DeleteRequests()), 1)

	copyCalls, mountCalls, tailCalls := target.BalanceStats()
	require.GreaterOrEqual(t, copyCalls, 1)
	require.GreaterOrEqual(t, mountCalls, 1)
	require.GreaterOrEqual(t, tailCalls, 1)
}
