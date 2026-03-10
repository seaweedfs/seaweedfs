package volume_balance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
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

	copyCalls, _, tailCalls := target.BalanceStats()
	require.GreaterOrEqual(t, copyCalls, 1)
	require.GreaterOrEqual(t, tailCalls, 1)
}

func TestVolumeBalanceBatchExecutionIntegration(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := pluginworker.NewVolumeBalanceHandler(dialOption)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{
			GrpcDialOption: dialOption,
		},
		Handlers: []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("volume_balance")

	// Create one source and one target fake volume server.
	source := pluginworkers.NewVolumeServer(t, "")
	target := pluginworkers.NewVolumeServer(t, "")

	// Build a batch job with 3 volume moves from source → target.
	volumeIDs := []uint32{401, 402, 403}
	moves := make([]*worker_pb.BalanceMoveSpec, len(volumeIDs))
	for i, vid := range volumeIDs {
		moves[i] = &worker_pb.BalanceMoveSpec{
			VolumeId:   vid,
			SourceNode: source.Address(),
			TargetNode: target.Address(),
			Collection: "batch-test",
		}
	}

	params := &worker_pb.TaskParams{
		TaskId: "batch-balance-test",
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				MaxConcurrentMoves: 2,
				Moves:              moves,
			},
		},
	}
	paramBytes, err := proto.Marshal(params)
	require.NoError(t, err)

	job := &plugin_pb.JobSpec{
		JobId:   "batch-balance-test",
		JobType: "volume_balance",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"task_params_pb": {
				Kind: &plugin_pb.ConfigValue_BytesValue{BytesValue: paramBytes},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := harness.Plugin().ExecuteJob(ctx, job, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success, "batch balance job should succeed; result: %+v", result)

	// Each of the 3 moves should have marked the source readonly and deleted.
	require.Equal(t, len(volumeIDs), source.MarkReadonlyCount(),
		"each move should mark source volume readonly")
	require.Equal(t, len(volumeIDs), len(source.DeleteRequests()),
		"each move should delete the source volume")

	// Verify delete requests reference the expected volume IDs.
	deletedVols := make(map[uint32]bool)
	for _, req := range source.DeleteRequests() {
		deletedVols[req.VolumeId] = true
	}
	for _, vid := range volumeIDs {
		require.True(t, deletedVols[vid], "volume %d should have been deleted from source", vid)
	}

	// Pre-delete verification should have called ReadVolumeFileStatus on both
	// source and target for each volume.
	require.Equal(t, len(volumeIDs), source.ReadFileStatusCount(),
		"each move should read source volume status before delete")
	require.Equal(t, len(volumeIDs), target.ReadFileStatusCount(),
		"each move should read target volume status before delete")

	// Target should have received copy and tail calls for all 3 volumes.
	copyCalls, _, tailCalls := target.BalanceStats()
	require.Equal(t, len(volumeIDs), copyCalls, "target should receive one copy per volume")
	require.Equal(t, len(volumeIDs), tailCalls, "target should receive one tail per volume")
}
