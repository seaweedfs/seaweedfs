package erasure_coding_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	ecstorage "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestErasureCodingExecutionEncodesShards(t *testing.T) {
	volumeID := uint32(123)
	datSize := 1 * 1024 * 1024

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := pluginworker.NewErasureCodingHandler(dialOption, t.TempDir())
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{
			GrpcDialOption: dialOption,
		},
		Handlers: []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("erasure_coding")

	sourceServer := pluginworkers.NewVolumeServer(t, "")
	pluginworkers.WriteTestVolumeFiles(t, sourceServer.BaseDir(), volumeID, datSize)

	targetServers := make([]*pluginworkers.VolumeServer, 0, ecstorage.TotalShardsCount)
	targetAddresses := make([]string, 0, ecstorage.TotalShardsCount)
	for i := 0; i < ecstorage.TotalShardsCount; i++ {
		target := pluginworkers.NewVolumeServer(t, "")
		targetServers = append(targetServers, target)
		targetAddresses = append(targetAddresses, target.Address())
	}

	job := &plugin_pb.JobSpec{
		JobId:   fmt.Sprintf("ec-job-%d", volumeID),
		JobType: "erasure_coding",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "ec-test"},
			},
			"source_server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: sourceServer.Address()},
			},
			"target_servers": {
				Kind: &plugin_pb.ConfigValue_StringList{StringList: &plugin_pb.StringList{Values: targetAddresses}},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := harness.Plugin().ExecuteJob(ctx, job, nil, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success)

	require.GreaterOrEqual(t, sourceServer.MarkReadonlyCount(), 1)
	require.GreaterOrEqual(t, len(sourceServer.DeleteRequests()), 1)

	for shardID := 0; shardID < ecstorage.TotalShardsCount; shardID++ {
		targetIndex := shardID % len(targetServers)
		target := targetServers[targetIndex]
		expected := filepath.Join(target.BaseDir(), fmt.Sprintf("%d.ec%02d", volumeID, shardID))
		info, err := os.Stat(expected)
		require.NoErrorf(t, err, "missing shard file %s", expected)
		require.Greater(t, info.Size(), int64(0))
	}
}
