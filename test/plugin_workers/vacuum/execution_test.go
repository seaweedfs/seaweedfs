package vacuum_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	pluginworkers "github.com/seaweedfs/seaweedfs/test/plugin_workers"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func vacuumJobSpec(volumeID uint32, server string) *plugin_pb.JobSpec {
	return &plugin_pb.JobSpec{
		JobId:   fmt.Sprintf("vacuum-job-%d", volumeID),
		JobType: "vacuum",
		Parameters: map[string]*plugin_pb.ConfigValue{
			"volume_id": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(volumeID)},
			},
			"server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: server},
			},
			"collection": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: "vac-test"},
			},
		},
	}
}

func TestVacuumExecutionIntegration(t *testing.T) {
	volumeID := uint32(202)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := vacuum.NewVacuumHandler(dialOption, 1)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{
			GrpcDialOption: dialOption,
		},
		Handlers: []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("vacuum")

	source := pluginworkers.NewVolumeServer(t, "")
	source.SetVacuumGarbageRatio(0.6)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := harness.Plugin().ExecuteJob(ctx, vacuumJobSpec(volumeID, source.Address()), nil, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success)

	checkCalls, compactCalls, commitCalls, cleanupCalls := source.VacuumStats()
	require.GreaterOrEqual(t, checkCalls, 2)
	require.GreaterOrEqual(t, compactCalls, 1)
	require.GreaterOrEqual(t, commitCalls, 1)
	// Cleanup is only invoked when Phase 1 (Compact) fails to roll back
	// the .cpd/.cpx/.cpldb temp files; on the success path Commit
	// consumes them (rename .cpd → .dat, .cpx → .idx, .cpldb → .ldb via
	// the leveldb needle map) so no Cleanup call is needed. Matches
	// topology.vacuumOneVolumeId which only calls batchVacuumVolumeCleanup
	// on the Compact-failure branch.
	require.Equal(t, 0, cleanupCalls)
	// Phase 3 marks each replica writable so master returns it to the
	// writables layout. See upstream seaweedfs#9685.
	require.GreaterOrEqual(t, source.MarkWritableCount(), 1)
}

// A replica that commits still read-only (operator-set, EIO-quarantined,
// disk-space-low) must not be force-marked writable: master built-in vacuum
// skips it via SetVolumeAvailable, and it recovers on its own ReadOnly=false
// heartbeat.
func TestVacuumExecutionSkipsMarkWritableWhenReadOnly(t *testing.T) {
	volumeID := uint32(203)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handler := vacuum.NewVacuumHandler(dialOption, 1)
	harness := pluginworkers.NewHarness(t, pluginworkers.HarnessConfig{
		WorkerOptions: pluginworker.WorkerOptions{
			GrpcDialOption: dialOption,
		},
		Handlers: []pluginworker.JobHandler{handler},
	})
	harness.WaitForJobType("vacuum")

	source := pluginworkers.NewVolumeServer(t, "")
	source.SetVacuumGarbageRatio(0.6)
	source.SetVacuumCommitReadOnly(true)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := harness.Plugin().ExecuteJob(ctx, vacuumJobSpec(volumeID, source.Address()), nil, 1)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Success)

	_, _, commitCalls, _ := source.VacuumStats()
	require.GreaterOrEqual(t, commitCalls, 1)
	require.Equal(t, 0, source.MarkWritableCount())
}
