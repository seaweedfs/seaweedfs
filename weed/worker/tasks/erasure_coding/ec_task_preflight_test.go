package erasure_coding

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Reproduces the swtest interrupted-encode scenario at the task boundary: a
// previous encode of this volume was cut mid-distribute, leaving partial EC
// shards mounted on a destination. When a fresh encode task starts, its Step 0
// preflight (ensureCleanEcStart) must clear those shards before any
// destructive step, so distribute's ReceiveFile is not refused and no orphan
// shards survive to confuse the volume-server loader or make detection refuse
// the volume. The destination is named only as a target (no source row) to
// prove the preflight reaches the full write set, not just shard-bearing
// sources.
func TestEnsureCleanEcStartClearsStaleShardsBeforeEncode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const (
		volumeID   = uint32(94782)
		collection = "ec-preflight-invariant"
	)

	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 9478200, 0x9478BEEF)
	upResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid,
		[]byte("payload-for-preflight-stale-ec-cleanup"))
	_ = framework.ReadAllAndClose(t, upResp)
	require.Equal(t, http.StatusCreated, upResp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID, Collection: collection,
	})
	require.NoError(t, err)

	// A half-finished previous distribute left a partial shard set mounted.
	staleShards := []uint32{0, 1, 2}
	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: collection,
		ShardIds: staleShards,
	})
	require.NoError(t, err)

	shardPath := makeTinyEcShardFile(t)

	// Precondition for the reproduction: the mounted partial EC blocks a fresh
	// ReceiveFile via the mounted-volume guard.
	err = sendShardViaReceiveFile(ctx, grpcClient, volumeID, collection, 0, shardPath)
	require.Error(t, err, "expected ReceiveFile to be refused while stale EC volume is mounted")

	task := NewErasureCodingTask(
		"preflight-invariant",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		collection,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	// Named only as a target — no source row for this node. The preflight must
	// still clear its stale shards.
	task.targets = []*worker_pb.TaskTarget{
		{Node: clusterHarness.VolumeServerAddress(), VolumeId: volumeID, ShardIds: []uint32{0}},
	}

	require.NoError(t, task.ensureCleanEcStart(ctx),
		"Step 0 preflight must clear stale EC shards at task start")

	_, infoErr := grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: volumeID})
	require.Error(t, infoErr, "stale EC volume must be gone after the preflight")

	require.NoError(t,
		sendShardViaReceiveFile(ctx, grpcClient, volumeID, collection, 0, shardPath),
		"ReceiveFile must succeed once the preflight has cleared the stale shards")
}

// A post-distribute failure that has no successor to clean up after it (a
// single-attempt job, or the last of a retry series) must leave nothing behind:
// rollbackDistribute tears down the shards this attempt wrote while leaving the
// source volume intact for a future re-encode. Reproduces the swtest finding
// that a terminally-failed encode stranded orphan shards.
func TestRollbackDistributeClearsShardsAndKeepsSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const (
		volumeID   = uint32(94783)
		collection = "ec-rollback-distribute"
	)

	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 9478300, 0x9478D00D)
	upResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid,
		[]byte("payload-for-rollback-distribute"))
	_ = framework.ReadAllAndClose(t, upResp)
	require.Equal(t, http.StatusCreated, upResp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID, Collection: collection,
	})
	require.NoError(t, err)
	// Shards this attempt "distributed" and mounted on the destination.
	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: collection,
		ShardIds: []uint32{0, 1, 2, 3},
	})
	require.NoError(t, err)

	task := NewErasureCodingTask(
		"rollback-distribute",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		collection,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	task.targets = []*worker_pb.TaskTarget{
		{Node: clusterHarness.VolumeServerAddress(), VolumeId: volumeID, ShardIds: []uint32{0}},
	}

	// Simulate the failure path after distribute began.
	task.rollbackDistribute(ctx)

	_, infoErr := grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: volumeID})
	require.Error(t, infoErr, "rollback must tear down the shards this attempt distributed")

	// The source normal volume must survive so a future re-encode can proceed.
	_, statusErr := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	require.NoError(t, statusErr, "rollback must leave the source volume intact")
}

// A malformed plan (no targets) must be rejected by the preflight before the
// source is marked readonly or copied, so a bad plan cannot leave the source
// fenced readonly with nothing to show for it. Pure precondition check: no
// cluster and no RPC.
func TestEnsureCleanEcStartRejectsMissingTargets(t *testing.T) {
	task := NewErasureCodingTask(
		"preflight-no-targets",
		"10.0.0.1:8080",
		42,
		"c",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	// No targets set.
	err := task.ensureCleanEcStart(context.Background())
	require.Error(t, err, "preflight must reject an encode with no shard targets")
	require.True(t, strings.Contains(err.Error(), "no EC shard targets"),
		"error must name the missing-targets invariant, got: %v", err)
}

// A non-empty target slice whose entries are malformed (nil, empty Node, or no
// assigned shards) must also be rejected before the source is marked readonly —
// cleanupStaleEcShards silently skips such entries, so a length check alone
// would let the encode proceed with nothing to distribute to.
func TestEnsureCleanEcStartRejectsMalformedTarget(t *testing.T) {
	cases := map[string][]*worker_pb.TaskTarget{
		"nil target": {nil},
		"empty node": {{Node: "", VolumeId: 42, ShardIds: []uint32{0}}},
		"no shards":  {{Node: "10.0.0.2:8080", VolumeId: 42}},
		"one good one bad": {
			{Node: "10.0.0.2:8080", VolumeId: 42, ShardIds: []uint32{0}},
			{Node: "", VolumeId: 42, ShardIds: []uint32{1}},
		},
	}
	for name, targets := range cases {
		t.Run(name, func(t *testing.T) {
			task := NewErasureCodingTask(
				"preflight-malformed-target",
				"10.0.0.1:8080",
				42,
				"c",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			task.targets = targets
			err := task.ensureCleanEcStart(context.Background())
			require.Error(t, err, "preflight must reject a malformed target")
			require.True(t, strings.Contains(err.Error(), "malformed EC shard target"),
				"error must name the malformed-target invariant, got: %v", err)
		})
	}
}

// A plan with targets but no source replica and no assigned server must be
// rejected before any destructive step.
func TestEnsureCleanEcStartRejectsMissingSource(t *testing.T) {
	task := NewErasureCodingTask(
		"preflight-no-source",
		"", // no assigned server
		42,
		"c",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	task.targets = []*worker_pb.TaskTarget{
		{Node: "10.0.0.2:8080", VolumeId: 42, ShardIds: []uint32{0}},
	}
	// No sources and no server.
	err := task.ensureCleanEcStart(context.Background())
	require.Error(t, err, "preflight must reject an encode with no source replica")
	require.True(t, strings.Contains(err.Error(), "no source replica"),
		"error must name the missing-source invariant, got: %v", err)
}
