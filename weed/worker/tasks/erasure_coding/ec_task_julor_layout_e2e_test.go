package erasure_coding

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A real .dat source plus a 0-byte stub replica left by an interrupted encode
// and partial stale EC shards: empty live-idx vs non-empty must refuse encode
// (no union, no skip).
func TestEcEncodeJulorLayoutRefuses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const disksPerServer = 2
	cluster := framework.StartMultiVolumeClusterWithDisks(t, matrix.P1(), 3, disksPerServer)
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	const (
		volumeID   = uint32(9500)
		collection = "ec-julor"
	)
	// S0: real source. S1: 0-byte stub + EC target. S2: pure EC target.
	const (
		srcServer  = 0
		stubServer = 1
	)
	addr := func(i int) string { return serverAddress(cluster, i) }

	conns := make([]*grpc.ClientConn, 3)
	clients := make([]volume_server_pb.VolumeServerClient, 3)
	for i := 0; i < 3; i++ {
		conns[i], clients[i] = framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(i))
		defer conns[i].Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// S0: real source replica with data.
	framework.AllocateVolume(t, clients[srcServer], volumeID, collection)
	httpClient := framework.NewHTTPClient()
	for i := 0; i < 8; i++ {
		fid := framework.NewFileID(volumeID, uint64(950000+i), 0x9500CA00+uint32(i))
		payload := make([]byte, 4096)
		for j := range payload {
			payload[j] = byte(i + 1)
		}
		resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(srcServer), fid, payload)
		_ = framework.ReadAllAndClose(t, resp)
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}

	// Partial stale EC shards from a previous interrupted encode, mounted on the
	// source. cleanupStaleEcShards must clear them before re-distribute.
	_, err := clients[srcServer].VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID, Collection: collection,
	})
	require.NoError(t, err)
	staleShards := []uint32{11, 12, 13}
	_, err = clients[srcServer].VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: collection, ShardIds: staleShards,
	})
	require.NoError(t, err)

	// S1: a 0-byte stub replica of the same volume (interrupted-encode leftover).
	framework.AllocateVolume(t, clients[stubServer], volumeID, collection)

	// Plan: 14 shards spread across all three servers and both disks. The source
	// server (S0) receives shards too, so its regular volume coexists with EC on
	// the same disk before deletion.
	targets := []*worker_pb.TaskTarget{
		{Node: addr(0), DiskId: 0, ShardIds: shardRange(0, 3)},   // S0 d0: 0,1,2
		{Node: addr(0), DiskId: 1, ShardIds: shardRange(3, 5)},   // S0 d1: 3,4
		{Node: addr(1), DiskId: 0, ShardIds: shardRange(5, 8)},   // S1 d0: 5,6,7
		{Node: addr(1), DiskId: 1, ShardIds: shardRange(8, 10)},  // S1 d1: 8,9
		{Node: addr(2), DiskId: 0, ShardIds: shardRange(10, 12)}, // S2 d0: 10,11
		{Node: addr(2), DiskId: 1, ShardIds: shardRange(12, 14)}, // S2 d1: 12,13
	}

	task := NewErasureCodingTask("ec-julor", addr(srcServer), volumeID, collection, dialOption)
	params := &worker_pb.TaskParams{
		VolumeId:   volumeID,
		Collection: collection,
		Sources: []*worker_pb.TaskSource{
			{Node: addr(srcServer), VolumeId: volumeID},                        // real source
			{Node: addr(stubServer), VolumeId: volumeID},                       // 0-byte stub
			{Node: addr(srcServer), VolumeId: volumeID, ShardIds: staleShards}, // stale EC shards to clear
		},
		Targets: targets,
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:   erasure_coding.DataShardsCount,
				ParityShards: erasure_coding.ParityShardsCount,
				WorkingDir:   t.TempDir(),
			},
		},
	}

	err = task.Execute(ctx, params)
	require.Error(t, err, "empty live-idx vs non-empty must refuse encode")
	require.Contains(t, err.Error(), "no union")
	require.NotEqual(t, addr(stubServer), task.server, "after gate t.server is not a stub URL")
}
