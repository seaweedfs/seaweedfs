package erasure_coding

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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

// Reproduces the issue-9478 volume-13 layout end to end on a multi-server,
// multi-disk cluster and asserts the cluster converges to exactly one valid EC
// layout. Before the encode the volume is in the stuck state: a real .dat
// source, a 0-byte stub replica left by an interrupted encode, and partial
// stale EC shards from that attempt. After the worker runs (sweep stubs ->
// clear stale shards -> distribute -> mount -> verify -> delete originals):
//
//   - every assigned shard is present on its target disk, with .ecx/.vif
//     alongside it (the EC volume's info file survives even when shards land on
//     the source's own disk);
//   - no regular .dat/.idx remains anywhere — the source is deleted after
//     verify, the stub was swept before distribute;
//   - the stale shards that aren't part of the new assignment are gone.
func TestEcEncodeJulorLayoutConverges(t *testing.T) {
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
		fid := framework.NewFileID(volumeID, uint64(950000+i), uint32(0x9500CA00+i))
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
			{Node: addr(srcServer), VolumeId: volumeID},                         // real source
			{Node: addr(stubServer), VolumeId: volumeID},                        // 0-byte stub
			{Node: addr(srcServer), VolumeId: volumeID, ShardIds: staleShards},  // stale EC shards to clear
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

	require.NoError(t, task.Execute(ctx, params))

	base := fmt.Sprintf("%s_%d", collection, volumeID)

	// Shards are distributed per-server; the disk within a server is auto-selected
	// when DiskId is 0, and on a multi-disk server shards can spread to a sibling
	// disk while the .ecx/.vif index lives on one of them (the cross-disk EC
	// layout). So assert per server (aggregate across its disks): each server
	// holds exactly its assigned shards, its .ecx/.vif survive on at least one
	// disk (the info file is not stripped, even on the source's own disk), and no
	// regular .dat/.idx remains. Stale shards 11-13 mounted on the source must be
	// gone — they are not in S0's assignment.
	serverShards := map[int][]uint32{
		0: shardRange(0, 5),   // S0 (source): 0..4
		1: shardRange(5, 10),  // S1 (stub):   5..9
		2: shardRange(10, 14), // S2 (target): 10..13
	}
	allShards := map[uint32]bool{}
	for server, want := range serverShards {
		found := map[uint32]bool{}
		hasEcx, hasVif := false, false
		for d := 0; d < disksPerServer; d++ {
			dir := cluster.VolumeDiskDir(server, d)
			requireAbsent(t, dir, base+".dat")
			requireAbsent(t, dir, base+".idx")
			for id := 0; id < erasure_coding.TotalShardsCount; id++ {
				if ecFileExists(dir, fmt.Sprintf("%s.ec%02d", base, id)) {
					found[uint32(id)] = true
					allShards[uint32(id)] = true
				}
			}
			hasEcx = hasEcx || ecFileExists(dir, base+".ecx")
			hasVif = hasVif || ecFileExists(dir, base+".vif")
		}
		gotIDs := make([]uint32, 0, len(found))
		for id := range found {
			gotIDs = append(gotIDs, id)
		}
		require.ElementsMatch(t, want, gotIDs, "server %d should hold exactly its assigned shards", server)
		require.True(t, hasEcx, "server %d must keep its .ecx", server)
		require.True(t, hasVif, "server %d must keep its .vif (not stripped by the original-volume delete)", server)
	}
	require.Len(t, allShards, erasure_coding.TotalShardsCount, "the full shard set must be present across the cluster")
}

func ecFileExists(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}
