package erasure_coding

import (
	"context"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestCopyVolumeFilesToWorkerUsesCurrentCompactionRevision(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(951)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()

	liveFID := framework.NewFileID(volumeID, 1001, 0x1111AAAA)
	liveUploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), liveFID, []byte("live-payload-for-ec-copy"))
	_ = framework.ReadAllAndClose(t, liveUploadResp)
	require.Equal(t, http.StatusCreated, liveUploadResp.StatusCode)

	deletedFID := framework.NewFileID(volumeID, 1002, 0x2222BBBB)
	deletedUploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), deletedFID, []byte("deleted-payload-for-vacuum"))
	_ = framework.ReadAllAndClose(t, deletedUploadResp)
	require.Equal(t, http.StatusCreated, deletedUploadResp.StatusCode)

	deleteReq, err := http.NewRequest(http.MethodDelete, clusterHarness.VolumeAdminURL()+"/"+deletedFID, nil)
	require.NoError(t, err)
	deleteResp := framework.DoRequest(t, httpClient, deleteReq)
	_ = framework.ReadAllAndClose(t, deleteResp)
	require.Equal(t, http.StatusAccepted, deleteResp.StatusCode)

	compactVolumeOnce(t, grpcClient, volumeID)

	task := NewErasureCodingTask(
		"copy-after-compaction",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		"",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, task.markReplicasReadonly(ctx))

	fileStatus, err := task.readSourceVolumeFileStatus(ctx)
	require.NoError(t, err)
	require.Greater(t, fileStatus.GetCompactionRevision(), uint32(0))

	localFiles, err := task.copyVolumeFilesToWorker(ctx, t.TempDir())
	require.NoError(t, err)

	datInfo, err := os.Stat(localFiles["dat"])
	require.NoError(t, err)
	require.Equal(t, int64(fileStatus.GetDatFileSize()), datInfo.Size())

	idxInfo, err := os.Stat(localFiles["idx"])
	require.NoError(t, err)
	require.Equal(t, int64(fileStatus.GetIdxFileSize()), idxInfo.Size())
}

// The worker-local encode path must stamp the EC ratio and an encode identity
// into the .vif, or the read guard is silently off for worker-encoded volumes.
func TestGenerateEcShardsLocallyStampsEncodeIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(952)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 2001, 0x3333CCCC)
	resp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, []byte("payload-for-ec-encode-identity"))
	_ = framework.ReadAllAndClose(t, resp)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	task := NewErasureCodingTask(
		"ec-encode-identity",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		"",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, task.markReplicasReadonly(ctx))
	localFiles, err := task.copyVolumeFilesToWorker(ctx, t.TempDir())
	require.NoError(t, err)

	shardFiles, err := task.generateEcShardsLocally(localFiles, t.TempDir())
	require.NoError(t, err)

	vifPath := shardFiles["vif"]
	require.NotEmpty(t, vifPath, "generate must produce a .vif")
	vi, _, found, err := volume_info.MaybeLoadVolumeInfo(vifPath)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, vi.EcShardConfig, "worker .vif must record the EC config")
	require.Greater(t, vi.EcShardConfig.DataShards, uint32(0))
	require.Greater(t, vi.EcShardConfig.ParityShards, uint32(0))
	require.NotZero(t, vi.EcShardConfig.EncodeTsNs, "worker-encoded .vif must carry an encode identity")
}

func compactVolumeOnce(t *testing.T, grpcClient volume_server_pb.VolumeServerClient, volumeID uint32) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	compactStream, err := grpcClient.VacuumVolumeCompact(ctx, &volume_server_pb.VacuumVolumeCompactRequest{
		VolumeId: volumeID,
	})
	require.NoError(t, err)

	for {
		_, err = compactStream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	_, err = grpcClient.VacuumVolumeCommit(ctx, &volume_server_pb.VacuumVolumeCommitRequest{
		VolumeId: volumeID,
	})
	require.NoError(t, err)

	_, err = grpcClient.VacuumVolumeCleanup(ctx, &volume_server_pb.VacuumVolumeCleanupRequest{
		VolumeId: volumeID,
	})
	require.NoError(t, err)
}
