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

	require.NoError(t, task.markVolumeReadonly())

	fileStatus, err := task.readSourceVolumeFileStatus()
	require.NoError(t, err)
	require.Greater(t, fileStatus.GetCompactionRevision(), uint32(0))

	localFiles, err := task.copyVolumeFilesToWorker(t.TempDir())
	require.NoError(t, err)

	datInfo, err := os.Stat(localFiles["dat"])
	require.NoError(t, err)
	require.Equal(t, int64(fileStatus.GetDatFileSize()), datInfo.Size())

	idxInfo, err := os.Stat(localFiles["idx"])
	require.NoError(t, err)
	require.Equal(t, int64(fileStatus.GetIdxFileSize()), idxInfo.Size())
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
