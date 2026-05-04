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

// One reachable replica + one unreachable: the reachable delete still
// succeeds, and the function surfaces an error naming the failure.
func TestDeleteOriginalVolumeSurfacesReplicaFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(91842)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 918420, 0x91842042)
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid,
		[]byte("delete-surface-content-for-issue-9184"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode)

	task := NewErasureCodingTask(
		"delete-surface-fix",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		"",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	unreachable := "127.0.0.1:1"
	task.sources = []*worker_pb.TaskSource{
		{
			Node:     clusterHarness.VolumeServerAddress(),
			VolumeId: volumeID,
		},
		{
			Node:     unreachable,
			VolumeId: volumeID,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := task.deleteOriginalVolume(ctx)
	require.Error(t, err, "deleteOriginalVolume must surface replica delete failures (#9184)")
	require.Contains(t, err.Error(), unreachable,
		"returned error should name the replica that failed: %v", err)
	require.True(t,
		strings.Contains(err.Error(), "failed to delete"),
		"returned error should describe what failed: %v", err)

	_, statusErr := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	require.Error(t, statusErr,
		"reachable replica %d should have been deleted before failure was surfaced", volumeID)
}

func TestDeleteOriginalVolumeSucceedsWhenAllReplicasReachable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(91844)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 918440, 0x91844042)
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid,
		[]byte("delete-happy-path-content-for-issue-9184"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode)

	task := NewErasureCodingTask(
		"delete-happy-path",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		"",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	task.sources = []*worker_pb.TaskSource{
		{Node: clusterHarness.VolumeServerAddress(), VolumeId: volumeID},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, task.deleteOriginalVolume(ctx))

	_, statusErr := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	require.Error(t, statusErr, "volume %d should be gone after successful delete", volumeID)
}
