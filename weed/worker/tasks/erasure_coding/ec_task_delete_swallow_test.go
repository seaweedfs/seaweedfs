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
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestDeleteOriginalVolumeSwallowsReplicaFailures reproduces the error-
// swallowing behavior described in
// https://github.com/seaweedfs/seaweedfs/issues/9184: when any replica
// VolumeDelete call fails, ErasureCodingTask.deleteOriginalVolume logs the
// error but returns nil. The EC task therefore reports success to the admin,
// the surviving replica stays on disk, and a later detection scan re-proposes
// the same volume for EC encoding — which in turn drives the retry-truncates-
// mounted-shard corruption also tracked by that issue.
//
// The test points the task at one reachable replica (so at least one delete
// can succeed) plus one unreachable replica (address that refuses to dial).
// The current implementation returns nil; when the fix lands the function
// must surface the failure and this test needs to be inverted.
func TestDeleteOriginalVolumeSwallowsReplicaFailures(t *testing.T) {
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
		[]byte("delete-swallow-content-for-issue-9184"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	require.Equal(t, http.StatusCreated, uploadResp.StatusCode)

	task := NewErasureCodingTask(
		"delete-swallow-repro",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		"",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	// Two replicas: the real volume server (reachable) and a bogus address
	// that will refuse connections. deleteOriginalVolume iterates sources
	// and attempts VolumeDelete on each.
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

	// BUG #9184: deleteOriginalVolume returns nil even though one replica
	// failed. That means the EC task reports overall success, which is what
	// lets the stale replica on the unreachable server become a retry
	// candidate (or, if the delete had been on the reachable side, leaves a
	// surviving original volume). The fix must return an error so the task
	// is retried without reporting success. Until then, pin the current
	// behavior here.
	err := task.deleteOriginalVolume(ctx)
	require.NoError(t, err, "bug #9184 regression: deleteOriginalVolume should currently swallow the replica error; got: %v", err)

	// Double-check the reachable replica really was deleted, so we know the
	// test is exercising the "some-succeeded, some-failed" branch and not
	// e.g. a silent no-op. VolumeStatus errors for unknown volumes.
	_, statusErr := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	require.Error(t, statusErr,
		"bug #9184 invariant: reachable replica %d should have been deleted before the failure was swallowed", volumeID)

	t.Logf("bug #9184 reproduced: deleteOriginalVolume returned nil even though delete on %s failed", unreachable)
}
