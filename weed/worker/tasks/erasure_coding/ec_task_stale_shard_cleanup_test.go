package erasure_coding

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

// Reproduces the cross-server stale-EC-shard collision reported as a 4.24
// recurrence of #9478: a previous EC encode left partial shards mounted on
// destination servers that are NOT the .dat owner, so PR #9480's same-store
// prune doesn't fire. The EC scheduler keeps re-proposing, and every retry
// hits ReceiveFile's "ec volume %d is mounted; refusing overwrite" guard on
// those destinations. Worker must clear stale shards from each destination
// before distributing.
//
// The test sets up the post-failure state on a single volume server,
// confirms the refusal, then drives the new cleanupStaleEcShards helper
// and asserts the next ReceiveFile attempt succeeds.
func TestCleanupStaleEcShardsBeforeDistribute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const (
		volumeID   = uint32(9478)
		collection = "ec-9478-xserver"
	)

	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 947800, 0x9478CAFE)
	upResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid,
		[]byte("payload-for-cross-server-stale-ec-cleanup"))
	_ = framework.ReadAllAndClose(t, upResp)
	require.Equal(t, http.StatusCreated, upResp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID, Collection: collection,
	})
	require.NoError(t, err)

	// Mount a partial subset to mimic the half-done previous distribute that
	// the user reported on 10.0.14.12 / .13 / .9: 1-3 shards mounted per node,
	// no .dat on the destination, nothing left to trigger #9480's prune.
	staleShards := []uint32{0, 1, 2}
	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: collection,
		ShardIds: staleShards,
	})
	require.NoError(t, err)

	shardPath := makeTinyEcShardFile(t)

	// Pre-fix state: ReceiveFile against the mounted EC volume is refused.
	// This is the exact symptom 4.24 keeps logging in the user's cluster.
	err = sendShardViaReceiveFile(ctx, grpcClient, volumeID, collection, 0, shardPath)
	require.Error(t, err, "expected ReceiveFile to be refused while EC volume is mounted")
	require.True(t,
		strings.Contains(err.Error(), "is mounted") ||
			strings.Contains(err.Error(), "unmount before ReceiveFile"),
		"expected refusal to name the mounted-volume guard, got: %v", err)

	// Drive the worker-side cleanup we expect ec_task to perform before
	// distributing. The source is keyed by len(ShardIds)>0; getReplicas
	// must NOT treat this as a regular volume replica to delete.
	task := NewErasureCodingTask(
		"9478-xserver",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		collection,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	task.dataShards = erasure_coding.DataShardsCount
	task.parityShards = erasure_coding.ParityShardsCount
	task.sources = []*worker_pb.TaskSource{
		{
			Node:     clusterHarness.VolumeServerAddress(),
			VolumeId: volumeID,
			ShardIds: staleShards,
		},
	}

	require.NoError(t, task.cleanupStaleEcShards(ctx))

	// EC volume should no longer be mounted on the destination.
	_, infoErr := grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: volumeID})
	require.Error(t, infoErr, "EC volume should be gone from server after cleanupStaleEcShards")

	// The new shard transfer that previously collided now goes through.
	require.NoError(t,
		sendShardViaReceiveFile(ctx, grpcClient, volumeID, collection, 0, shardPath),
		"ReceiveFile must succeed after stale shards are cleaned up")

	// getReplicas must skip EC-shard sources so the post-encode VolumeDelete
	// step does not run against destination-only nodes.
	require.Empty(t, task.getReplicas(),
		"EC-shard sources (ShardIds>0) must not appear in replica delete list")
}

// EC-shard cleanup must be a no-op when sources carry only the regular
// volume replica (ShardIds empty). The destination volume server should not
// receive any unmount/delete calls.
func TestCleanupStaleEcShardsSkipsRegularReplicas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(9479)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	task := NewErasureCodingTask(
		"9478-no-stale",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		"",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	task.sources = []*worker_pb.TaskSource{
		{Node: clusterHarness.VolumeServerAddress(), VolumeId: volumeID}, // replica
	}

	require.NoError(t, task.cleanupStaleEcShards(ctx),
		"cleanup with only replica sources must succeed without touching the server")

	// The .dat volume is untouched.
	_, err := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	require.NoError(t, err, "regular volume must remain after cleanupStaleEcShards no-op")
}

// makeTinyEcShardFile writes a small file we can stream through ReceiveFile.
// The contents do not need to be a valid shard — the volume server's
// "is mounted" guard runs before any content is consumed, and on the success
// path the file just lands on disk to confirm acceptance.
func makeTinyEcShardFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "shard.bin")
	require.NoError(t, os.WriteFile(p, []byte("ec-shard-placeholder"), 0o600))
	return p
}

// sendShardViaReceiveFile streams a shard file to the volume server via the
// same gRPC API the EC worker uses (volume_grpc_copy.go:ReceiveFile). It
// returns the error the server replies with so tests can distinguish the
// "is mounted; refusing overwrite" guard from transport errors.
func sendShardViaReceiveFile(
	ctx context.Context,
	client volume_server_pb.VolumeServerClient,
	volumeID uint32,
	collection string,
	shardID uint32,
	filePath string,
) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	stream, err := client.ReceiveFile(ctx)
	if err != nil {
		return err
	}

	if err := stream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{
				VolumeId:   volumeID,
				Ext:        erasure_coding.ToExt(int(shardID)),
				Collection: collection,
				IsEcVolume: true,
				ShardId:    shardID,
				FileSize:   uint64(info.Size()),
			},
		},
	}); err != nil {
		return err
	}

	buf := make([]byte, 32*1024)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&volume_server_pb.ReceiveFileRequest{
				Data: &volume_server_pb.ReceiveFileRequest_FileContent{
					FileContent: buf[:n],
				},
			}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return &receiveFileServerError{msg: resp.Error}
	}
	return nil
}

type receiveFileServerError struct{ msg string }

func (e *receiveFileServerError) Error() string { return e.msg }
