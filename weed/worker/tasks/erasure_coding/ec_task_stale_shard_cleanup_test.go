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

// Reproduces a stuck re-encode: partial EC shards mounted on a destination
// from a previous failed encode cause ReceiveFile to refuse with the
// mounted-volume guard. cleanupStaleEcShards must clear them so the next
// ReceiveFile lands.
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

	// Partial subset mimics a half-finished previous distribute: shards
	// mounted on the destination with no .dat to anchor a same-store prune.
	staleShards := []uint32{0, 1, 2}
	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: collection,
		ShardIds: staleShards,
	})
	require.NoError(t, err)

	shardPath := makeTinyEcShardFile(t)

	// Pre-cleanup: the mounted partial EC blocks ReceiveFile.
	err = sendShardViaReceiveFile(ctx, grpcClient, volumeID, collection, 0, shardPath)
	require.Error(t, err, "expected ReceiveFile to be refused while EC volume is mounted")
	require.True(t,
		strings.Contains(err.Error(), "is mounted") ||
			strings.Contains(err.Error(), "unmount before ReceiveFile"),
		"expected refusal to name the mounted-volume guard, got: %v", err)

	// ShardIds set marks this as an EC-shard cleanup source: cleanup will
	// target it; getReplicas must skip it.
	task := NewErasureCodingTask(
		"stale-ec-xserver",
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

	_, infoErr := grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: volumeID})
	require.Error(t, infoErr, "EC volume should be gone after cleanupStaleEcShards")

	require.NoError(t,
		sendShardViaReceiveFile(ctx, grpcClient, volumeID, collection, 0, shardPath),
		"ReceiveFile must succeed after cleanup")

	require.Empty(t, task.getReplicas(),
		"EC-shard sources must not appear in replica delete list")
}

// The destination has more shards mounted than t.sources lists — simulates
// detection-time topology missing shards (e.g., a prior attempt's mount
// hadn't heartbeated yet, or different shards live on a sibling disk). The
// cleanup must clear FindEcVolume regardless, by issuing unmount/delete
// over the full shard range, so the next ReceiveFile lands.
func TestCleanupStaleEcShardsClearsShardsBeyondSources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const (
		volumeID   = uint32(94780)
		collection = "ec-9478-beyond"
	)

	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 9478001, 0x9478FACE)
	upResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid,
		[]byte("payload-for-shards-beyond-sources"))
	_ = framework.ReadAllAndClose(t, upResp)
	require.Equal(t, http.StatusCreated, upResp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID, Collection: collection,
	})
	require.NoError(t, err)

	// Five shards mounted on the destination.
	mountedShards := []uint32{0, 1, 2, 3, 4}
	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: collection,
		ShardIds: mountedShards,
	})
	require.NoError(t, err)

	// Detection only "saw" two of them — the rest must still get cleared.
	knownToDetection := []uint32{0, 1}

	task := NewErasureCodingTask(
		"stale-ec-beyond-sources",
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
			ShardIds: knownToDetection,
		},
	}

	require.NoError(t, task.cleanupStaleEcShards(ctx))

	_, infoErr := grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: volumeID})
	require.Error(t, infoErr,
		"all mounted shards must be cleared even though detection only listed a subset")

	shardPath := makeTinyEcShardFile(t)
	require.NoError(t,
		sendShardViaReceiveFile(ctx, grpcClient, volumeID, collection, 4, shardPath),
		"ReceiveFile for a shard outside detection's snapshot must succeed after cleanup")
}

// Cleanup also targets fresh destinations from t.targets even when no
// EC-shard source row exists for them. This catches concurrent-attempt
// fallout where a previous worker mounted shards on a node we're now
// writing to but the topology snapshot is stale.
func TestCleanupStaleEcShardsCoversTargetsWithoutSources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const (
		volumeID   = uint32(94781)
		collection = "ec-9478-targets-only"
	)

	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 9478101, 0x947811CE)
	upResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid,
		[]byte("payload-for-targets-only-cleanup"))
	_ = framework.ReadAllAndClose(t, upResp)
	require.Equal(t, http.StatusCreated, upResp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID, Collection: collection,
	})
	require.NoError(t, err)
	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: collection,
		ShardIds: []uint32{0, 1},
	})
	require.NoError(t, err)

	task := NewErasureCodingTask(
		"stale-ec-targets-only",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		collection,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	task.dataShards = erasure_coding.DataShardsCount
	task.parityShards = erasure_coding.ParityShardsCount
	// No sources. The destination is named only as a target — the cleanup
	// must still reach it.
	task.targets = []*worker_pb.TaskTarget{
		{Node: clusterHarness.VolumeServerAddress(), VolumeId: volumeID, ShardIds: []uint32{0}},
	}

	require.NoError(t, task.cleanupStaleEcShards(ctx))

	_, infoErr := grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: volumeID})
	require.Error(t, infoErr,
		"target-only destinations must also be cleaned even without a source row")
}

// Cleanup is a no-op when sources carry only the regular .dat replica.
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
		"no-stale-ec",
		clusterHarness.VolumeServerAddress(),
		volumeID,
		"",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	task.sources = []*worker_pb.TaskSource{
		{Node: clusterHarness.VolumeServerAddress(), VolumeId: volumeID},
	}

	require.NoError(t, task.cleanupStaleEcShards(ctx))

	_, err := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	require.NoError(t, err, "regular volume must remain untouched")
}

// makeTinyEcShardFile writes a placeholder payload — the mounted-volume
// guard fires before any content is consumed, so the bytes don't need to
// be a real shard.
func makeTinyEcShardFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "shard.bin")
	require.NoError(t, os.WriteFile(p, []byte("ec-shard-placeholder"), 0o600))
	return p
}

// sendShardViaReceiveFile streams a shard file through the same ReceiveFile
// gRPC the EC worker uses, returning the server's reply error verbatim.
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
