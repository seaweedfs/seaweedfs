package volume_server_grpc_test

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
)

// TestReceiveFileTruncatesMountedEcShard reproduces the overwrite race
// called out in https://github.com/seaweedfs/seaweedfs/issues/9184: after an
// EC shard has been generated and mounted, ReceiveFile for the same
// (volume, shard) opens the on-disk file with os.Create, which truncates in
// place. The in-memory EcVolume holds a file descriptor against the same
// inode, so truncation corrupts the live shard.
//
// This test asserts the current (buggy) behavior. When a fix lands,
// ReceiveFile for a mounted EC shard should reject the request (or
// rename-then-swap) and this test must be updated accordingly.
func TestReceiveFileTruncatesMountedEcShard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(91841)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 918401, 0x9184CAFE)
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid,
		[]byte("ec-receive-overwrite-content-for-issue-9184-repro"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   volumeID,
		Collection: "",
	}); err != nil {
		t.Fatalf("VolumeEcShardsGenerate: %v", err)
	}

	if _, err := grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   volumeID,
		Collection: "",
		ShardIds:   []uint32{0},
	}); err != nil {
		t.Fatalf("VolumeEcShardsMount: %v", err)
	}

	// Locate the mounted shard file so we can observe truncation. The
	// framework's single-data-dir layout places the shard under
	// <baseDir>/volume. EcShardBaseFileName for an empty collection is
	// "<vid>", so the shard file is "<vid>.ec00".
	dataDir := filepath.Join(clusterHarness.BaseDir(), "volume")
	shardPath := filepath.Join(dataDir, fmt.Sprintf("%d.ec00", volumeID))
	origInfo, err := os.Stat(shardPath)
	if err != nil {
		t.Fatalf("stat mounted shard %s: %v", shardPath, err)
	}
	origSize := origInfo.Size()
	if origSize == 0 {
		t.Fatalf("mounted shard %s unexpectedly empty", shardPath)
	}

	// Confirm a live read works before the overwrite so we know the mount is
	// healthy. This is the shard Reader reading through the fd held by the
	// EcVolume in memory.
	readStream, err := grpcClient.VolumeEcShardRead(ctx, &volume_server_pb.VolumeEcShardReadRequest{
		VolumeId: volumeID,
		ShardId:  0,
		Offset:   0,
		Size:     1,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardRead (pre): %v", err)
	}
	if _, err := readStream.Recv(); err != nil {
		t.Fatalf("VolumeEcShardRead Recv (pre): %v", err)
	}

	// Send a deliberately-smaller payload via ReceiveFile for the same
	// (volume, shard). In the buggy code path, the server calls os.Create on
	// the live shard path, truncating the file that the mounted EcVolume has
	// open.
	overwritePayload := []byte("bug-9184-overwrite")
	if int64(len(overwritePayload)) >= origSize {
		t.Fatalf("overwrite payload (%d bytes) not smaller than original shard (%d bytes); adjust test",
			len(overwritePayload), origSize)
	}
	receiveStream, err := grpcClient.ReceiveFile(ctx)
	if err != nil {
		t.Fatalf("ReceiveFile stream create: %v", err)
	}
	if err = receiveStream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{
				VolumeId:   volumeID,
				Ext:        ".ec00",
				Collection: "",
				IsEcVolume: true,
				ShardId:    0,
				FileSize:   uint64(len(overwritePayload)),
			},
		},
	}); err != nil {
		t.Fatalf("ReceiveFile send info: %v", err)
	}
	if err = receiveStream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_FileContent{FileContent: overwritePayload},
	}); err != nil {
		t.Fatalf("ReceiveFile send content: %v", err)
	}
	resp, err := receiveStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("ReceiveFile close: %v", err)
	}

	// BUG #9184: the current server accepts the overwrite and reports
	// success. A safe implementation would reject the write (or rename the
	// old file out of the way) because the shard is mounted. When the fix
	// lands, invert this assertion: expect resp.GetError() != "" or a gRPC
	// error from CloseAndRecv.
	if resp.GetError() != "" {
		t.Fatalf("bug #9184 regression: ReceiveFile rejected the overwrite (already fixed?); resp=%+v", resp)
	}
	if resp.GetBytesWritten() != uint64(len(overwritePayload)) {
		t.Fatalf("bug #9184: expected bytes_written=%d, got %d", len(overwritePayload), resp.GetBytesWritten())
	}

	// Verify the on-disk file was truncated in place. Same inode, smaller
	// length — the mounted EcVolume's fd now points to a shorter file.
	afterInfo, err := os.Stat(shardPath)
	if err != nil {
		t.Fatalf("stat shard after overwrite: %v", err)
	}
	if afterInfo.Size() != int64(len(overwritePayload)) {
		t.Fatalf("bug #9184: expected shard size to be truncated to %d, got %d",
			len(overwritePayload), afterInfo.Size())
	}
	if afterInfo.Size() >= origSize {
		t.Fatalf("bug #9184: expected shard shrunk from %d, still %d", origSize, afterInfo.Size())
	}

	t.Logf("bug #9184 reproduced: mounted shard %s truncated from %d to %d bytes",
		shardPath, origSize, afterInfo.Size())
}
