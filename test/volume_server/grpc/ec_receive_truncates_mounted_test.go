package volume_server_grpc_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestReceiveFileRejectsOverwriteOfMountedEcShard(t *testing.T) {
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

	overwritePayload := []byte("bug-9184-overwrite")
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
	resp, err := receiveStream.CloseAndRecv()
	if err != nil {
		t.Logf("ReceiveFile rejected at stream level: %v", err)
	} else {
		if resp.GetError() == "" {
			t.Fatalf("expected ReceiveFile to reject overwrite of mounted shard, got success: %+v", resp)
		}
		if !strings.Contains(resp.GetError(), "mounted") {
			t.Fatalf("expected error to mention mounted; got: %s", resp.GetError())
		}
	}

	afterInfo, err := os.Stat(shardPath)
	if err != nil {
		t.Fatalf("stat shard after rejected overwrite: %v", err)
	}
	if afterInfo.Size() != origSize {
		t.Fatalf("shard %s was modified despite rejection: size was %d, now %d",
			shardPath, origSize, afterInfo.Size())
	}

	postStream, err := grpcClient.VolumeEcShardRead(ctx, &volume_server_pb.VolumeEcShardReadRequest{
		VolumeId: volumeID,
		ShardId:  0,
		Offset:   0,
		Size:     1,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardRead (post): %v", err)
	}
	if _, err := postStream.Recv(); err != nil {
		t.Fatalf("VolumeEcShardRead Recv (post): %v", err)
	}

	t.Logf("ReceiveFile correctly refused overwrite; mounted shard intact at %d bytes", afterInfo.Size())
}

func TestReceiveFileAllowsEcShardWhenNoMount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const volumeID = uint32(91843)
	const collection = "ec-receive-no-mount"
	payload := []byte("ok-to-receive-not-mounted")

	stream, err := grpcClient.ReceiveFile(ctx)
	if err != nil {
		t.Fatalf("ReceiveFile stream create: %v", err)
	}
	if err = stream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{
				VolumeId:   volumeID,
				Ext:        ".ec00",
				Collection: collection,
				IsEcVolume: true,
				ShardId:    0,
				FileSize:   uint64(len(payload)),
			},
		},
	}); err != nil {
		t.Fatalf("ReceiveFile send info: %v", err)
	}
	if err = stream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_FileContent{FileContent: payload},
	}); err != nil {
		t.Fatalf("ReceiveFile send content: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("ReceiveFile close: %v", err)
	}
	if resp.GetError() != "" {
		t.Fatalf("expected success on unmounted volume, got error: %s", resp.GetError())
	}
	if resp.GetBytesWritten() != uint64(len(payload)) {
		t.Fatalf("bytes_written mismatch: got %d want %d", resp.GetBytesWritten(), len(payload))
	}
}
