package volume_server_grpc_test

import (
	"context"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestVolumeIncrementalCopyDataAndNoDataPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(91)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	client := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 770001, 0x1122AABB)
	uploadResp := framework.UploadBytes(t, client, clusterHarness.VolumeAdminURL(), fid, []byte("incremental-copy-content"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != 201 {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataStream, err := grpcClient.VolumeIncrementalCopy(ctx, &volume_server_pb.VolumeIncrementalCopyRequest{
		VolumeId: volumeID,
		SinceNs:  0,
	})
	if err != nil {
		t.Fatalf("VolumeIncrementalCopy start failed: %v", err)
	}

	totalBytes := 0
	for {
		msg, recvErr := dataStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("VolumeIncrementalCopy recv failed: %v", recvErr)
		}
		totalBytes += len(msg.GetFileContent())
	}
	if totalBytes == 0 {
		t.Fatalf("VolumeIncrementalCopy expected streamed bytes for since_ns=0")
	}

	noDataStream, err := grpcClient.VolumeIncrementalCopy(ctx, &volume_server_pb.VolumeIncrementalCopyRequest{
		VolumeId: volumeID,
		SinceNs:  math.MaxUint64,
	})
	if err != nil {
		t.Fatalf("VolumeIncrementalCopy no-data start failed: %v", err)
	}
	_, err = noDataStream.Recv()
	if err != io.EOF {
		t.Fatalf("VolumeIncrementalCopy no-data expected EOF, got: %v", err)
	}
}

func TestCopyFileIgnoreNotFoundAndStopOffsetZeroPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(92)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	missingNoIgnore, err := grpcClient.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
		VolumeId:                 volumeID,
		Ext:                      ".definitely-missing",
		CompactionRevision:       math.MaxUint32,
		StopOffset:               1,
		IgnoreSourceFileNotFound: false,
	})
	if err == nil {
		_, err = missingNoIgnore.Recv()
	}
	if err == nil {
		t.Fatalf("CopyFile should fail for missing source file when ignore_source_file_not_found=false")
	}

	missingIgnored, err := grpcClient.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
		VolumeId:                 volumeID,
		Ext:                      ".definitely-missing",
		CompactionRevision:       math.MaxUint32,
		StopOffset:               1,
		IgnoreSourceFileNotFound: true,
	})
	if err != nil {
		t.Fatalf("CopyFile ignore-not-found start failed: %v", err)
	}
	_, err = missingIgnored.Recv()
	if err != io.EOF {
		t.Fatalf("CopyFile ignore-not-found expected EOF, got: %v", err)
	}

	stopZeroStream, err := grpcClient.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
		VolumeId:                 volumeID,
		Ext:                      ".definitely-missing",
		CompactionRevision:       math.MaxUint32,
		StopOffset:               0,
		IgnoreSourceFileNotFound: false,
	})
	if err != nil {
		t.Fatalf("CopyFile stop_offset=0 start failed: %v", err)
	}
	_, err = stopZeroStream.Recv()
	if err != io.EOF {
		t.Fatalf("CopyFile stop_offset=0 expected EOF, got: %v", err)
	}
}

func TestReceiveFileProtocolViolationResponses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	contentFirstStream, err := grpcClient.ReceiveFile(ctx)
	if err != nil {
		t.Fatalf("ReceiveFile stream create failed: %v", err)
	}
	if err = contentFirstStream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_FileContent{
			FileContent: []byte("content-before-info"),
		},
	}); err != nil {
		t.Fatalf("ReceiveFile send content-first failed: %v", err)
	}
	contentFirstResp, err := contentFirstStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("ReceiveFile content-first close failed: %v", err)
	}
	if !strings.Contains(contentFirstResp.GetError(), "file info must be sent first") {
		t.Fatalf("ReceiveFile content-first response mismatch: %+v", contentFirstResp)
	}

	unknownTypeStream, err := grpcClient.ReceiveFile(ctx)
	if err != nil {
		t.Fatalf("ReceiveFile stream create for unknown-type failed: %v", err)
	}
	if err = unknownTypeStream.Send(&volume_server_pb.ReceiveFileRequest{}); err != nil {
		t.Fatalf("ReceiveFile send unknown-type request failed: %v", err)
	}
	unknownTypeResp, err := unknownTypeStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("ReceiveFile unknown-type close failed: %v", err)
	}
	if !strings.Contains(unknownTypeResp.GetError(), "unknown message type") {
		t.Fatalf("ReceiveFile unknown-type response mismatch: %+v", unknownTypeResp)
	}
}
