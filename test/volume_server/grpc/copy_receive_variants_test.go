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

func TestCopyFileCompactionRevisionMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(94)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := grpcClient.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
		VolumeId:           volumeID,
		Ext:                ".idx",
		CompactionRevision: 1, // fresh volume starts at revision 0
		StopOffset:         1,
	})
	if err == nil {
		_, err = stream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "is compacted") {
		t.Fatalf("CopyFile compaction mismatch error mismatch: %v", err)
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

func TestReceiveFileSuccessForRegularVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(95)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payloadA := []byte("receive-file-chunk-a:")
	payloadB := []byte("receive-file-chunk-b")
	expected := append(append([]byte{}, payloadA...), payloadB...)

	receiveStream, err := grpcClient.ReceiveFile(ctx)
	if err != nil {
		t.Fatalf("ReceiveFile stream create failed: %v", err)
	}

	if err = receiveStream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{
				VolumeId:   volumeID,
				Ext:        ".tmprecv",
				Collection: "",
				IsEcVolume: false,
				FileSize:   uint64(len(expected)),
			},
		},
	}); err != nil {
		t.Fatalf("ReceiveFile send info failed: %v", err)
	}
	if err = receiveStream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_FileContent{FileContent: payloadA},
	}); err != nil {
		t.Fatalf("ReceiveFile send payloadA failed: %v", err)
	}
	if err = receiveStream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_FileContent{FileContent: payloadB},
	}); err != nil {
		t.Fatalf("ReceiveFile send payloadB failed: %v", err)
	}

	resp, err := receiveStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("ReceiveFile close failed: %v", err)
	}
	if resp.GetError() != "" {
		t.Fatalf("ReceiveFile unexpected error response: %+v", resp)
	}
	if resp.GetBytesWritten() != uint64(len(expected)) {
		t.Fatalf("ReceiveFile bytes_written mismatch: got %d want %d", resp.GetBytesWritten(), len(expected))
	}

	copyStream, err := grpcClient.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
		VolumeId:           volumeID,
		Ext:                ".tmprecv",
		CompactionRevision: math.MaxUint32,
		StopOffset:         uint64(len(expected)),
	})
	if err != nil {
		t.Fatalf("CopyFile for received data start failed: %v", err)
	}

	var copied []byte
	for {
		msg, recvErr := copyStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("CopyFile for received data recv failed: %v", recvErr)
		}
		copied = append(copied, msg.GetFileContent()...)
	}

	if string(copied) != string(expected) {
		t.Fatalf("received file data mismatch: got %q want %q", string(copied), string(expected))
	}
}
