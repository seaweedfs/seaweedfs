package volume_server_grpc_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestVolumeSyncStatusAndReadVolumeFileStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(41)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	syncResp, err := grpcClient.VolumeSyncStatus(ctx, &volume_server_pb.VolumeSyncStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeSyncStatus failed: %v", err)
	}
	if syncResp.GetVolumeId() != volumeID {
		t.Fatalf("VolumeSyncStatus volume id mismatch: got %d want %d", syncResp.GetVolumeId(), volumeID)
	}

	statusResp, err := grpcClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("ReadVolumeFileStatus failed: %v", err)
	}
	if statusResp.GetVolumeId() != volumeID {
		t.Fatalf("ReadVolumeFileStatus volume id mismatch: got %d want %d", statusResp.GetVolumeId(), volumeID)
	}
	if statusResp.GetVersion() == 0 {
		t.Fatalf("ReadVolumeFileStatus expected non-zero version")
	}
}

func TestCopyAndStreamMethodsMissingVolumePaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeSyncStatus(ctx, &volume_server_pb.VolumeSyncStatusRequest{VolumeId: 98761})
	if err == nil {
		t.Fatalf("VolumeSyncStatus should fail for missing volume")
	}

	incrementalStream, err := grpcClient.VolumeIncrementalCopy(ctx, &volume_server_pb.VolumeIncrementalCopyRequest{VolumeId: 98762, SinceNs: 0})
	if err == nil {
		_, err = incrementalStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("VolumeIncrementalCopy missing-volume error mismatch: %v", err)
	}

	readAllStream, err := grpcClient.ReadAllNeedles(ctx, &volume_server_pb.ReadAllNeedlesRequest{VolumeIds: []uint32{98763}})
	if err == nil {
		_, err = readAllStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("ReadAllNeedles missing-volume error mismatch: %v", err)
	}

	copyFileStream, err := grpcClient.CopyFile(ctx, &volume_server_pb.CopyFileRequest{VolumeId: 98764, Ext: ".dat", StopOffset: 1})
	if err == nil {
		_, err = copyFileStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("CopyFile missing-volume error mismatch: %v", err)
	}

	_, err = grpcClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: 98765})
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("ReadVolumeFileStatus missing-volume error mismatch: %v", err)
	}
}

func TestVolumeCopyAndReceiveFileMaintenanceRejection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stateResp, err := grpcClient.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	_, err = grpcClient.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{Maintenance: true, Version: stateResp.GetState().GetVersion()},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}

	copyStream, err := grpcClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{VolumeId: 1, SourceDataNode: "127.0.0.1:1234"})
	if err == nil {
		_, err = copyStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeCopy maintenance error mismatch: %v", err)
	}

	receiveClient, err := grpcClient.ReceiveFile(ctx)
	if err != nil {
		t.Fatalf("ReceiveFile client creation failed: %v", err)
	}
	_ = receiveClient.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{VolumeId: 1, Ext: ".dat"},
		},
	})
	_, err = receiveClient.CloseAndRecv()
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("ReceiveFile maintenance error mismatch: %v", err)
	}
}

func TestVolumeCopySuccessFromPeerAndMountsDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartDualVolumeCluster(t, matrix.P1())
	sourceConn, sourceClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	destConn, destClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer destConn.Close()

	const volumeID = uint32(42)
	framework.AllocateVolume(t, sourceClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 880001, 0x12345678)
	payload := []byte("volume-copy-success-payload")
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload to source expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	copyStream, err := destClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		Collection:     "",
		SourceDataNode: clusterHarness.VolumeAdminAddress(0) + "." + strings.Split(clusterHarness.VolumeGRPCAddress(0), ":")[1],
	})
	if err != nil {
		t.Fatalf("VolumeCopy start failed: %v", err)
	}

	sawFinalAppendTimestamp := false
	for {
		msg, recvErr := copyStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("VolumeCopy recv failed: %v", recvErr)
		}
		if msg.GetLastAppendAtNs() > 0 {
			sawFinalAppendTimestamp = true
		}
	}
	if !sawFinalAppendTimestamp {
		t.Fatalf("VolumeCopy expected final response with last_append_at_ns")
	}

	destReadResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid)
	destReadBody := framework.ReadAllAndClose(t, destReadResp)
	if destReadResp.StatusCode != http.StatusOK {
		t.Fatalf("read from copied destination expected 200, got %d", destReadResp.StatusCode)
	}
	if string(destReadBody) != string(payload) {
		t.Fatalf("destination copied payload mismatch: got %q want %q", string(destReadBody), string(payload))
	}
}

func TestVolumeCopyOverwritesExistingDestinationVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartDualVolumeCluster(t, matrix.P1())
	sourceConn, sourceClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	destConn, destClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer destConn.Close()

	const volumeID = uint32(43)
	framework.AllocateVolume(t, sourceClient, volumeID, "")
	framework.AllocateVolume(t, destClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 880002, 0x23456789)
	sourcePayload := []byte("volume-copy-overwrite-source")
	destPayload := []byte("volume-copy-overwrite-destination-old")

	sourceUploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(0), fid, sourcePayload)
	_ = framework.ReadAllAndClose(t, sourceUploadResp)
	if sourceUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload to source expected 201, got %d", sourceUploadResp.StatusCode)
	}

	destUploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid, destPayload)
	_ = framework.ReadAllAndClose(t, destUploadResp)
	if destUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload to destination expected 201, got %d", destUploadResp.StatusCode)
	}

	destReadBeforeResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid)
	destReadBeforeBody := framework.ReadAllAndClose(t, destReadBeforeResp)
	if destReadBeforeResp.StatusCode != http.StatusOK {
		t.Fatalf("destination pre-copy read expected 200, got %d", destReadBeforeResp.StatusCode)
	}
	if string(destReadBeforeBody) != string(destPayload) {
		t.Fatalf("destination pre-copy payload mismatch: got %q want %q", string(destReadBeforeBody), string(destPayload))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	copyStream, err := destClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
		VolumeId:       volumeID,
		Collection:     "",
		SourceDataNode: clusterHarness.VolumeAdminAddress(0) + "." + strings.Split(clusterHarness.VolumeGRPCAddress(0), ":")[1],
	})
	if err != nil {
		t.Fatalf("VolumeCopy overwrite start failed: %v", err)
	}

	sawFinalAppendTimestamp := false
	for {
		msg, recvErr := copyStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("VolumeCopy overwrite recv failed: %v", recvErr)
		}
		if msg.GetLastAppendAtNs() > 0 {
			sawFinalAppendTimestamp = true
		}
	}
	if !sawFinalAppendTimestamp {
		t.Fatalf("VolumeCopy overwrite expected final response with last_append_at_ns")
	}

	destReadAfterResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid)
	destReadAfterBody := framework.ReadAllAndClose(t, destReadAfterResp)
	if destReadAfterResp.StatusCode != http.StatusOK {
		t.Fatalf("destination post-copy read expected 200, got %d", destReadAfterResp.StatusCode)
	}
	if string(destReadAfterBody) != string(sourcePayload) {
		t.Fatalf("destination post-copy payload mismatch: got %q want %q", string(destReadAfterBody), string(sourcePayload))
	}
}
