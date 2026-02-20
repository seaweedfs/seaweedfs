package volume_server_grpc_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVolumeTailSenderMissingVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := grpcClient.VolumeTailSender(ctx, &volume_server_pb.VolumeTailSenderRequest{VolumeId: 77777, SinceNs: 0, IdleTimeoutSeconds: 1})
	if err == nil {
		_, err = stream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("VolumeTailSender missing-volume error mismatch: %v", err)
	}
}

func TestVolumeTailSenderHeartbeatThenEOF(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(71)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := grpcClient.VolumeTailSender(ctx, &volume_server_pb.VolumeTailSenderRequest{
		VolumeId:           volumeID,
		SinceNs:            0,
		IdleTimeoutSeconds: 1,
	})
	if err != nil {
		t.Fatalf("VolumeTailSender start failed: %v", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("VolumeTailSender first recv failed: %v", err)
	}
	if !msg.GetIsLastChunk() {
		t.Fatalf("expected first tail message to be heartbeat IsLastChunk=true")
	}

	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("expected EOF after idle timeout drain, got: %v", err)
	}
}

func TestVolumeTailReceiverMissingVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeTailReceiver(ctx, &volume_server_pb.VolumeTailReceiverRequest{VolumeId: 88888, SourceVolumeServer: clusterHarness.VolumeServerAddress(), SinceNs: 0, IdleTimeoutSeconds: 1})
	if err == nil || !strings.Contains(err.Error(), "receiver not found volume") {
		t.Fatalf("VolumeTailReceiver missing-volume error mismatch: %v", err)
	}
}

func TestVolumeTailReceiverSourceUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(89)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeTailReceiver(ctx, &volume_server_pb.VolumeTailReceiverRequest{
		VolumeId:           volumeID,
		SourceVolumeServer: "127.0.0.1:19999.29999",
		SinceNs:            0,
		IdleTimeoutSeconds: 1,
	})
	if err == nil {
		t.Fatalf("VolumeTailReceiver should fail when source volume server is unavailable")
	}
	lowered := strings.ToLower(err.Error())
	if !strings.Contains(lowered, "dial") && !strings.Contains(lowered, "unavailable") && !strings.Contains(lowered, "connection refused") {
		t.Fatalf("VolumeTailReceiver source-unavailable error mismatch: %v", err)
	}
}

func TestVolumeTailReceiverReplicatesSourceUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartDualVolumeCluster(t, matrix.P1())
	sourceConn, sourceClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(0))
	defer sourceConn.Close()
	destConn, destClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress(1))
	defer destConn.Close()

	const volumeID = uint32(72)
	framework.AllocateVolume(t, sourceClient, volumeID, "")
	framework.AllocateVolume(t, destClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 880003, 0x3456789A)
	payload := []byte("tail-receiver-replicates-source-updates")

	sourceUploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(0), fid, payload)
	_ = framework.ReadAllAndClose(t, sourceUploadResp)
	if sourceUploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("source upload expected 201, got %d", sourceUploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := destClient.VolumeTailReceiver(ctx, &volume_server_pb.VolumeTailReceiverRequest{
		VolumeId:           volumeID,
		SourceVolumeServer: clusterHarness.VolumeAdminAddress(0) + "." + strings.Split(clusterHarness.VolumeGRPCAddress(0), ":")[1],
		SinceNs:            0,
		IdleTimeoutSeconds: 1,
	})
	if err != nil {
		t.Fatalf("VolumeTailReceiver success path failed: %v", err)
	}

	destReadResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(1), fid)
	destReadBody := framework.ReadAllAndClose(t, destReadResp)
	if destReadResp.StatusCode != http.StatusOK {
		t.Fatalf("destination read after tail receive expected 200, got %d", destReadResp.StatusCode)
	}
	if string(destReadBody) != string(payload) {
		t.Fatalf("destination tail-received payload mismatch: got %q want %q", string(destReadBody), string(payload))
	}
}

func TestVolumeTailSenderLargeNeedleChunking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(73)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 880004, 0x456789AB)
	largePayload := bytes.Repeat([]byte("L"), 2*1024*1024+128*1024)
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, largePayload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("large upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := grpcClient.VolumeTailSender(ctx, &volume_server_pb.VolumeTailSenderRequest{
		VolumeId:           volumeID,
		SinceNs:            0,
		IdleTimeoutSeconds: 1,
	})
	if err != nil {
		t.Fatalf("VolumeTailSender start failed: %v", err)
	}

	dataChunkCount := 0
	sawNonLastDataChunk := false
	sawLastDataChunk := false
	for {
		msg, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("VolumeTailSender recv failed: %v", recvErr)
		}
		if len(msg.GetNeedleBody()) == 0 {
			continue
		}
		dataChunkCount++
		if msg.GetIsLastChunk() {
			sawLastDataChunk = true
		} else {
			sawNonLastDataChunk = true
		}
	}

	if dataChunkCount < 2 {
		t.Fatalf("VolumeTailSender expected multiple chunks for large needle, got %d", dataChunkCount)
	}
	if !sawNonLastDataChunk {
		t.Fatalf("VolumeTailSender expected at least one non-last data chunk")
	}
	if !sawLastDataChunk {
		t.Fatalf("VolumeTailSender expected a final data chunk marked IsLastChunk=true")
	}
}

func TestVolumeTailSenderStreamCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(74)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := grpcClient.VolumeTailSender(ctx, &volume_server_pb.VolumeTailSenderRequest{
		VolumeId:           volumeID,
		SinceNs:            0,
		IdleTimeoutSeconds: 30,
	})
	if err != nil {
		cancel()
		t.Fatalf("VolumeTailSender start failed: %v", err)
	}

	firstMsg, err := stream.Recv()
	if err != nil {
		cancel()
		t.Fatalf("VolumeTailSender first recv failed: %v", err)
	}
	if !firstMsg.GetIsLastChunk() {
		cancel()
		t.Fatalf("expected heartbeat first message before cancellation")
	}

	cancel()
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("VolumeTailSender recv after cancellation expected error")
	}
	if status.Code(err) != codes.Canceled {
		t.Fatalf("VolumeTailSender cancellation expected grpc code Canceled, got %v (%v)", status.Code(err), err)
	}
}
