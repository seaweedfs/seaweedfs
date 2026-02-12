package volume_server_grpc_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
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
