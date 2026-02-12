package volume_server_grpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestReadNeedleBlobAndMetaMissingVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.ReadNeedleBlob(ctx, &volume_server_pb.ReadNeedleBlobRequest{
		VolumeId: 99111,
		Offset:   0,
		Size:     16,
	})
	if err == nil {
		t.Fatalf("ReadNeedleBlob should fail for missing volume")
	}
	if !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("ReadNeedleBlob missing volume error mismatch: %v", err)
	}

	_, err = grpcClient.ReadNeedleMeta(ctx, &volume_server_pb.ReadNeedleMetaRequest{
		VolumeId: 99112,
		NeedleId: 1,
		Offset:   0,
		Size:     16,
	})
	if err == nil {
		t.Fatalf("ReadNeedleMeta should fail for missing volume")
	}
	if !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("ReadNeedleMeta missing volume error mismatch: %v", err)
	}
}

func TestWriteNeedleBlobMaintenanceAndMissingVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.WriteNeedleBlob(ctx, &volume_server_pb.WriteNeedleBlobRequest{
		VolumeId:   99113,
		NeedleId:   1,
		NeedleBlob: []byte("abc"),
		Size:       3,
	})
	if err == nil {
		t.Fatalf("WriteNeedleBlob should fail for missing volume")
	}
	if !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("WriteNeedleBlob missing volume error mismatch: %v", err)
	}

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

	_, err = grpcClient.WriteNeedleBlob(ctx, &volume_server_pb.WriteNeedleBlobRequest{
		VolumeId:   1,
		NeedleId:   2,
		NeedleBlob: []byte("def"),
		Size:       3,
	})
	if err == nil {
		t.Fatalf("WriteNeedleBlob should fail in maintenance mode")
	}
	if !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("WriteNeedleBlob maintenance mode error mismatch: %v", err)
	}
}
