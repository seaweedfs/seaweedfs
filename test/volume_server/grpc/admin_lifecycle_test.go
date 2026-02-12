package volume_server_grpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVolumeAdminLifecycleRPCs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const volumeID = uint32(11)
	framework.AllocateVolume(t, client, volumeID, "")

	statusResp, err := client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeStatus failed: %v", err)
	}
	if statusResp.GetFileCount() != 0 {
		t.Fatalf("new volume should be empty, got file_count=%d", statusResp.GetFileCount())
	}

	if _, err = client.VolumeUnmount(ctx, &volume_server_pb.VolumeUnmountRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("VolumeUnmount failed: %v", err)
	}
	if _, err = client.VolumeMount(ctx, &volume_server_pb.VolumeMountRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("VolumeMount failed: %v", err)
	}

	if _, err = client.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{VolumeId: volumeID, OnlyEmpty: true}); err != nil {
		t.Fatalf("VolumeDelete failed: %v", err)
	}

	_, err = client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err == nil {
		t.Fatalf("VolumeStatus should fail after delete")
	}
	if st, ok := status.FromError(err); !ok || st.Code() == codes.OK {
		t.Fatalf("VolumeStatus error should be a non-OK grpc status, got: %v", err)
	}
}

func TestMaintenanceModeRejectsAllocateVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stateResp, err := client.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	_, err = client.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{Maintenance: true, Version: stateResp.GetState().GetVersion()},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}

	_, err = client.AllocateVolume(ctx, &volume_server_pb.AllocateVolumeRequest{VolumeId: 12, Replication: "000"})
	if err == nil {
		t.Fatalf("AllocateVolume should fail when maintenance mode is enabled")
	}
	if !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("expected maintenance mode error, got: %v", err)
	}
}
