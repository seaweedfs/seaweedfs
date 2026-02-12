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

func TestVacuumVolumeCheckSuccessAndMissingVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(31)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := grpcClient.VacuumVolumeCheck(ctx, &volume_server_pb.VacuumVolumeCheckRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VacuumVolumeCheck existing volume failed: %v", err)
	}
	if resp.GetGarbageRatio() < 0 || resp.GetGarbageRatio() > 1 {
		t.Fatalf("unexpected garbage ratio: %f", resp.GetGarbageRatio())
	}

	_, err = grpcClient.VacuumVolumeCheck(ctx, &volume_server_pb.VacuumVolumeCheckRequest{VolumeId: 99999})
	if err == nil {
		t.Fatalf("VacuumVolumeCheck should fail for missing volume")
	}
}

func TestVacuumMaintenanceModeRejections(t *testing.T) {
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

	assertMaintenanceErr := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s should fail in maintenance mode", name)
		}
		if !strings.Contains(err.Error(), "maintenance mode") {
			t.Fatalf("%s expected maintenance mode error, got: %v", name, err)
		}
	}

	compactStream, err := grpcClient.VacuumVolumeCompact(ctx, &volume_server_pb.VacuumVolumeCompactRequest{VolumeId: 31})
	if err == nil {
		_, err = compactStream.Recv()
	}
	assertMaintenanceErr("VacuumVolumeCompact", err)

	_, err = grpcClient.VacuumVolumeCommit(ctx, &volume_server_pb.VacuumVolumeCommitRequest{VolumeId: 31})
	assertMaintenanceErr("VacuumVolumeCommit", err)

	_, err = grpcClient.VacuumVolumeCleanup(ctx, &volume_server_pb.VacuumVolumeCleanupRequest{VolumeId: 31})
	assertMaintenanceErr("VacuumVolumeCleanup", err)
}
