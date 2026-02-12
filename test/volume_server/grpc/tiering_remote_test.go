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

func TestFetchAndWriteNeedleMaintenanceAndMissingVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.FetchAndWriteNeedle(ctx, &volume_server_pb.FetchAndWriteNeedleRequest{
		VolumeId: 98781,
		NeedleId: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "not found volume id") {
		t.Fatalf("FetchAndWriteNeedle missing-volume error mismatch: %v", err)
	}

	stateResp, err := grpcClient.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	_, err = grpcClient.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{
			Maintenance: true,
			Version:     stateResp.GetState().GetVersion(),
		},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}

	_, err = grpcClient.FetchAndWriteNeedle(ctx, &volume_server_pb.FetchAndWriteNeedleRequest{
		VolumeId: 1,
		NeedleId: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("FetchAndWriteNeedle maintenance error mismatch: %v", err)
	}
}

func TestVolumeTierMoveDatToRemoteErrorPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(85)
	const collection = "tier-collection"
	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	missingStream, err := grpcClient.VolumeTierMoveDatToRemote(ctx, &volume_server_pb.VolumeTierMoveDatToRemoteRequest{
		VolumeId:               98782,
		Collection:             collection,
		DestinationBackendName: "dummy",
	})
	if err == nil {
		_, err = missingStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VolumeTierMoveDatToRemote missing-volume error mismatch: %v", err)
	}

	mismatchStream, err := grpcClient.VolumeTierMoveDatToRemote(ctx, &volume_server_pb.VolumeTierMoveDatToRemoteRequest{
		VolumeId:               volumeID,
		Collection:             "wrong-collection",
		DestinationBackendName: "dummy",
	})
	if err == nil {
		_, err = mismatchStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "unexpected input") {
		t.Fatalf("VolumeTierMoveDatToRemote collection mismatch error mismatch: %v", err)
	}

	stateResp, err := grpcClient.GetState(ctx, &volume_server_pb.GetStateRequest{})
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	_, err = grpcClient.SetState(ctx, &volume_server_pb.SetStateRequest{
		State: &volume_server_pb.VolumeServerState{
			Maintenance: true,
			Version:     stateResp.GetState().GetVersion(),
		},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}

	maintenanceStream, err := grpcClient.VolumeTierMoveDatToRemote(ctx, &volume_server_pb.VolumeTierMoveDatToRemoteRequest{
		VolumeId:               volumeID,
		Collection:             collection,
		DestinationBackendName: "dummy",
	})
	if err == nil {
		_, err = maintenanceStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeTierMoveDatToRemote maintenance error mismatch: %v", err)
	}
}

func TestVolumeTierMoveDatFromRemoteErrorPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(86)
	const collection = "tier-download-collection"
	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	missingStream, err := grpcClient.VolumeTierMoveDatFromRemote(ctx, &volume_server_pb.VolumeTierMoveDatFromRemoteRequest{
		VolumeId:   98783,
		Collection: collection,
	})
	if err == nil {
		_, err = missingStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VolumeTierMoveDatFromRemote missing-volume error mismatch: %v", err)
	}

	mismatchStream, err := grpcClient.VolumeTierMoveDatFromRemote(ctx, &volume_server_pb.VolumeTierMoveDatFromRemoteRequest{
		VolumeId:   volumeID,
		Collection: "wrong-collection",
	})
	if err == nil {
		_, err = mismatchStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "unexpected input") {
		t.Fatalf("VolumeTierMoveDatFromRemote collection mismatch error mismatch: %v", err)
	}

	localDiskStream, err := grpcClient.VolumeTierMoveDatFromRemote(ctx, &volume_server_pb.VolumeTierMoveDatFromRemoteRequest{
		VolumeId:   volumeID,
		Collection: collection,
	})
	if err == nil {
		_, err = localDiskStream.Recv()
	}
	if err == nil || !strings.Contains(err.Error(), "already on local disk") {
		t.Fatalf("VolumeTierMoveDatFromRemote local-disk error mismatch: %v", err)
	}
}
