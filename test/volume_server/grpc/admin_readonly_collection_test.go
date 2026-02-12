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

func TestVolumeMarkReadonlyAndWritableLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(72)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
		Persist:  false,
	})
	if err != nil {
		t.Fatalf("VolumeMarkReadonly failed: %v", err)
	}

	readOnlyStatus, err := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeStatus after readonly failed: %v", err)
	}
	if !readOnlyStatus.GetIsReadOnly() {
		t.Fatalf("VolumeStatus expected readonly=true after VolumeMarkReadonly")
	}

	_, err = grpcClient.VolumeMarkWritable(ctx, &volume_server_pb.VolumeMarkWritableRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeMarkWritable failed: %v", err)
	}

	writableStatus, err := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeStatus after writable failed: %v", err)
	}
	if writableStatus.GetIsReadOnly() {
		t.Fatalf("VolumeStatus expected readonly=false after VolumeMarkWritable")
	}
}

func TestVolumeMarkReadonlyPersistTrue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(74)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
		Persist:  true,
	})
	if err != nil {
		t.Fatalf("VolumeMarkReadonly persist=true failed: %v", err)
	}

	statusResp, err := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeStatus after persist readonly failed: %v", err)
	}
	if !statusResp.GetIsReadOnly() {
		t.Fatalf("VolumeStatus expected readonly=true after persist readonly")
	}
}

func TestVolumeMarkReadonlyWritableErrorPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: 98771, Persist: true})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VolumeMarkReadonly missing-volume error mismatch: %v", err)
	}

	_, err = grpcClient.VolumeMarkWritable(ctx, &volume_server_pb.VolumeMarkWritableRequest{VolumeId: 98772})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VolumeMarkWritable missing-volume error mismatch: %v", err)
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

	_, err = grpcClient.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: 1, Persist: true})
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeMarkReadonly maintenance error mismatch: %v", err)
	}

	_, err = grpcClient.VolumeMarkWritable(ctx, &volume_server_pb.VolumeMarkWritableRequest{VolumeId: 1})
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeMarkWritable maintenance error mismatch: %v", err)
	}
}

func TestDeleteCollectionRemovesVolumeAndIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(73)
	const collection = "it-delete-collection"

	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeStatus before DeleteCollection failed: %v", err)
	}

	_, err = grpcClient.DeleteCollection(ctx, &volume_server_pb.DeleteCollectionRequest{Collection: collection})
	if err != nil {
		t.Fatalf("DeleteCollection existing collection failed: %v", err)
	}

	_, err = grpcClient.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	if err == nil {
		t.Fatalf("VolumeStatus should fail after collection delete")
	}
	if !strings.Contains(err.Error(), "not found volume") {
		t.Fatalf("VolumeStatus after DeleteCollection error mismatch: %v", err)
	}

	_, err = grpcClient.DeleteCollection(ctx, &volume_server_pb.DeleteCollectionRequest{Collection: collection})
	if err != nil {
		t.Fatalf("DeleteCollection idempotent retry failed: %v", err)
	}
}
