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
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestEcMaintenanceModeRejections(t *testing.T) {
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
		State: &volume_server_pb.VolumeServerState{
			Maintenance: true,
			Version:     stateResp.GetState().GetVersion(),
		},
	})
	if err != nil {
		t.Fatalf("SetState maintenance=true failed: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{VolumeId: 1, Collection: ""})
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeEcShardsGenerate maintenance error mismatch: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       1,
		Collection:     "",
		SourceDataNode: "127.0.0.1:1",
		ShardIds:       []uint32{0},
	})
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeEcShardsCopy maintenance error mismatch: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
		VolumeId:   1,
		Collection: "",
		ShardIds:   []uint32{0},
	})
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeEcShardsDelete maintenance error mismatch: %v", err)
	}

	_, err = grpcClient.VolumeEcBlobDelete(ctx, &volume_server_pb.VolumeEcBlobDeleteRequest{
		VolumeId:   1,
		Collection: "",
		FileKey:    1,
		Version:    3,
	})
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeEcBlobDelete maintenance error mismatch: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsToVolume(ctx, &volume_server_pb.VolumeEcShardsToVolumeRequest{
		VolumeId:   1,
		Collection: "",
	})
	if err == nil || !strings.Contains(err.Error(), "maintenance mode") {
		t.Fatalf("VolumeEcShardsToVolume maintenance error mismatch: %v", err)
	}
}

func TestEcMissingInvalidAndNoopPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   98791,
		Collection: "",
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VolumeEcShardsGenerate missing-volume error mismatch: %v", err)
	}

	rebuildResp, err := grpcClient.VolumeEcShardsRebuild(ctx, &volume_server_pb.VolumeEcShardsRebuildRequest{
		VolumeId:   98792,
		Collection: "ec-rebuild",
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsRebuild missing-volume should return empty success, got: %v", err)
	}
	if len(rebuildResp.GetRebuiltShardIds()) != 0 {
		t.Fatalf("VolumeEcShardsRebuild expected no rebuilt shards for missing volume, got %v", rebuildResp.GetRebuiltShardIds())
	}

	_, err = grpcClient.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       98793,
		Collection:     "ec-copy",
		SourceDataNode: "127.0.0.1:1",
		ShardIds:       []uint32{0},
		DiskId:         99,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid disk_id") {
		t.Fatalf("VolumeEcShardsCopy invalid-disk error mismatch: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
		VolumeId:   98794,
		Collection: "ec-delete",
		ShardIds:   []uint32{0, 1},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsDelete missing-volume should be no-op success, got: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   98795,
		Collection: "ec-mount",
		ShardIds:   []uint32{0},
	})
	if err == nil {
		t.Fatalf("VolumeEcShardsMount should fail for missing EC shards")
	}

	_, err = grpcClient.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
		VolumeId: 98796,
		ShardIds: []uint32{0},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsUnmount missing shards should be no-op success, got: %v", err)
	}

	readStream, err := grpcClient.VolumeEcShardRead(ctx, &volume_server_pb.VolumeEcShardReadRequest{
		VolumeId: 98797,
		ShardId:  0,
		Offset:   0,
		Size:     1,
	})
	if err == nil {
		_, err = readStream.Recv()
	}
	if err == nil || err == io.EOF {
		t.Fatalf("VolumeEcShardRead should fail for missing EC volume")
	}

	_, err = grpcClient.VolumeEcBlobDelete(ctx, &volume_server_pb.VolumeEcBlobDeleteRequest{
		VolumeId:   98798,
		Collection: "ec-blob",
		FileKey:    1,
		Version:    3,
	})
	if err != nil {
		t.Fatalf("VolumeEcBlobDelete missing local EC volume should be no-op success, got: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsToVolume(ctx, &volume_server_pb.VolumeEcShardsToVolumeRequest{
		VolumeId:   98799,
		Collection: "ec-to-volume",
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VolumeEcShardsToVolume missing-volume error mismatch: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: 98800,
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VolumeEcShardsInfo missing-volume error mismatch: %v", err)
	}
}

func TestEcGenerateMountInfoUnmountLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(115)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 990001, 0x1234ABCD)
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, []byte("ec-generate-lifecycle-content"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   volumeID,
		Collection: "",
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsGenerate success path failed: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   volumeID,
		Collection: "",
		ShardIds:   []uint32{0},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsMount success path failed: %v", err)
	}

	infoResp, err := grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsInfo after mount failed: %v", err)
	}
	if len(infoResp.GetEcShardInfos()) == 0 {
		t.Fatalf("VolumeEcShardsInfo expected non-empty shard infos after mount")
	}
	if infoResp.GetVolumeSize() == 0 {
		t.Fatalf("VolumeEcShardsInfo expected non-zero volume size after mount")
	}

	_, err = grpcClient.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
		VolumeId: volumeID,
		ShardIds: []uint32{0},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsUnmount success path failed: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: volumeID,
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("VolumeEcShardsInfo after unmount expected not-found error, got: %v", err)
	}
}

func TestEcShardReadAndBlobDeleteLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(116)
	const fileKey = uint64(990002)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, fileKey, 0x2233CCDD)
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, []byte("ec-shard-read-delete-content"))
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   volumeID,
		Collection: "",
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsGenerate failed: %v", err)
	}

	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   volumeID,
		Collection: "",
		ShardIds:   []uint32{0},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsMount failed: %v", err)
	}

	readStream, err := grpcClient.VolumeEcShardRead(ctx, &volume_server_pb.VolumeEcShardReadRequest{
		VolumeId: volumeID,
		ShardId:  0,
		Offset:   0,
		Size:     1,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardRead start failed: %v", err)
	}
	firstChunk, err := readStream.Recv()
	if err != nil {
		t.Fatalf("VolumeEcShardRead recv failed: %v", err)
	}
	if len(firstChunk.GetData()) == 0 {
		t.Fatalf("VolumeEcShardRead expected non-empty data chunk before deletion")
	}

	_, err = grpcClient.VolumeEcBlobDelete(ctx, &volume_server_pb.VolumeEcBlobDeleteRequest{
		VolumeId:   volumeID,
		Collection: "",
		FileKey:    fileKey,
		Version:    uint32(needle.GetCurrentVersion()),
	})
	if err != nil {
		t.Fatalf("VolumeEcBlobDelete first delete failed: %v", err)
	}

	_, err = grpcClient.VolumeEcBlobDelete(ctx, &volume_server_pb.VolumeEcBlobDeleteRequest{
		VolumeId:   volumeID,
		Collection: "",
		FileKey:    fileKey,
		Version:    uint32(needle.GetCurrentVersion()),
	})
	if err != nil {
		t.Fatalf("VolumeEcBlobDelete second delete should be idempotent success, got: %v", err)
	}

	deletedStream, err := grpcClient.VolumeEcShardRead(ctx, &volume_server_pb.VolumeEcShardReadRequest{
		VolumeId: volumeID,
		ShardId:  0,
		FileKey:  fileKey,
		Offset:   0,
		Size:     1,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardRead deleted-check start failed: %v", err)
	}
	deletedMsg, err := deletedStream.Recv()
	if err != nil {
		t.Fatalf("VolumeEcShardRead deleted-check recv failed: %v", err)
	}
	if !deletedMsg.GetIsDeleted() {
		t.Fatalf("VolumeEcShardRead expected IsDeleted=true after blob delete")
	}
	_, err = deletedStream.Recv()
	if err != io.EOF {
		t.Fatalf("VolumeEcShardRead deleted-check expected EOF after deleted marker, got: %v", err)
	}
}
