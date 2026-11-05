package volume_server_grpc_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestEcShardsToVolumeRejectsCorruptDataShard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(123)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(volumeID, 990007, 0x66771123)
	payload := []byte("ec-shards-to-volume-corrupt-data-shard")
	uploadResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, payload)
	_ = framework.ReadAllAndClose(t, uploadResp)
	if uploadResp.StatusCode != http.StatusCreated {
		t.Fatalf("upload expected 201, got %d", uploadResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID,
	}); err != nil {
		t.Fatalf("VolumeEcShardsGenerate: %v", err)
	}
	if _, err := grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID,
		ShardIds: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
	}); err != nil {
		t.Fatalf("VolumeEcShardsMount: %v", err)
	}

	dataDir := filepath.Join(clusterHarness.BaseDir(), "volume")
	base := filepath.Join(dataDir, fmt.Sprintf("%d", volumeID))
	if _, err := os.Stat(base + ".ecsum"); err != nil {
		t.Fatalf("precondition: the encode wrote a bitrot sidecar: %v", err)
	}
	shard := base + ".ec03"
	content, err := os.ReadFile(shard)
	if err != nil {
		t.Fatalf("read %s: %v", shard, err)
	}
	content[0] ^= 0xff
	if err := os.WriteFile(shard, content, 0644); err != nil {
		t.Fatalf("corrupt %s: %v", shard, err)
	}

	_, err = grpcClient.VolumeEcShardsToVolume(ctx, &volume_server_pb.VolumeEcShardsToVolumeRequest{
		VolumeId: volumeID,
	})
	if err == nil || !strings.Contains(err.Error(), "data shard 3") {
		t.Fatalf("VolumeEcShardsToVolume with a corrupt data shard: %v", err)
	}

	for _, ext := range []string{".ecsum", ".ec00", ".ec03", ".ec13"} {
		if _, statErr := os.Stat(base + ext); statErr != nil {
			t.Fatalf("%s must survive the refused decode: %v", ext, statErr)
		}
	}

	readResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid)
	readBody := framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode != http.StatusOK || string(readBody) != string(payload) {
		t.Fatalf("EC read after the refused decode: status %d body %q", readResp.StatusCode, readBody)
	}
}
