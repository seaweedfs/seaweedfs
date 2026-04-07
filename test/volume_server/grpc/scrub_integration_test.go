package volume_server_grpc_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// --- Normal volume scrub tests ---

func TestScrubVolumeFullHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(200)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), framework.NewFileID(volumeID, 1, 1), []byte("data-one"))
	framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), framework.NewFileID(volumeID, 2, 2), []byte("data-two"))
	framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), framework.NewFileID(volumeID, 3, 3), []byte("data-three"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode_FULL,
	})
	if err != nil {
		t.Fatalf("ScrubVolume FULL on healthy volume failed: %v", err)
	}
	if resp.GetTotalVolumes() != 1 {
		t.Fatalf("expected total_volumes=1, got %d", resp.GetTotalVolumes())
	}
	if resp.GetTotalFiles() != 3 {
		t.Fatalf("expected total_files=3, got %d", resp.GetTotalFiles())
	}
	if len(resp.GetBrokenVolumeIds()) != 0 {
		t.Fatalf("expected no broken volumes, got %v: %v", resp.GetBrokenVolumeIds(), resp.GetDetails())
	}
}

func TestScrubVolumeFullCorruptData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(201)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	httpClient := framework.NewHTTPClient()
	framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), framework.NewFileID(volumeID, 1, 1), []byte("important data"))

	framework.CorruptDatFile(t, clusterHarness.BaseDir(), volumeID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode_FULL,
	})
	if err != nil {
		t.Fatalf("ScrubVolume FULL on corrupt volume failed: %v", err)
	}
	if len(resp.GetBrokenVolumeIds()) == 0 {
		t.Fatalf("expected broken volume after data corruption, got none")
	}
	if len(resp.GetDetails()) == 0 {
		t.Fatalf("expected error details for corrupt volume")
	}
}

func TestScrubVolumeMixedHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const healthyVol = uint32(202)
	const corruptVol = uint32(203)
	framework.AllocateVolume(t, grpcClient, healthyVol, "")
	framework.AllocateVolume(t, grpcClient, corruptVol, "")

	httpClient := framework.NewHTTPClient()
	framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), framework.NewFileID(healthyVol, 1, 1), []byte("healthy"))
	framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), framework.NewFileID(corruptVol, 1, 1), []byte("will corrupt"))

	framework.CorruptIndexFile(t, clusterHarness.BaseDir(), corruptVol)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{healthyVol, corruptVol},
		Mode:      volume_server_pb.VolumeScrubMode_INDEX,
	})
	if err != nil {
		t.Fatalf("ScrubVolume INDEX on mixed volumes failed: %v", err)
	}
	if resp.GetTotalVolumes() != 2 {
		t.Fatalf("expected total_volumes=2, got %d", resp.GetTotalVolumes())
	}
	if len(resp.GetBrokenVolumeIds()) != 1 {
		t.Fatalf("expected exactly 1 broken volume, got %v", resp.GetBrokenVolumeIds())
	}
	if resp.GetBrokenVolumeIds()[0] != corruptVol {
		t.Fatalf("expected broken volume %d, got %d", corruptVol, resp.GetBrokenVolumeIds()[0])
	}
}

func TestScrubVolumeMissingVolumeReturnsError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartSingleVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := grpcClient.ScrubVolume(ctx, &volume_server_pb.ScrubVolumeRequest{
		VolumeIds: []uint32{99999},
		Mode:      volume_server_pb.VolumeScrubMode_FULL,
	})
	if err == nil {
		t.Fatalf("ScrubVolume should fail for missing volume")
	}
}

// --- EC volume scrub tests ---

// ecSetup creates a volume, uploads data, generates EC shards, and mounts all of them.
func ecSetup(t *testing.T, grpcClient volume_server_pb.VolumeServerClient, httpClient *http.Client, volumeURL string, volumeID uint32) {
	t.Helper()
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	fid := framework.NewFileID(volumeID, 1, 0xABCD0001)
	uploadResp := framework.UploadBytes(t, httpClient, volumeURL, fid, []byte("ec-scrub-test-data-payload"))
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

	allShards := make([]uint32, erasure_coding.TotalShardsCount)
	for i := range allShards {
		allShards[i] = uint32(i)
	}
	_, err = grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   volumeID,
		Collection: "",
		ShardIds:   allShards,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsMount all shards failed: %v", err)
	}
}

func TestScrubEcVolumeIndexHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(210)
	httpClient := framework.NewHTTPClient()
	ecSetup(t, grpcClient, httpClient, clusterHarness.VolumeAdminURL(), volumeID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := grpcClient.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode_INDEX,
	})
	if err != nil {
		t.Fatalf("ScrubEcVolume INDEX on healthy volume failed: %v", err)
	}
	if resp.GetTotalVolumes() != 1 {
		t.Fatalf("expected total_volumes=1, got %d", resp.GetTotalVolumes())
	}
	if len(resp.GetBrokenVolumeIds()) != 0 {
		t.Fatalf("expected no broken volumes, got %v: %v", resp.GetBrokenVolumeIds(), resp.GetDetails())
	}
}

func TestScrubEcVolumeLocalHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(211)
	httpClient := framework.NewHTTPClient()
	ecSetup(t, grpcClient, httpClient, clusterHarness.VolumeAdminURL(), volumeID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := grpcClient.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode_LOCAL,
	})
	if err != nil {
		t.Fatalf("ScrubEcVolume LOCAL on healthy volume failed: %v", err)
	}
	if resp.GetTotalVolumes() != 1 {
		t.Fatalf("expected total_volumes=1, got %d", resp.GetTotalVolumes())
	}
	if resp.GetTotalFiles() < 1 {
		t.Fatalf("expected at least 1 file, got %d", resp.GetTotalFiles())
	}
	if len(resp.GetBrokenVolumeIds()) != 0 {
		t.Fatalf("expected no broken volumes, got %v: %v", resp.GetBrokenVolumeIds(), resp.GetDetails())
	}
	if len(resp.GetBrokenShardInfos()) != 0 {
		t.Fatalf("expected no broken shards, got %v", resp.GetBrokenShardInfos())
	}
}

func TestScrubEcVolumeLocalCorruptShard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(212)
	httpClient := framework.NewHTTPClient()
	ecSetup(t, grpcClient, httpClient, clusterHarness.VolumeAdminURL(), volumeID)

	// Corrupt shard 0 by truncating it.
	framework.CorruptEcShardFile(t, clusterHarness.BaseDir(), volumeID, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := grpcClient.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode_LOCAL,
	})
	if err != nil {
		t.Fatalf("ScrubEcVolume LOCAL on corrupt shard failed: %v", err)
	}
	if len(resp.GetBrokenVolumeIds()) == 0 {
		t.Fatalf("expected broken volume after shard corruption")
	}
	if len(resp.GetBrokenShardInfos()) == 0 {
		t.Fatalf("expected broken shard info after shard corruption")
	}
	// Verify the reported broken shard is shard 0.
	foundShard0 := false
	for _, si := range resp.GetBrokenShardInfos() {
		if si.GetShardId() == 0 {
			foundShard0 = true
			break
		}
	}
	if !foundShard0 {
		t.Fatalf("expected shard 0 in broken shard infos, got %v", resp.GetBrokenShardInfos())
	}
}

func TestScrubEcVolumeAutoSelectWithEcPresent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeIDA = uint32(213)
	const volumeIDB = uint32(214)
	httpClient := framework.NewHTTPClient()
	ecSetup(t, grpcClient, httpClient, clusterHarness.VolumeAdminURL(), volumeIDA)
	ecSetup(t, grpcClient, httpClient, clusterHarness.VolumeAdminURL(), volumeIDB)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Auto-select (empty VolumeIds) should find both EC volumes.
	resp, err := grpcClient.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
		Mode: volume_server_pb.VolumeScrubMode_INDEX,
	})
	if err != nil {
		t.Fatalf("ScrubEcVolume auto-select failed: %v", err)
	}
	if resp.GetTotalVolumes() < 2 {
		t.Fatalf("expected at least 2 EC volumes via auto-select, got %d", resp.GetTotalVolumes())
	}
	if len(resp.GetBrokenVolumeIds()) != 0 {
		t.Fatalf("expected no broken volumes, got %v", resp.GetBrokenVolumeIds())
	}
}

func TestScrubEcVolumeUnsupportedMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(215)
	httpClient := framework.NewHTTPClient()
	ecSetup(t, grpcClient, httpClient, clusterHarness.VolumeAdminURL(), volumeID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := grpcClient.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode(99),
	})
	if err == nil {
		t.Fatalf("ScrubEcVolume should fail for unsupported mode")
	}
}

func TestScrubEcVolumeIndexCorruptEcx(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(216)
	httpClient := framework.NewHTTPClient()
	ecSetup(t, grpcClient, httpClient, clusterHarness.VolumeAdminURL(), volumeID)

	// Corrupt the .ecx index file by appending garbage.
	framework.CorruptEcxFile(t, clusterHarness.BaseDir(), volumeID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := grpcClient.ScrubEcVolume(ctx, &volume_server_pb.ScrubEcVolumeRequest{
		VolumeIds: []uint32{volumeID},
		Mode:      volume_server_pb.VolumeScrubMode_INDEX,
	})
	if err != nil {
		t.Fatalf("ScrubEcVolume INDEX on corrupt ecx failed: %v", err)
	}
	if len(resp.GetBrokenVolumeIds()) == 0 {
		t.Fatalf("expected broken volume after ECX corruption")
	}
}
