package volume_server_grpc_test

import (
	"context"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestVolumeEcShardsInfoReturnsAllShardsAcrossDisks drives the full path
// behind the ec.encode source-deletion gate. A multi-disk volume server
// ends up with EC shards split across disks (each registers its own
// EcVolume entry in DiskLocation.ecVolumes), and the volume server's
// VolumeEcShardsInfo RPC must walk every DiskLocation rather than
// reporting whichever disk Store.FindEcVolume picks first.
//
// Pre-fix, verifyEcShardsBeforeDelete refused to delete source volumes —
// the shard-bitmap union across destinations fell short of dataShards +
// parityShards because each destination only reported shards on one of
// its disks. With the handler fix, the same VerifyShardsAcrossServers
// call returns a complete bitmap and the gate opens.
func TestVolumeEcShardsInfoReturnsAllShardsAcrossDisks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		dataDirCount = 2
		volumeID     = uint32(9558)
		collection   = "ec-multi-disk-verify"
	)

	clusterHarness := framework.StartSingleVolumeClusterWithDataDirs(t, matrix.P1(), dataDirCount)
	dataDirs := clusterHarness.VolumeDataDirs()
	if len(dataDirs) != dataDirCount {
		t.Fatalf("expected %d data dirs, got %d: %v", dataDirCount, len(dataDirs), dataDirs)
	}

	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	httpClient := framework.NewHTTPClient()
	needles := []struct {
		fid     string
		payload []byte
	}{
		{framework.NewFileID(volumeID, 9559, 0xC0FFEE01), bytesOfLen(64, 0xB1)},
		{framework.NewFileID(volumeID, 9560, 0xC0FFEE02), bytesOfLen(8192, 0xB2)},
		{framework.NewFileID(volumeID, 9561, 0xC0FFEE03), bytesOfLen(131072, 0xB3)},
	}
	for _, n := range needles {
		resp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload)
		_ = framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload %s expected 201, got %d", n.fid, resp.StatusCode)
		}
	}

	if _, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   volumeID,
		Collection: collection,
	}); err != nil {
		t.Fatalf("VolumeEcShardsGenerate: %v", err)
	}

	// Generate places every shard plus the .ecx/.ecj/.vif on the .dat's
	// disk (disk 0). Mount all 14 there first so the next step's restart
	// has a steady starting state.
	allShards := make([]uint32, erasure_coding.TotalShardsCount)
	for i := range allShards {
		allShards[i] = uint32(i)
	}
	if _, err := grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   volumeID,
		Collection: collection,
		ShardIds:   allShards,
	}); err != nil {
		t.Fatalf("VolumeEcShardsMount all shards: %v", err)
	}

	// Drop the .dat so the EC shards are the only data path — mirrors the
	// real ec.encode flow before verifyEcShardsBeforeDelete fires.
	if _, err := grpcClient.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
		VolumeId: volumeID,
	}); err != nil {
		t.Fatalf("VolumeDelete (drop .dat): %v", err)
	}

	// Move half the shards onto disk 1, leaving .ecx on disk 0. After
	// restart, the cross-disk reconcile path attaches each disk's shards
	// against its own EcVolume entry — the exact in-memory shape the bug
	// reporter saw on a multi-disk destination.
	clusterHarness.StopVolumeServer()
	const splitAt = 7
	for shard := 0; shard < splitAt; shard++ {
		movedFile(t, dataDirs[0], dataDirs[1], collection, volumeID, erasure_coding.ToExt(shard))
	}
	if fileExistsIn(dataDirs[1], collection, volumeID, ".ecx") {
		t.Fatalf("setup: .ecx must stay on disk 0 to exercise the multi-disk path")
	}

	clusterHarness.RestartVolumeServer()
	conn2, grpcClient2 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn2.Close()

	postReconcileLayout := scanShardLayout(t, dataDirs, collection, volumeID)
	if got, want := totalShardsInLayout(postReconcileLayout), erasure_coding.TotalShardsCount; got != want {
		t.Fatalf("post-reconcile: total shards on disk mismatch: got %d, want %d (layout=%v)", got, want, postReconcileLayout)
	}
	if len(postReconcileLayout[0]) == 0 || len(postReconcileLayout[1]) == 0 {
		t.Fatalf("post-reconcile: expected shards on BOTH disks, got per-disk layout %v", postReconcileLayout)
	}

	// Direct RPC assertion: VolumeEcShardsInfo must report every shard
	// the server holds, not just the ones registered against the first
	// matching DiskLocation.
	infoResp, err := grpcClient2.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsInfo: %v", err)
	}
	gotShardIds := make([]int, 0, len(infoResp.GetEcShardInfos()))
	for _, info := range infoResp.GetEcShardInfos() {
		if info.GetVolumeId() != volumeID {
			t.Errorf("EcShardInfo VolumeId=%d, want %d", info.GetVolumeId(), volumeID)
		}
		gotShardIds = append(gotShardIds, int(info.GetShardId()))
	}
	sort.Ints(gotShardIds)
	wantShardIds := make([]int, erasure_coding.TotalShardsCount)
	for i := range wantShardIds {
		wantShardIds[i] = i
	}
	if len(gotShardIds) != len(wantShardIds) {
		t.Fatalf("VolumeEcShardsInfo returned %d shards (ids=%v), want %d (ids=%v) — per-disk layout=%v",
			len(gotShardIds), gotShardIds, len(wantShardIds), wantShardIds, postReconcileLayout)
	}
	for i, sid := range wantShardIds {
		if gotShardIds[i] != sid {
			t.Fatalf("VolumeEcShardsInfo shard ids=%v, want %v (per-disk layout=%v)",
				gotShardIds, wantShardIds, postReconcileLayout)
		}
	}

	// End-to-end assertion via the same helper the worker uses to gate
	// source-volume deletion (weed/worker/tasks/erasure_coding/ec_task.go
	// verifyEcShardsBeforeDelete). The union across destinations is what
	// RequireRecoverableShardSet measures; with one destination that holds
	// every shard, the union must cover dataShards + parityShards without
	// being reported as degraded.
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	servers := []string{clusterHarness.VolumeServerAddress()}
	union, perServer := erasure_coding.VerifyShardsAcrossServers(ctx, volumeID, servers, dialOption)
	degraded, err := erasure_coding.RequireRecoverableShardSet(
		volumeID, union, erasure_coding.DataShardsCount, erasure_coding.TotalShardsCount)
	if err != nil {
		t.Fatalf("verifyEcShardsBeforeDelete-equivalent gate failed: %v\nper-server inventory: %s\nper-disk layout: %v",
			err, erasure_coding.SummarizeShardInventory(perServer), postReconcileLayout)
	}
	if degraded {
		t.Fatalf("verifyEcShardsBeforeDelete-equivalent gate reported a degraded shard set\nper-server inventory: %s\nper-disk layout: %v",
			erasure_coding.SummarizeShardInventory(perServer), postReconcileLayout)
	}
	if got, want := union.Count(), erasure_coding.TotalShardsCount; got != want {
		t.Fatalf("VerifyShardsAcrossServers union covered %d/%d shards (per-server=%s, layout=%v)",
			got, want, erasure_coding.SummarizeShardInventory(perServer), postReconcileLayout)
	}
}
