package volume_server_grpc_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// TestEcLifecycleAcrossMultipleDisks drives encode, mount, read, drop-dat,
// cross-disk redistribute + restart + reconcile, blob delete, and rebuild on
// a 2-disk volume server.
func TestEcLifecycleAcrossMultipleDisks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		dataDirCount = 2
		volumeID     = uint32(9500)
		collection   = "ec-multi-disk"
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

	// populate
	type needleSpec struct {
		fid     string
		payload []byte
	}
	needles := []needleSpec{
		{framework.NewFileID(volumeID, 9501, 0xAA000001), bytesOfLen(11, 0xA1)},
		{framework.NewFileID(volumeID, 9502, 0xAA000002), bytesOfLen(1024, 0xA2)},
		{framework.NewFileID(volumeID, 9503, 0xAA000003), bytesOfLen(63*1024+17, 0xA3)},
		{framework.NewFileID(volumeID, 9504, 0xAA000004), bytesOfLen(2, 0xA4)},
		{framework.NewFileID(volumeID, 9505, 0xAA000005), bytesOfLen(513*1024, 0xA5)},
	}
	httpClient := framework.NewHTTPClient()
	for _, n := range needles {
		resp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload)
		_ = framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload %s expected 201, got %d", n.fid, resp.StatusCode)
		}
	}

	// encode — every shard lands on the .dat's disk
	if _, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   volumeID,
		Collection: collection,
	}); err != nil {
		t.Fatalf("VolumeEcShardsGenerate: %v", err)
	}
	postGenerateLayout := scanShardLayout(t, dataDirs, collection, volumeID)
	if len(postGenerateLayout[0]) != erasure_coding.TotalShardsCount {
		t.Fatalf("post-generate: expected all %d shards on disk 0, got per-disk layout %v",
			erasure_coding.TotalShardsCount, postGenerateLayout)
	}
	if len(postGenerateLayout[1]) != 0 {
		t.Fatalf("post-generate: disk 1 should be empty, got %v", postGenerateLayout[1])
	}
	// .ecj is created lazily on first EC delete
	for _, side := range []string{".ecx", ".vif"} {
		if !fileExistsIn(dataDirs[0], collection, volumeID, side) {
			t.Fatalf("post-generate: expected %s on disk 0", side)
		}
	}

	// mount + read via EC
	if _, err := grpcClient.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   volumeID,
		Collection: collection,
		ShardIds:   dataShardIds(),
	}); err != nil {
		t.Fatalf("VolumeEcShardsMount data shards: %v", err)
	}
	for _, n := range needles {
		verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload, "after-mount")
	}

	// drop the original volume so EC shards are the only source
	if _, err := grpcClient.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
		VolumeId: volumeID,
	}); err != nil {
		t.Fatalf("VolumeDelete (drop .dat): %v", err)
	}
	if fileExistsIn(dataDirs[0], collection, volumeID, ".dat") {
		t.Fatalf("VolumeDelete left .dat in place on disk 0")
	}
	for _, n := range needles {
		verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload, "after-dat-delete")
	}

	// move half the shards onto disk 1, leaving .ecx on disk 0
	clusterHarness.StopVolumeServer()
	const splitAt = 7
	for shard := 0; shard < splitAt; shard++ {
		movedFile(t, dataDirs[0], dataDirs[1], collection, volumeID, erasure_coding.ToExt(shard))
	}
	postMoveLayout := scanShardLayout(t, dataDirs, collection, volumeID)
	if len(postMoveLayout[0]) != erasure_coding.TotalShardsCount-splitAt {
		t.Fatalf("post-move: expected %d shards on disk 0, got %v", erasure_coding.TotalShardsCount-splitAt, postMoveLayout[0])
	}
	if len(postMoveLayout[1]) != splitAt {
		t.Fatalf("post-move: expected %d shards on disk 1, got %v", splitAt, postMoveLayout[1])
	}
	if !fileExistsIn(dataDirs[0], collection, volumeID, ".ecx") {
		t.Fatalf("post-move: .ecx must stay on disk 0 to exercise the cross-disk reconcile path")
	}
	if fileExistsIn(dataDirs[1], collection, volumeID, ".ecx") {
		t.Fatalf("post-move: .ecx leaked onto disk 1; reconcile path would not be exercised")
	}

	clusterHarness.RestartVolumeServer()
	conn2, grpcClient2 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn2.Close()

	// VolumeEcShardsInfo only sees one disk's EcVolume; filesystem layout is
	// the ground truth for the whole-store shard count.
	postReconcileLayout := scanShardLayout(t, dataDirs, collection, volumeID)
	if got, want := totalShardsInLayout(postReconcileLayout), erasure_coding.TotalShardsCount; got != want {
		t.Fatalf("post-reconcile: total shards on disk mismatch: got %d, want %d (layout=%v)", got, want, postReconcileLayout)
	}
	if got, want := len(postReconcileLayout[0]), erasure_coding.TotalShardsCount-splitAt; got != want {
		t.Fatalf("post-reconcile: disk 0 shard count drift: got %d, want %d (layout=%v)", got, want, postReconcileLayout)
	}
	if got, want := len(postReconcileLayout[1]), splitAt; got != want {
		t.Fatalf("post-reconcile: disk 1 shard count drift: got %d, want %d (layout=%v)", got, want, postReconcileLayout)
	}
	if _, err := grpcClient2.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: volumeID,
	}); err != nil {
		t.Fatalf("VolumeEcShardsInfo after redistribute restart: %v", err)
	}
	for _, n := range needles {
		verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload, "after-cross-disk-reconcile")
	}

	// blob delete on a cross-disk layout
	deletedNeedle := needles[2]
	deleteReq, err := http.NewRequest(http.MethodDelete, clusterHarness.VolumeAdminURL()+"/"+deletedNeedle.fid, nil)
	if err != nil {
		t.Fatalf("build delete request: %v", err)
	}
	deleteResp := framework.DoRequest(t, httpClient, deleteReq)
	_ = framework.ReadAllAndClose(t, deleteResp)
	if deleteResp.StatusCode != http.StatusAccepted {
		t.Fatalf("HTTP DELETE %s expected 202, got %d", deletedNeedle.fid, deleteResp.StatusCode)
	}
	readResp := framework.ReadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), deletedNeedle.fid)
	_ = framework.ReadAllAndClose(t, readResp)
	if readResp.StatusCode == http.StatusOK {
		t.Fatalf("HTTP GET on deleted EC needle expected non-200, got %d", readResp.StatusCode)
	}
	for _, n := range needles {
		if n.fid == deletedNeedle.fid {
			continue
		}
		verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload, "after-blob-delete")
	}

	// rebuild a shard whose inputs are split across two disks
	clusterHarness.StopVolumeServer()
	const repairTargetShard = 9 // sits on disk 0 after the split
	shardPath := filepath.Join(dataDirs[0], shardFileName(collection, volumeID, repairTargetShard))
	if _, statErr := os.Stat(shardPath); statErr != nil {
		t.Fatalf("repair setup: shard %d not on disk 0 (%s): %v", repairTargetShard, shardPath, statErr)
	}
	if err := os.Remove(shardPath); err != nil {
		t.Fatalf("repair setup: remove shard %d on disk 0: %v", repairTargetShard, err)
	}
	clusterHarness.RestartVolumeServer()
	conn3, grpcClient3 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn3.Close()

	rebuildResp, err := grpcClient3.VolumeEcShardsRebuild(ctx, &volume_server_pb.VolumeEcShardsRebuildRequest{
		VolumeId:   volumeID,
		Collection: collection,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsRebuild after cross-disk shard loss: %v", err)
	}
	if !containsUint32(rebuildResp.GetRebuiltShardIds(), repairTargetShard) {
		t.Fatalf("VolumeEcShardsRebuild expected to rebuild shard %d, got %v",
			repairTargetShard, rebuildResp.GetRebuiltShardIds())
	}
	// The rebuilder picks whichever disk hosts the most shards plus a
	// matching .ecx. After the boot-time mirror runs, every shard-
	// bearing disk owns its own sidecars, so rebuild can legitimately
	// restore the shard on either disk — accept both.
	rebuiltOn := -1
	for i, dir := range dataDirs {
		candidate := filepath.Join(dir, shardFileName(collection, volumeID, repairTargetShard))
		if _, statErr := os.Stat(candidate); statErr == nil {
			rebuiltOn = i
			shardPath = candidate
			break
		}
	}
	if rebuiltOn < 0 {
		t.Fatalf("rebuild did not restore shard %d on any disk (checked %v)", repairTargetShard, dataDirs)
	}

	if _, err := grpcClient3.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   volumeID,
		Collection: collection,
		ShardIds:   []uint32{repairTargetShard},
	}); err != nil {
		t.Fatalf("VolumeEcShardsMount rebuilt shard %d: %v", repairTargetShard, err)
	}
	for _, n := range needles {
		if n.fid == deletedNeedle.fid {
			continue
		}
		verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload, "after-repair")
	}
}

// TestEcPartialShardsOnSiblingDiskCleanedUpOnRestart seeds a healthy .dat on
// disk 0, plants the on-disk footprint of an interrupted EC encode on disk 1
// (one shard plus .ecx / .ecj / .vif, no .dat), and asserts the startup prune
// wipes disk 1 without touching disk 0.
func TestEcPartialShardsOnSiblingDiskCleanedUpOnRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		dataDirCount = 2
		volumeID     = uint32(9501)
		collection   = "ec-9478"
		partialShard = 1
	)

	clusterHarness := framework.StartSingleVolumeClusterWithDataDirs(t, matrix.P1(), dataDirCount)
	dataDirs := clusterHarness.VolumeDataDirs()
	if len(dataDirs) != dataDirCount {
		t.Fatalf("expected %d data dirs, got %d: %v", dataDirCount, len(dataDirs), dataDirs)
	}

	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	framework.AllocateVolume(t, grpcClient, volumeID, collection)

	httpClient := framework.NewHTTPClient()
	needleFid := framework.NewFileID(volumeID, 7777, 0xCAFEBABE)
	needlePayload := bytesOfLen(4096, 0xC7)
	upResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), needleFid, needlePayload)
	_ = framework.ReadAllAndClose(t, upResp)
	if upResp.StatusCode != http.StatusCreated {
		t.Fatalf("seed upload expected 201, got %d", upResp.StatusCode)
	}

	datPath := filepath.Join(dataDirs[0], datFileName(collection, volumeID))
	datInfo, err := os.Stat(datPath)
	if err != nil {
		t.Fatalf("seed setup: .dat must exist on disk 0 (%s): %v", datPath, err)
	}
	// prune's size guard refuses cleanup if the sibling .dat looks truncated
	if datInfo.Size() == 0 {
		t.Fatalf("seed setup: .dat on disk 0 is zero bytes")
	}

	clusterHarness.StopVolumeServer()
	plantPartialEc(t, dataDirs[1], collection, volumeID, partialShard, datInfo.Size())

	preRestartLayout := scanShardLayout(t, dataDirs, collection, volumeID)
	if len(preRestartLayout[0]) != 0 {
		t.Fatalf("seed setup: disk 0 should hold no EC shards before restart, got %v", preRestartLayout[0])
	}
	if len(preRestartLayout[1]) != 1 {
		t.Fatalf("seed setup: disk 1 should hold exactly one planted shard, got %v", preRestartLayout[1])
	}

	clusterHarness.RestartVolumeServer()

	conn2, grpcClient2 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn2.Close()

	// the volume admin port answers before the startup prune finishes
	postPruneLayout := waitForLayout(t, dataDirs, collection, volumeID, func(layout map[int][]int) bool {
		return len(layout[1]) == 0
	}, 10*time.Second)

	if len(postPruneLayout[1]) != 0 {
		t.Fatalf("partial EC shards survived restart on disk 1: %v", postPruneLayout[1])
	}
	// .vif is intentionally left behind: with no .ecx / shard to anchor it,
	// the next startup ignores it.
	for _, side := range []string{".ecx", ".ecj"} {
		if fileExistsIn(dataDirs[1], collection, volumeID, side) {
			t.Fatalf("%s survived prune on disk 1", side)
		}
	}
	if !fileExistsIn(dataDirs[0], collection, volumeID, ".dat") {
		t.Fatalf("prune wiped the healthy .dat on disk 0")
	}
	if !fileExistsIn(dataDirs[0], collection, volumeID, ".idx") {
		t.Fatalf("prune wiped the healthy .idx on disk 0")
	}
	if _, err = grpcClient2.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: volumeID,
	}); err == nil {
		t.Fatalf("VolumeEcShardsInfo returned success after prune; the partial EC mount should be gone")
	}
	verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), needleFid, needlePayload, "after-prune")
}

func bytesOfLen(n int, seed byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(int(seed) + i*31)
	}
	return out
}

func dataShardIds() []uint32 {
	ids := make([]uint32, erasure_coding.DataShardsCount)
	for i := range ids {
		ids[i] = uint32(i)
	}
	return ids
}

func datFileName(collection string, volumeID uint32) string {
	if collection == "" {
		return fmt.Sprintf("%d.dat", volumeID)
	}
	return fmt.Sprintf("%s_%d.dat", collection, volumeID)
}

func shardFileName(collection string, volumeID uint32, shardID int) string {
	if collection == "" {
		return fmt.Sprintf("%d%s", volumeID, erasure_coding.ToExt(shardID))
	}
	return fmt.Sprintf("%s_%d%s", collection, volumeID, erasure_coding.ToExt(shardID))
}

func sideFileName(collection string, volumeID uint32, ext string) string {
	if collection == "" {
		return fmt.Sprintf("%d%s", volumeID, ext)
	}
	return fmt.Sprintf("%s_%d%s", collection, volumeID, ext)
}

func fileExistsIn(dir, collection string, volumeID uint32, ext string) bool {
	_, err := os.Stat(filepath.Join(dir, sideFileName(collection, volumeID, ext)))
	return err == nil
}

func scanShardLayout(t testing.TB, dataDirs []string, collection string, volumeID uint32) map[int][]int {
	t.Helper()
	out := make(map[int][]int, len(dataDirs))
	for diskIdx, dir := range dataDirs {
		out[diskIdx] = nil
		for shard := 0; shard < erasure_coding.TotalShardsCount; shard++ {
			if _, err := os.Stat(filepath.Join(dir, shardFileName(collection, volumeID, shard))); err == nil {
				out[diskIdx] = append(out[diskIdx], shard)
			}
		}
		sort.Ints(out[diskIdx])
	}
	return out
}

func waitForLayout(t testing.TB, dataDirs []string, collection string, volumeID uint32, pred func(map[int][]int) bool, timeout time.Duration) map[int][]int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var layout map[int][]int
	for time.Now().Before(deadline) {
		layout = scanShardLayout(t, dataDirs, collection, volumeID)
		if pred(layout) {
			return layout
		}
		time.Sleep(100 * time.Millisecond)
	}
	return layout
}

func movedFile(t testing.TB, fromDir, toDir, collection string, volumeID uint32, ext string) {
	t.Helper()
	name := sideFileName(collection, volumeID, ext)
	src := filepath.Join(fromDir, name)
	dst := filepath.Join(toDir, name)
	if err := os.Rename(src, dst); err != nil {
		t.Fatalf("move %s → %s: %v", src, dst, err)
	}
}

func totalShardsInLayout(layout map[int][]int) int {
	n := 0
	for _, ids := range layout {
		n += len(ids)
	}
	return n
}

func containsUint32(s []uint32, v uint32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func verifyHTTPRead(t testing.TB, client *http.Client, base, fid string, want []byte, stage string) {
	t.Helper()
	resp := framework.ReadBytes(t, client, base, fid)
	body := framework.ReadAllAndClose(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("[%s] GET %s expected 200, got %d", stage, fid, resp.StatusCode)
	}
	if len(body) != len(want) {
		t.Fatalf("[%s] GET %s size mismatch: got %d, want %d", stage, fid, len(body), len(want))
	}
	for i := range want {
		if body[i] != want[i] {
			t.Fatalf("[%s] GET %s byte %d mismatch: got %d, want %d", stage, fid, i, body[i], want[i])
		}
	}
}

// plantPartialEc writes the footprint of an interrupted EC encode (one shard
// sized for datFileSize, plus .ecx / .ecj / .vif, no .dat) into dir.
func plantPartialEc(t testing.TB, dir, collection string, volumeID uint32, shardID int, datFileSize int64) {
	t.Helper()
	shardPath := filepath.Join(dir, shardFileName(collection, volumeID, shardID))
	if f, err := os.Create(shardPath); err == nil {
		expectedShardSize := plantedShardSize(datFileSize)
		if err := f.Truncate(expectedShardSize); err != nil {
			f.Close()
			t.Fatalf("truncate planted shard: %v", err)
		}
		f.Close()
	} else {
		t.Fatalf("create planted shard %s: %v", shardPath, err)
	}
	for _, side := range []struct {
		ext     string
		content []byte
	}{
		{".ecx", []byte("dummy ecx")},
		{".ecj", nil},
		{".vif", nil},
	} {
		p := filepath.Join(dir, sideFileName(collection, volumeID, side.ext))
		f, err := os.Create(p)
		if err != nil {
			t.Fatalf("create planted %s: %v", side.ext, err)
		}
		if side.content != nil {
			if _, err := f.Write(side.content); err != nil {
				f.Close()
				t.Fatalf("write planted %s: %v", side.ext, err)
			}
		}
		f.Close()
	}
}

// mirrors calculateExpectedShardSize in weed/storage/disk_location_ec.go
func plantedShardSize(datFileSize int64) int64 {
	const dataShards = int64(erasure_coding.DataShardsCount)
	largeBatch := int64(erasure_coding.ErasureCodingLargeBlockSize) * dataShards
	numLarge := datFileSize / largeBatch
	shardSize := numLarge * int64(erasure_coding.ErasureCodingLargeBlockSize)
	remaining := datFileSize - numLarge*largeBatch
	if remaining > 0 {
		smallBatch := int64(erasure_coding.ErasureCodingSmallBlockSize) * dataShards
		numSmall := (remaining + smallBatch - 1) / smallBatch
		shardSize += numSmall * int64(erasure_coding.ErasureCodingSmallBlockSize)
	}
	return shardSize
}
