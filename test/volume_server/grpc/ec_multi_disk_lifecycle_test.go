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

// TestEcLifecycleAcrossMultipleDisks drives the full EC lifecycle on a volume
// server that owns multiple data directories, mirroring how the production
// loader / reconciler / pruner interact across DiskLocations:
//
//  1. encode    — VolumeEcShardsGenerate writes every .ec?? + .ecx + .ecj +
//     .vif next to the source .dat on disk 0.
//  2. mount + read — VolumeEcShardsMount loads the data shards in memory and
//     HTTP GETs against the volume admin port now serve from the EC path.
//  3. drop .dat — VolumeDelete tears down the regular volume so the EC shards
//     are the only on-disk source of truth, matching the steady state after
//     the shell `ec.encode` pipeline completes its cleanup phase.
//  4. redistribute — between volume-server restarts, half the shard files are
//     moved from disk 0 to disk 1, leaving .ecx / .ecj / .vif on disk 0. On
//     restart the per-disk EC loader sees orphan shards on disk 1, then the
//     Store-level reconcileEcShardsAcrossDisks pass attaches them using
//     disk 0's index files (issue #9212).
//  5. verify    — VolumeEcShardsInfo reports all 14 shards and every needle is
//     still readable through the EC path with the shards spread across disks.
//  6. blob delete — VolumeEcBlobDelete marks one needle deleted; subsequent
//     HTTP GETs from EC return 404, exercising deletion handling on a
//     cross-disk layout.
//  7. repair    — one shard file is physically removed from disk 1 between
//     restarts. After restart VolumeEcShardsRebuild regenerates the missing
//     shard, reading input shards from BOTH disks via the additionalDirs
//     fan-out the multi-disk rebuild path was designed for.
//
// The test cleans nothing up until t.Cleanup runs — assertions report
// per-disk layouts so failures pinpoint which lifecycle step regressed.
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

	// ── step 1: populate the volume ───────────────────────────────────────
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

	// ── step 2: encode ────────────────────────────────────────────────────
	if _, err := grpcClient.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   volumeID,
		Collection: collection,
	}); err != nil {
		t.Fatalf("VolumeEcShardsGenerate: %v", err)
	}

	// After generation every shard file plus .ecx / .ecj / .vif should sit
	// on the disk that owns the .dat (disk 0 for the first allocation).
	postGenerateLayout := scanShardLayout(t, dataDirs, collection, volumeID)
	if len(postGenerateLayout[0]) != erasure_coding.TotalShardsCount {
		t.Fatalf("post-generate: expected all %d shards on disk 0, got per-disk layout %v",
			erasure_coding.TotalShardsCount, postGenerateLayout)
	}
	if len(postGenerateLayout[1]) != 0 {
		t.Fatalf("post-generate: disk 1 should be empty, got %v", postGenerateLayout[1])
	}
	// .ecj is created lazily on first EC delete, so it is not asserted here.
	for _, side := range []string{".ecx", ".vif"} {
		if !fileExistsIn(dataDirs[0], collection, volumeID, side) {
			t.Fatalf("post-generate: expected %s on disk 0", side)
		}
	}

	// ── step 3: mount + verify HTTP reads go through the EC path ─────────
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

	// ── step 4: drop the original .dat so the EC shards are authoritative ─
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

	// ── step 5: redistribute shards 0..6 onto disk 1, restart, reconcile ──
	clusterHarness.StopVolumeServer()

	// keep the upper-half shards on disk 0 (alongside .ecx / .ecj / .vif);
	// move the lower-half shards to disk 1, which becomes the orphan-shard
	// disk that reconcileEcShardsAcrossDisks must pick up on next start.
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

	// Re-dial — the previous gRPC connection is dead after the restart.
	conn2, grpcClient2 := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn2.Close()

	// The filesystem layout is the ground-truth check that the cross-disk
	// load happened correctly: shard files must still be where we put them
	// and no disk should have spuriously deleted any. (VolumeEcShardsInfo
	// only walks one DiskLocation's ecVolumes map, so it cannot be used to
	// count shards across the whole store — issue #9212's point is that the
	// master must aggregate the heartbeats from both per-disk EcVolumes.)
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

	// Each per-disk EcVolume the reconcile path produced should show up
	// via FindEcVolume on its own DiskLocation. We probe disk 1's view
	// indirectly through VolumeEcShardsInfo against the (already loaded)
	// shards on disk 0 — the call must succeed because at least one
	// DiskLocation has an EcVolume for this vid.
	if _, err := grpcClient2.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: volumeID,
	}); err != nil {
		t.Fatalf("VolumeEcShardsInfo after redistribute restart: %v", err)
	}

	// Reads must still work — shards 0..6 are now served by the orphan-disk
	// EcVolume bound to disk 0's .ecx via cross-disk reconcile, while shards
	// 7..13 stay native to disk 0. The volume server's read path consults
	// the master for shard locations and falls back to remote/loopback +
	// parity recovery, so every needle should still come back intact.
	for _, n := range needles {
		verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload, "after-cross-disk-reconcile")
	}

	// ── step 6: blob delete on a cross-disk layout ────────────────────────
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
	// All other needles must still come back intact, confirming deletion
	// did not corrupt the cross-disk index.
	for _, n := range needles {
		if n.fid == deletedNeedle.fid {
			continue
		}
		verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), n.fid, n.payload, "after-blob-delete")
	}

	// ── step 7: repair a shard whose neighbours live on a sibling disk ────
	clusterHarness.StopVolumeServer()
	// shard 9 sits on disk 0 after the split; deleting it forces rebuild
	// to read input shards from BOTH disks (disk 1 holds 0..6, disk 0
	// holds the remaining 7..13).
	const repairTargetShard = 9
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
	if _, statErr := os.Stat(shardPath); statErr != nil {
		t.Fatalf("rebuild did not restore shard %d on disk 0 (%s): %v", repairTargetShard, shardPath, statErr)
	}

	// The remaining (non-deleted) needles must still be readable now that
	// the shard inventory is whole again.
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

// TestEcPartialShardsOnSiblingDiskCleanedUpOnRestart is the end-to-end
// reproducer for issue #9478. The unit test in
// weed/storage/store_ec_hybrid_repro_test.go covers the per-disk loader and
// the Store-level prune call directly. This test drives the same scenario
// through a real volume server restart:
//
//   - encode a regular volume on disk 0 so a healthy .dat / .idx pair lives
//     on disk 0;
//   - stop the volume server, plant the on-disk artefacts of an interrupted
//     EC encode on disk 1 (one shard file plus .ecx / .ecj / .vif, NO .dat
//     next to them);
//   - restart the volume server and verify that the per-disk loader's
//     partial-shard mount is undone by pruneIncompleteEcWithSiblingDat —
//     the partial files on disk 1 are removed, the healthy .dat on disk 0
//     is untouched, and the volume can still serve every needle uploaded
//     before the encode mishap.
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

	// Verify .dat is on disk 0 — the planted partial EC will go on disk 1
	// so the prune path is triggered by a sibling-disk healthy .dat.
	datPath := filepath.Join(dataDirs[0], datFileName(collection, volumeID))
	datInfo, err := os.Stat(datPath)
	if err != nil {
		t.Fatalf("seed setup: .dat must exist on disk 0 (%s): %v", datPath, err)
	}
	if datInfo.Size() == 0 {
		t.Fatalf("seed setup: .dat on disk 0 is zero bytes — the prune safety guard would refuse to cleanup")
	}

	clusterHarness.StopVolumeServer()

	// Plant a partial EC encode on disk 1: one shard (sized as if a real
	// encode had produced it for the actual .dat on disk 0) plus .ecx /
	// .ecj / .vif. No .dat on disk 1, mirroring the issue 9478 layout.
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

	// Give the volume server a beat to finish its startup passes (per-disk
	// loader, prune, cross-disk reconcile). Status responds before all
	// passes finish, so probe with a polled gRPC check.
	postPruneLayout := waitForLayout(t, dataDirs, collection, volumeID, func(layout map[int][]int) bool {
		return len(layout[1]) == 0
	}, 10*time.Second)

	// The partial shard, .ecx and .ecj on disk 1 must be wiped.
	// (.vif is intentionally left behind by removeEcVolumeFiles — it has
	// no index files to anchor it so the next startup simply ignores it,
	// and the existing unit test in store_ec_hybrid_repro_test.go matches
	// that scope.)
	if len(postPruneLayout[1]) != 0 {
		t.Fatalf("issue 9478: partial EC shards survived restart on disk 1: %v", postPruneLayout[1])
	}
	for _, side := range []string{".ecx", ".ecj"} {
		if fileExistsIn(dataDirs[1], collection, volumeID, side) {
			t.Fatalf("issue 9478: %s survived prune on disk 1", side)
		}
	}

	// And the healthy .dat on disk 0 must be untouched.
	if !fileExistsIn(dataDirs[0], collection, volumeID, ".dat") {
		t.Fatalf("issue 9478: prune wiped the healthy .dat on disk 0")
	}
	if !fileExistsIn(dataDirs[0], collection, volumeID, ".idx") {
		t.Fatalf("issue 9478: prune wiped the healthy .idx on disk 0")
	}

	// The volume must not be reported as an EC volume anymore — that was
	// the master-side symptom in issue 9478 (a regular replica and an EC
	// shard set for the same vid heartbeated at the same time). With the
	// prune working, FindEcVolume returns "not found".
	_, err = grpcClient2.VolumeEcShardsInfo(ctx, &volume_server_pb.VolumeEcShardsInfoRequest{
		VolumeId: volumeID,
	})
	if err == nil {
		t.Fatalf("issue 9478: VolumeEcShardsInfo unexpectedly returned success after prune; the partial EC mount should have been undone")
	}

	// And the original needle must still be readable through the regular
	// volume on disk 0.
	verifyHTTPRead(t, httpClient, clusterHarness.VolumeAdminURL(), needleFid, needlePayload, "after-9478-prune")
}

// ── helpers ──────────────────────────────────────────────────────────────

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

// scanShardLayout returns a per-disk map of shard IDs found on each data dir.
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

// movedFile relocates one file between two data directories so the next
// volume-server startup observes the new layout.
func movedFile(t testing.TB, fromDir, toDir, collection string, volumeID uint32, ext string) {
	t.Helper()
	name := sideFileName(collection, volumeID, ext)
	src := filepath.Join(fromDir, name)
	dst := filepath.Join(toDir, name)
	if err := os.Rename(src, dst); err != nil {
		t.Fatalf("move %s → %s: %v", src, dst, err)
	}
}

func shardIDsFromInfo(resp *volume_server_pb.VolumeEcShardsInfoResponse) map[uint32]struct{} {
	out := make(map[uint32]struct{})
	for _, s := range resp.GetEcShardInfos() {
		out[s.GetShardId()] = struct{}{}
	}
	return out
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

// plantPartialEc creates the file footprint of an interrupted EC encode in
// dir — exactly one shard file (sized to match the planted datFileSize so
// pruneIncompleteEcWithSiblingDat's size guard treats the sibling .dat as
// credible) plus the .ecx / .ecj / .vif side files. No .dat is written here,
// which is what makes the layout look like a distributed EC volume to the
// per-disk loader. Mirrors the layout produced by the unit test in
// weed/storage/store_ec_hybrid_repro_test.go.
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

// plantedShardSize duplicates the small subset of calculateExpectedShardSize
// (weed/storage/disk_location_ec.go) the planter needs, using the public
// constants from erasure_coding so we do not reach into unexported package
// internals from this integration test.
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
