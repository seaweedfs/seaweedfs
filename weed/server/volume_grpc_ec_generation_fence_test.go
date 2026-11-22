package weed_server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// buildEcStoreWithGeneration creates a single-disk store holding one EC volume
// whose .vif records the given encode generation, with the given shards mounted.
func buildEcStoreWithGeneration(t *testing.T, dir, collection string, vid needle.VolumeId, encodeTsNs int64, shardIds []erasure_coding.ShardId) *storage.Store {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{100}, []util.MinFreeSpace{{}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewEcShardsChan:
			case <-store.NewVolumesChan:
			case <-store.DeletedVolumesChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})

	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	require.NoError(t, os.WriteFile(base+".ecx", make([]byte, 16), 0o644))
	require.NoError(t, os.WriteFile(base+".ecj", nil, 0o644))
	require.NoError(t, volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.Version3),
		DatFileSize: 10 * 1024 * 1024,
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   10,
			ParityShards: 4,
			EncodeTsNs:   encodeTsNs,
		},
	}))
	for _, sid := range shardIds {
		f, err := os.Create(base + erasure_coding.ToExt(int(sid)))
		require.NoError(t, err)
		require.NoError(t, f.Truncate(1))
		require.NoError(t, f.Close())
	}
	for _, sid := range shardIds {
		require.NoError(t, store.MountEcShards(collection, vid, sid, ""))
	}
	return store
}

func mountedEcShardIds(t *testing.T, vs *VolumeServer, vid needle.VolumeId) map[int]bool {
	t.Helper()
	resp, err := vs.VolumeEcShardsInfo(context.Background(), &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: uint32(vid)})
	require.NoError(t, err)
	ids := make(map[int]bool)
	for _, info := range resp.GetEcShardInfos() {
		ids[int(info.GetShardId())] = true
	}
	return ids
}

// TestFullTeardownFencedByGeneration pins the finding-27 data-safety rule: a
// generation-fenced FullTeardown deletes a disk whose .vif generation is strictly
// older than the request, but preserves a same-or-newer generation, a generation-0
// (recovered/pre-upgrade) volume, and falls back to a blanket wipe for request 0.
func TestFullTeardownFencedByGeneration(t *testing.T) {
	const collection = "ec-fence"
	vid := needle.VolumeId(55)
	shardIds := []erasure_coding.ShardId{0, 1}
	shardExists := func(dir string) bool {
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		return util.FileExists(base + erasure_coding.ToExt(0))
	}
	teardown := func(vs *VolumeServer, reqGen int64) {
		_, err := vs.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:     uint32(vid),
			Collection:   collection,
			FullTeardown: true,
			EncodeTsNs:   reqGen,
		})
		require.NoError(t, err)
	}

	t.Run("older_disk_wiped", func(t *testing.T) {
		dir := t.TempDir()
		vs := &VolumeServer{store: buildEcStoreWithGeneration(t, dir, collection, vid, 100, shardIds)}
		teardown(vs, 200) // request newer than the disk's generation 100
		require.False(t, shardExists(dir), "a strictly-older generation must be wiped")
	})

	t.Run("newer_disk_preserved", func(t *testing.T) {
		dir := t.TempDir()
		vs := &VolumeServer{store: buildEcStoreWithGeneration(t, dir, collection, vid, 200, shardIds)}
		teardown(vs, 100) // request older than the disk's generation 200
		require.True(t, shardExists(dir), "a newer generation (a live newer run) must be preserved")
	})

	t.Run("zero_gen_preserved", func(t *testing.T) {
		dir := t.TempDir()
		vs := &VolumeServer{store: buildEcStoreWithGeneration(t, dir, collection, vid, 0, shardIds)}
		teardown(vs, 200) // a recovered/pre-upgrade live volume reports generation 0
		require.True(t, shardExists(dir), "a generation-0 volume must be preserved under a fenced teardown")
	})

	t.Run("zero_request_blanket_wipe", func(t *testing.T) {
		dir := t.TempDir()
		vs := &VolumeServer{store: buildEcStoreWithGeneration(t, dir, collection, vid, 200, shardIds)}
		teardown(vs, 0) // shell pre-encode / pre-upgrade caller wipes everything
		require.False(t, shardExists(dir), "request generation 0 must blanket-wipe")
	})
}

// TestUnmountEcShardsFencedByGeneration pins that the gen-aware unmount (issued
// before the teardown) preserves a same-or-newer mounted generation, so a stale
// worker cannot unmount a newer run's live shards out from under it.
func TestUnmountEcShardsFencedByGeneration(t *testing.T) {
	const collection = "ec-unmount-fence"
	vid := needle.VolumeId(56)
	dir := t.TempDir()
	store := buildEcStoreWithGeneration(t, dir, collection, vid, 200, []erasure_coding.ShardId{0, 1})
	vs := &VolumeServer{store: store}

	// Request older than the disk generation 200: preserve (skip unmount).
	require.NoError(t, store.UnmountEcShards(vid, 0, 100))
	require.True(t, mountedEcShardIds(t, vs, vid)[0], "a same-or-newer generation shard must stay mounted")

	// Request newer than the disk generation 200: a genuinely-older leftover is unmounted.
	require.NoError(t, store.UnmountEcShards(vid, 1, 300))
	require.False(t, mountedEcShardIds(t, vs, vid)[1], "a strictly-older generation shard must be unmounted")
}

// TestTeardownRemovesStagedGenerations pins that a full teardown wipes the
// 2PC-staged <base>.*.v<N> files together with the canonical ones: they are
// invisible to EC bookkeeping, so anything left behind leaks forever.
func TestTeardownRemovesStagedGenerations(t *testing.T) {
	const collection = "ec-gen-leak"
	vid := needle.VolumeId(57)
	dir := t.TempDir()
	vs := &VolumeServer{store: buildEcStoreWithGeneration(t, dir, collection, vid, 100, []erasure_coding.ShardId{0, 1})}

	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	for _, name := range []string{
		base + ".ec00.v3", base + ".ec01.v3", base + ".ecx.v3", base + ".vif.v3", base + ".ecsum.v3",
	} {
		require.NoError(t, os.WriteFile(name, []byte("staged"), 0o644))
	}

	_, err := vs.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
		VolumeId:     uint32(vid),
		Collection:   collection,
		FullTeardown: true,
		EncodeTsNs:   200,
	})
	require.NoError(t, err)
	left, err := filepath.Glob(base + "*")
	require.NoError(t, err)
	require.Empty(t, left, "teardown must leave no EC files, staged generations included: %v", left)
}

// TestDeleteGenerationsOlderThan covers the post-commit cleanup: staged
// generations below the threshold are removed while the committed generation
// and the live canonical files stay untouched.
func TestDeleteGenerationsOlderThan(t *testing.T) {
	const collection = "ec-gen-gc"
	vid := needle.VolumeId(58)
	dir := t.TempDir()
	vs := &VolumeServer{store: buildEcStoreWithGeneration(t, dir, collection, vid, 100, []erasure_coding.ShardId{0, 1})}

	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	stale := []string{base + ".ec00.v3", base + ".ecx.v3", base + ".vif.v3"}
	fresh := []string{base + ".ec00.v7", base + ".vif.v7"}
	for _, name := range append(stale, fresh...) {
		require.NoError(t, os.WriteFile(name, []byte("staged"), 0o644))
	}

	_, err := vs.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
		VolumeId:                   uint32(vid),
		Collection:                 collection,
		DeleteGenerationsOlderThan: 5,
	})
	require.NoError(t, err)
	for _, name := range stale {
		require.False(t, util.FileExists(name), "%s must be removed", name)
	}
	for _, name := range fresh {
		require.True(t, util.FileExists(name), "%s must be preserved", name)
	}
	require.True(t, util.FileExists(base+".ec00"), "canonical shards must be preserved")
	require.True(t, util.FileExists(base+".ecx"))
	require.True(t, util.FileExists(base+".vif"))
	require.True(t, mountedEcShardIds(t, vs, vid)[0], "mounted shards must stay mounted")
}

// TestShardDeleteRemovesStagedGenerations pins that deleting a shard removes
// its staged generations too: a shard evicted off a disk leaves nothing.
func TestShardDeleteRemovesStagedGenerations(t *testing.T) {
	const collection = "ec-shard-gen"
	vid := needle.VolumeId(59)
	dir := t.TempDir()
	vs := &VolumeServer{store: buildEcStoreWithGeneration(t, dir, collection, vid, 100, []erasure_coding.ShardId{0, 5})}

	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	require.NoError(t, os.WriteFile(base+".ec05.v2", []byte("staged"), 0o644))
	require.NoError(t, vs.store.UnmountEcShards(vid, 5, 0))

	_, err := vs.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
		VolumeId:   uint32(vid),
		Collection: collection,
		ShardIds:   []uint32{5},
	})
	require.NoError(t, err)
	require.False(t, util.FileExists(base+".ec05"))
	require.False(t, util.FileExists(base+".ec05.v2"))
	require.True(t, util.FileExists(base+".ec00"), "sibling shards must be preserved")
	require.True(t, util.FileExists(base+".ecx"), "index must survive while shards remain")
}

// TestReadEcGenerationTsNs covers the per-disk .vif generation read used by the
// fenced teardown: a present .vif yields its generation (or 0 when it has no EC
// config), and a missing .vif is reported unreadable (preserved, fail-safe).
func TestReadEcGenerationTsNs(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "9")

	if _, readable := readEcGenerationTsNs(base, base); readable {
		t.Fatalf("a missing .vif must be reported unreadable")
	}

	require.NoError(t, volume_info.SaveVolumeInfo(base+".vif", &volume_server_pb.VolumeInfo{
		Version:       uint32(needle.Version3),
		EcShardConfig: &volume_server_pb.EcShardConfig{DataShards: 10, ParityShards: 4, EncodeTsNs: 12345},
	}))
	gen, readable := readEcGenerationTsNs(base, base)
	require.True(t, readable)
	require.Equal(t, int64(12345), gen)

	// A .vif with no EC config (recovered / live source volume) reads as generation 0.
	noCfg := filepath.Join(dir, "10")
	require.NoError(t, volume_info.SaveVolumeInfo(noCfg+".vif", &volume_server_pb.VolumeInfo{Version: uint32(needle.Version3)}))
	gen0, readable0 := readEcGenerationTsNs(noCfg, noCfg)
	require.True(t, readable0)
	require.Equal(t, int64(0), gen0)

	// A present-but-unparseable .vif is reported as generation 0 (present), which the
	// fenced teardown still preserves — never wiping on a parse error.
	corrupt := filepath.Join(dir, "11")
	require.NoError(t, os.WriteFile(corrupt+".vif", []byte("not-a-valid-vif"), 0o644))
	genC, readableC := readEcGenerationTsNs(corrupt, corrupt)
	require.True(t, readableC)
	require.Equal(t, int64(0), genC)
}

func TestVolumeEcShardsInfoReportsEncodeRun(t *testing.T) {
	vid := needle.VolumeId(57)
	store := buildEcStoreWithGeneration(t, t.TempDir(), "ec-info-run", vid, 4242, []erasure_coding.ShardId{0})
	vs := &VolumeServer{store: store}
	resp, err := vs.VolumeEcShardsInfo(context.Background(), &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: uint32(vid)})
	require.NoError(t, err)
	require.Equal(t, int64(4242), resp.GetEcShardConfig().GetEncodeTsNs())
}

func TestVolumeEcShardsInfoReportsMixedLocalRuns(t *testing.T) {
	for _, tc := range []struct {
		name    string
		runB    int64
		wantRun int64
	}{
		{"same run on both disks", 1000, 1000},
		{"different runs", 500, erasure_coding.MixedEncodeRuns},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diskA, diskB := t.TempDir(), t.TempDir()
			store := newMultiDiskEcScrubStore(t, diskA, diskB)
			const vid = needle.VolumeId(902)
			seedEcShardOnDisk(t, store, vid, "mixed", diskA, 0, 1000, 10, 4, 0)
			seedEcShardOnDisk(t, store, vid, "mixed", diskB, 1, tc.runB, 10, 4, 0)

			vs := &VolumeServer{store: store}
			resp, err := vs.VolumeEcShardsInfo(context.Background(), &volume_server_pb.VolumeEcShardsInfoRequest{VolumeId: uint32(vid)})
			require.NoError(t, err)
			require.Len(t, resp.GetEcShardInfos(), 2)
			require.Equal(t, tc.wantRun, resp.GetEcShardConfig().GetEncodeTsNs())

			var bits erasure_coding.ShardBits
			for _, s := range resp.GetEcShardInfos() {
				bits = bits.Set(erasure_coding.ShardId(s.ShardId))
			}
			err = erasure_coding.RequireAgreedEncodeRun(uint32(vid), map[string]erasure_coding.ServerShardInventory{
				"local": {Bits: bits, EncodeTsNs: resp.GetEcShardConfig().GetEncodeTsNs()},
				"peer":  {Bits: erasure_coding.ShardBits(0).Set(2), EncodeTsNs: 1000},
			})
			require.Equal(t, tc.wantRun == erasure_coding.MixedEncodeRuns, err != nil, "agreement error: %v", err)
		})
	}
}

func TestVolumeEcShardsCopyRefusesOtherEncodeRun(t *testing.T) {
	const collection = "ec-copy-run"
	vid := needle.VolumeId(58)
	for _, tc := range []struct {
		name    string
		srcRun  int64
		copyVif bool
		wantErr bool
	}{
		{"same run", 1000, true, false},
		{"other run", 2000, true, true},
		{"other run without .vif", 2000, false, true},
		{"unreported run", 0, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := buildEcStoreWithGeneration(t, filepath.Join(t.TempDir(), "src"), collection, vid, tc.srcRun, []erasure_coding.ShardId{0})
			source := serveEcCopySource(t, &VolumeServer{store: src})
			dstDir := filepath.Join(t.TempDir(), "dst")
			receiver := &VolumeServer{
				store:                         buildEcStoreWithGeneration(t, dstDir, collection, vid, 1000, []erasure_coding.ShardId{5}),
				grpcDialOption:                grpc.WithTransportCredentials(insecure.NewCredentials()),
				AllowUntrustedRemoteEndpoints: true,
			}
			before := listDir(t, dstDir)

			_, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(vid),
				Collection:     collection,
				ShardIds:       []uint32{0},
				CopyEcxFile:    true,
				CopyEcjFile:    true,
				CopyVifFile:    tc.copyVif,
				SourceDataNode: source,
			})
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
			require.Equal(t, before, listDir(t, dstDir))
			gen, _ := readEcGenerationTsNs(erasure_coding.EcShardFileName(collection, dstDir, int(vid)), "")
			require.Equal(t, int64(1000), gen)
		})
	}
}

func TestCopyFileRefusesSourceWithMixedLocalRuns(t *testing.T) {
	for _, tc := range []struct {
		name    string
		runB    int64
		wantErr bool
	}{
		{"same run on both disks", 1000, false},
		{"different runs", 500, true},
		{"unreported run", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diskA, diskB := t.TempDir(), t.TempDir()
			src := newMultiDiskEcScrubStore(t, diskA, diskB)
			const vid = needle.VolumeId(903)
			seedEcShardOnDisk(t, src, vid, "mixed-src", diskA, 0, 1000, 10, 4, 0)
			seedEcShardOnDisk(t, src, vid, "mixed-src", diskB, 1, tc.runB, 10, 4, 0)
			source := serveEcCopySource(t, &VolumeServer{store: src})

			dstDir := t.TempDir()
			receiver := newEcCopyReceiver(t, dstDir)
			before := listDir(t, dstDir)
			_, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(vid),
				Collection:     "mixed-src",
				ShardIds:       []uint32{0},
				CopyEcxFile:    true,
				CopyVifFile:    true,
				SourceDataNode: source,
			})
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "holds encode runs")
			require.Equal(t, before, listDir(t, dstDir))
		})
	}
}
