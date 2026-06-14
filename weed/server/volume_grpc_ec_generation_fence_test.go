package weed_server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
}
