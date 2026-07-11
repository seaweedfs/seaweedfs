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
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// The non-teardown EC shard delete must not remove the shared .ecx/.ecj index while
// a sibling disk still holds shards of the same volume (split-disk layout) — doing so
// orphans those shards. The shared index is removed only once no shard remains across
// any disk of the node.
func TestEcShardDeleteKeepsSharedIndexWhileSiblingHasShards(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	for _, d := range []string{dir0, dir1} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	const collection = "ec-shared-index"
	vid := needle.VolumeId(77)

	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1}, []int32{100, 100}, []util.MinFreeSpace{{}, {}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType, types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
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

	base0 := erasure_coding.EcShardFileName(collection, dir0, int(vid))
	// Shared index lives on disk0.
	require.NoError(t, os.WriteFile(base0+".ecx", make([]byte, 16), 0o644))
	require.NoError(t, os.WriteFile(base0+".ecj", nil, 0o644))
	require.NoError(t, os.WriteFile(base0+".vif", []byte("x"), 0o644))
	plant := func(dir string, ids ...int) {
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		for _, id := range ids {
			require.NoError(t, os.WriteFile(base+erasure_coding.ToExt(id), []byte("s"), 0o644))
		}
	}
	plant(dir0, 0, 5)
	plant(dir1, 7, 12)

	vs := &VolumeServer{store: store}
	del := func(ids ...uint32) {
		_, err := vs.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
			ShardIds:   ids,
		})
		require.NoError(t, err)
	}

	// Delete disk0's shards; disk1 still holds 7 and 12, so the shared index stays.
	del(0, 5)
	require.False(t, util.FileExists(base0+erasure_coding.ToExt(0)), "deleted shard file should be gone")
	require.True(t, util.FileExists(base0+".ecx"), "shared .ecx must be preserved while a sibling disk holds shards")

	// Delete disk1's shards; no shard remains node-wide, so the shared index is removed.
	del(7, 12)
	require.False(t, util.FileExists(base0+".ecx"), "shared .ecx must be removed once no shard remains node-wide")
}

// Same protection for the idx-base bitrot sidecar: with one -dir.idx shared by
// every disk, emptying one disk must not sweep <idx>/<volume>.ecsum while a
// sibling disk still holds shards; it goes when the last sibling is emptied.
func TestEcShardDeleteKeepsSharedIdxSidecarWhileSiblingHasShards(t *testing.T) {
	tempDir := t.TempDir()
	dir0 := filepath.Join(tempDir, "disk0")
	dir1 := filepath.Join(tempDir, "disk1")
	idxDir := filepath.Join(tempDir, "idx")
	for _, d := range []string{dir0, dir1, idxDir} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	const collection = "ec-shared-idx-sidecar"
	vid := needle.VolumeId(78)

	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir0, dir1}, []int32{100, 100}, []util.MinFreeSpace{{}, {}}, idxDir,
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType, types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
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

	plant := func(dir string, ids ...int) {
		base := erasure_coding.EcShardFileName(collection, dir, int(vid))
		for _, id := range ids {
			require.NoError(t, os.WriteFile(base+erasure_coding.ToExt(id), []byte("s"), 0o644))
		}
	}
	plant(dir0, 0)
	plant(dir1, 7)
	idxSidecar := erasure_coding.EcShardFileName(collection, idxDir, int(vid)) + erasure_coding.BitrotSidecarExt
	require.NoError(t, os.WriteFile(idxSidecar, []byte("x"), 0o644))

	vs := &VolumeServer{store: store}
	del := func(ids ...uint32) {
		_, err := vs.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
			ShardIds:   ids,
		})
		require.NoError(t, err)
	}

	del(0)
	require.True(t, util.FileExists(idxSidecar), "shared idx sidecar must survive while a sibling disk holds shards")

	del(7)
	require.False(t, util.FileExists(idxSidecar), "shared idx sidecar must be removed once no shard remains node-wide")
}
