package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

func TestRenameOrCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "a.idx")
	dst := filepath.Join(dir, "sub", "a.idx")
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
	content := []byte("index-bytes")
	require.NoError(t, os.WriteFile(src, content, 0o644))

	require.NoError(t, RenameOrCopyFile(src, dst))

	_, err := os.Stat(src)
	require.True(t, os.IsNotExist(err), "source should be gone after the move")
	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, content, got, "content must survive the move")
}

// newIdxSplitStore builds a single-disk store whose index directory differs
// from its data directory (-dir.idx), draining every notify channel so mount
// and unmount never block.
func newIdxSplitStore(t *testing.T, dataDir, idxDir string) *Store {
	t.Helper()
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(idxDir, 0o755))
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dataDir}, []int32{100}, []util.MinFreeSpace{{}}, idxDir,
		NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3,
		stats.DefaultDiskIOProbeConfig())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-store.DeletedVolumesChan:
			case <-store.NewEcShardsChan:
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
	return store
}

// TestConsolidateVolumeIndexMovesIdxToIdxDir pins the relocate a decode runs
// once the EC shards are gone: an index co-located with the data (where the
// reconstruct left it) is moved back to the -dir.idx directory, and the volume
// stays mounted.
func TestConsolidateVolumeIndexMovesIdxToIdxDir(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	idxDir := filepath.Join(root, "idx")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(idxDir, 0o755))
	const vid = needle.VolumeId(7)

	// Create the volume with its index co-located in the data dir (the state a
	// reconstruct leaves), then close it so the store mounts it fresh.
	v, err := NewVolume(dataDir, dataDir, "", vid, NeedleMapInMemory,
		&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	v.Close()

	dataIdx := filepath.Join(dataDir, "7.idx")
	idxDirIdx := filepath.Join(idxDir, "7.idx")
	require.FileExists(t, dataIdx, "precondition: index co-located with the data")

	store := newIdxSplitStore(t, dataDir, idxDir)
	require.NoError(t, store.MountVolume(vid))

	require.NoError(t, store.ConsolidateVolumeIndex(vid))

	require.FileExists(t, idxDirIdx, "index should have moved to the idx dir")
	_, err = os.Stat(dataIdx)
	require.True(t, os.IsNotExist(err), "index should be gone from the data dir")
	require.NotNil(t, store.findVolume(vid), "volume should stay mounted after consolidation")
}

// TestConsolidateVolumeIndexNoopWithoutIdxDir pins that the relocate is a no-op
// when no separate index directory is configured.
func TestConsolidateVolumeIndexNoopWithoutIdxDir(t *testing.T) {
	dataDir := t.TempDir()
	const vid = needle.VolumeId(8)
	v, err := NewVolume(dataDir, dataDir, "", vid, NeedleMapInMemory,
		&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	v.Close()

	store := newIdxSplitStore(t, dataDir, dataDir) // idx dir == data dir
	require.NoError(t, store.MountVolume(vid))

	require.NoError(t, store.ConsolidateVolumeIndex(vid))
	require.FileExists(t, filepath.Join(dataDir, "8.idx"), "index stays put without -dir.idx")
	require.NotNil(t, store.findVolume(vid), "volume stays mounted")
}
