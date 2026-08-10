package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newSingleDirStore builds a single-disk store over dir, draining every notify
// channel so mount and unmount never block.
func newSingleDirStore(t *testing.T, dir string) *Store {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{100}, []util.MinFreeSpace{{}}, "",
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
		close(done)
	})
	return store
}

// canDelete rejects writes, keeps accepting deletes, and survives a restart.
func TestMarkVolumeReadonlyCanDelete(t *testing.T) {
	dir := t.TempDir()
	store := newSingleDirStore(t, dir)
	const vid = needle.VolumeId(11)
	require.NoError(t, store.AddVolume(vid, "", NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0))

	n1, n2 := newRandomNeedle(1), newRandomNeedle(2)
	_, err := store.WriteVolumeNeedle(vid, n1, true, false)
	require.NoError(t, err)
	_, err = store.WriteVolumeNeedle(vid, n2, true, false)
	require.NoError(t, err)

	require.NoError(t, store.MarkVolumeReadonly(vid, true, true))
	v := store.GetVolume(vid)
	require.True(t, v.noWriteCanDelete)
	require.False(t, v.noWriteOrDelete)
	require.True(t, v.IsReadOnly())

	_, err = store.WriteVolumeNeedle(vid, newRandomNeedle(3), true, false)
	require.Error(t, err, "writes must be rejected")
	_, err = store.DeleteVolumeNeedle(vid, n1)
	require.NoError(t, err, "deletes must still land")

	store.Close()
	store2 := newSingleDirStore(t, dir)
	v2 := store2.GetVolume(vid)
	require.NotNil(t, v2)
	require.True(t, v2.noWriteCanDelete)
	require.False(t, v2.noWriteOrDelete)

	_, err = store2.WriteVolumeNeedle(vid, newRandomNeedle(4), true, false)
	require.Error(t, err, "writes must stay rejected after restart")
	_, err = store2.DeleteVolumeNeedle(vid, n2)
	require.NoError(t, err, "deletes must still land after restart")

	// Downgrading to plain readonly clears the canDelete flag.
	require.NoError(t, store2.MarkVolumeReadonly(vid, false, true))
	require.True(t, v2.noWriteOrDelete)
	require.False(t, v2.noWriteCanDelete)
	_, err = store2.DeleteVolumeNeedle(vid, n2)
	require.Error(t, err, "plain readonly must reject deletes")

	// -writable clears the state again.
	require.NoError(t, store2.MarkVolumeWritable(vid))
	require.False(t, v2.IsReadOnly())
	_, err = store2.WriteVolumeNeedle(vid, newRandomNeedle(5), true, false)
	require.NoError(t, err)
	store2.Close()
}

// Upgrading a volume that booted plain persisted-readonly to canDelete must
// reopen the O_RDONLY .idx for writing, or delete tombstones cannot append.
func TestMarkVolumeReadonlyCanDelete_AfterReadonlyBoot(t *testing.T) {
	dir := t.TempDir()
	store := newSingleDirStore(t, dir)
	const vid = needle.VolumeId(12)
	require.NoError(t, store.AddVolume(vid, "", NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0))

	n := newRandomNeedle(1)
	_, err := store.WriteVolumeNeedle(vid, n, true, false)
	require.NoError(t, err)
	require.NoError(t, store.MarkVolumeReadonly(vid, false, true))
	store.Close()

	store2 := newSingleDirStore(t, dir)
	v := store2.GetVolume(vid)
	require.NotNil(t, v)
	require.True(t, v.noWriteOrDelete)
	_, err = store2.DeleteVolumeNeedle(vid, n)
	require.Error(t, err, "plain readonly must reject deletes")

	require.NoError(t, store2.MarkVolumeReadonly(vid, true, true))
	require.False(t, v.noWriteOrDelete)
	require.True(t, v.noWriteCanDelete)
	_, err = store2.DeleteVolumeNeedle(vid, n)
	require.NoError(t, err, "deletes must land once canDelete is set")
	store2.Close()
}
