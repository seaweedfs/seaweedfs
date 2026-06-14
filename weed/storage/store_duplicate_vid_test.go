package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/stretchr/testify/require"
)

// A volume id can end up mounted on more than one disk of a server (a stale twin
// re-attached after a disk repair, since NewStore has no cross-disk duplicate
// guard). UnmountVolume must remove EVERY copy, not just the first match, or the
// stale twin survives and re-registers as the volume's content on the next
// heartbeat.
func TestUnmountVolumeRemovesAllDuplicateCopies(t *testing.T) {
	store := newTestStore(t, 2)
	const vid = needle.VolumeId(4242)

	store.Locations[0].SetVolume(vid, createTestVolume(vid, false))
	store.Locations[1].SetVolume(vid, createTestVolume(vid, false))

	require.NoError(t, store.UnmountVolume(vid))

	_, found0 := store.Locations[0].FindVolume(vid)
	_, found1 := store.Locations[1].FindVolume(vid)
	require.False(t, found0, "copy on disk 0 must be unmounted")
	require.False(t, found1, "the stale twin on disk 1 must also be unmounted")
}

// DeleteVolume must likewise destroy every copy of a duplicate volume id, not just
// the first match, so the stale twin cannot survive the delete.
func TestDeleteVolumeRemovesAllDuplicateCopies(t *testing.T) {
	store := newTestStore(t, 2)
	const vid = needle.VolumeId(4243)

	// Real volumes (not stubs) so Destroy can close and unlink them cleanly.
	for _, loc := range store.Locations {
		v, err := NewVolume(loc.Directory, loc.IdxDirectory, "", vid, NeedleMapInMemory,
			&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
		require.NoError(t, err)
		loc.SetVolume(vid, v)
	}

	require.NoError(t, store.DeleteVolume(vid, false, false))

	_, found0 := store.Locations[0].FindVolume(vid)
	_, found1 := store.Locations[1].FindVolume(vid)
	require.False(t, found0, "copy on disk 0 must be deleted")
	require.False(t, found1, "the stale twin on disk 1 must also be deleted")
}
