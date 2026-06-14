package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
