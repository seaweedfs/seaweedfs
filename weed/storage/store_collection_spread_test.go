package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func addSpreadTestVolume(t *testing.T, store *Store, vid needle.VolumeId, collection string) {
	t.Helper()
	if err := store.AddVolume(vid, collection, NeedleMapInMemory, "000", "",
		0, needle.GetCurrentVersion(), 0, types.HardDriveType, 3); err != nil {
		t.Fatalf("add volume %d: %v", vid, err)
	}
}

func locationOf(store *Store, vid needle.VolumeId) int {
	for i, location := range store.Locations {
		if _, found := location.FindVolume(vid); found {
			return i
		}
	}
	return -1
}

// A new volume goes to the disk with the fewest writable volumes of its own
// collection, even when that disk holds more volumes overall: two writable
// volumes of one collection on a disk split its write bandwidth.
func TestAddVolumeSpreadsCollectionOverDisks(t *testing.T) {
	store := newTestStore(t, 2)

	addSpreadTestVolume(t, store, 1, "other") // disk 0
	addSpreadTestVolume(t, store, 2, "other") // disk 1
	addSpreadTestVolume(t, store, 3, "spread")
	first := locationOf(store, 3)
	// Leave the disk with the "spread" volume holding fewer volumes overall.
	otherOnFirst := needle.VolumeId(1)
	if first == 1 {
		otherOnFirst = 2
	}
	if err := store.DeleteVolume(otherOnFirst, false, false, false); err != nil {
		t.Fatalf("delete volume %d: %v", otherOnFirst, err)
	}

	addSpreadTestVolume(t, store, 4, "spread")
	if got := locationOf(store, 4); got == first {
		t.Fatalf("volume 4 landed on disk %d next to volume 3 of the same collection; the other disk has none", got)
	}
}

// A volume whose .dat reached the size limit takes no more writes even though
// it is not read-only, so it does not count against its disk.
func TestAddVolumeIgnoresFullVolumesOfCollection(t *testing.T) {
	store := newTestStore(t, 2)
	// Every .dat holds at least its superblock, so all volumes are full.
	store.SetVolumeSizeLimit(1)

	addSpreadTestVolume(t, store, 1, "spread")
	fullDisk := locationOf(store, 1)
	addSpreadTestVolume(t, store, 2, "other") // the other disk
	// Both disks hold one volume and no writable "spread" volume.
	addSpreadTestVolume(t, store, 3, "spread")
	if got := locationOf(store, 3); got != fullDisk {
		t.Fatalf("volume 3 went to disk %d; the full volume 1 on disk %d should not count", got, fullDisk)
	}
}

// Read-only volumes no longer take writes, so they do not count against a disk.
func TestAddVolumeIgnoresReadOnlyVolumesOfCollection(t *testing.T) {
	store := newTestStore(t, 2)

	addSpreadTestVolume(t, store, 1, "spread")
	writableDisk := locationOf(store, 1)
	addSpreadTestVolume(t, store, 2, "other") // the other disk
	addSpreadTestVolume(t, store, 3, "spread")
	if got := locationOf(store, 3); got == writableDisk {
		t.Fatalf("volume 3 landed next to volume 1 of the same collection")
	}
	if err := store.MarkVolumeReadonly(3, false, false); err != nil {
		t.Fatalf("mark volume 3 read-only: %v", err)
	}

	// The other disk holds more volumes, but its "spread" volume is read-only.
	addSpreadTestVolume(t, store, 4, "spread")
	if got := locationOf(store, 4); got == writableDisk {
		t.Fatalf("volume 4 landed next to writable volume 1; the read-only volume 3 should not count")
	}
}
