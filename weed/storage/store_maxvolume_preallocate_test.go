package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// A volume that was not preallocated keeps its remaining room reserved when
// the master turns preallocation on, so it still takes a slot of the free space.
func TestMaybeAdjustVolumeMaxReservesPlainVolumesWhilePreallocating(t *testing.T) {
	dir := t.TempDir()

	// Room for 2.5 volumes: the plain volume claims one, leaving one more slot.
	volumeSizeLimit := uint64(float64(stats.NewDiskStatus(dir).Free) / 2.5)
	if volumeSizeLimit == 0 {
		t.Skip("cannot determine free disk space")
	}

	loc := &DiskLocation{Directory: dir, IdxDirectory: dir}
	loc.volumes = make(map[needle.VolumeId]*Volume)
	loc.SetVolume(1, &Volume{Id: 1, dir: dir, dirIdx: dir, DataBackend: &mockBackendStorageFile{}, nm: &mockNeedleMapper{}})
	s := &Store{Locations: []*DiskLocation{loc}}
	s.SetVolumeSizeLimit(volumeSizeLimit)
	s.SetPreallocate(true)

	s.MaybeAdjustVolumeMax()

	if loc.MaxVolumeCount != 2 {
		t.Errorf("MaxVolumeCount=%d; want 2", loc.MaxVolumeCount)
	}
}
