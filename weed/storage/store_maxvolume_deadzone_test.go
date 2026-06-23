package storage

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// With auto max (OriginalMaxVolumeCount == 0) and a large volumeSizeLimit,
// MaybeAdjustVolumeMax must not compute a maxVolumeCount of 0 when the free
// disk holds room for at least one volume. It once did so for disks sized
// between 1x and 2x the limit, leaving no writable volume and timing out every
// assign. An auto-sized disk with space always hosts at least one volume.
func TestMaybeAdjustVolumeMaxNoDeadZone(t *testing.T) {
	dir := t.TempDir()

	free := stats.NewDiskStatus(dir).Free
	if free < 4 {
		t.Skip("cannot determine free disk space")
	}

	// Drive the limit so the disk's free space is a known multiple of it,
	// then assert the slot count is the honest "how many full volumes fit",
	// floored at one. The 1.5x case is the exact regression from #9753.
	for _, ratio := range []float64{0.5, 1.5, 2.5, 3.5} {
		t.Run(fmt.Sprintf("ratio=%.1f", ratio), func(t *testing.T) {
			free := stats.NewDiskStatus(dir).Free
			volumeSizeLimit := uint64(float64(free) / ratio)
			if volumeSizeLimit == 0 {
				t.Skip("free disk too small for this ratio")
			}

			loc := NewDiskLocation(dir, 0 /* auto */, util.MinFreeSpace{}, "", types.HddType, nil, stats.DefaultDiskIOProbeConfig())
			defer loc.Close()
			s := &Store{Locations: []*DiskLocation{loc}}
			s.SetVolumeSizeLimit(volumeSizeLimit)

			s.MaybeAdjustVolumeMax()

			// Empty temp dir: no volumes, no EC shards, so the count is purely
			// the number of full volumes that fit, never below one.
			want := int32(free / volumeSizeLimit)
			if want < 1 {
				want = 1
			}
			t.Logf("free=%.1fGiB limit=%.1fGiB ratio≈%.2f -> MaxVolumeCount=%d (want %d)",
				float64(free)/(1<<30), float64(volumeSizeLimit)/(1<<30),
				float64(free)/float64(volumeSizeLimit), loc.MaxVolumeCount, want)

			if loc.MaxVolumeCount < 1 {
				t.Errorf("auto-sized disk with free space reported MaxVolumeCount=%d; want >= 1", loc.MaxVolumeCount)
			}
			if loc.MaxVolumeCount != want {
				t.Errorf("MaxVolumeCount=%d; want %d", loc.MaxVolumeCount, want)
			}
		})
	}
}
