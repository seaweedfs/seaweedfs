package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestHasFreeDiskLocation(t *testing.T) {
	diskLocation := &DiskLocation{
		Directory:              "/test/",
		DirectoryUuid:          "1234",
		IdxDirectory:           "/test/",
		DiskType:               types.HddType,
		OriginalMaxVolumeCount: 0,
		volumes:                make(map[needle.VolumeId]*Volume),
	}
	store := &Store{
		Locations: []*DiskLocation{diskLocation},
	}

	t.Run("low disk space prevents allocation", func(t *testing.T) {
		// setup
		diskLocation.isDiskSpaceLow = true
		diskLocation.MaxVolumeCount = 10
		diskLocation.MinFreeSpace = util.MinFreeSpace{Type: util.AsPercent, Percent: 40, Raw: "40"}
		defer func() { diskLocation.isDiskSpaceLow = false }() // teardown

		// act
		result := store.hasFreeDiskLocation(diskLocation)

		// assert
		if result {
			t.Errorf("Expected hasFreeDiskLocation to return false when disk space is low, but got true")
		}
	})

	t.Run("normal disk space and available volume count allows allocation", func(t *testing.T) {
		// setup
		diskLocation.isDiskSpaceLow = false
		diskLocation.MaxVolumeCount = 10
		diskLocation.MinFreeSpace = util.MinFreeSpace{Type: util.AsPercent, Percent: 40, Raw: "40"}
		diskLocation.volumes = make(map[needle.VolumeId]*Volume)

		// act
		result := store.hasFreeDiskLocation(diskLocation)

		// assert
		if !result {
			t.Errorf("Expected hasFreeDiskLocation to return true when disk space is normal and volume count is below max, but got false")
		}
	})

	t.Run("volume count at max prevents allocation", func(t *testing.T) {
		// setup
		diskLocation.isDiskSpaceLow = false
		diskLocation.MaxVolumeCount = 2
		diskLocation.MinFreeSpace = util.MinFreeSpace{Type: util.AsPercent, Percent: 10, Raw: "10"}
		diskLocation.volumes = make(map[needle.VolumeId]*Volume)
		diskLocation.volumes[1] = &Volume{}
		diskLocation.volumes[2] = &Volume{}
		defer func() { diskLocation.volumes = make(map[needle.VolumeId]*Volume) }() // teardown

		// act
		result := store.hasFreeDiskLocation(diskLocation)

		// assert
		if result {
			t.Errorf("Expected hasFreeDiskLocation to return false when volume count (%d) == max (%d), but got true", len(diskLocation.volumes), diskLocation.MaxVolumeCount)
		}
	})
}
