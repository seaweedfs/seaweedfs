package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestHasFreeDiskLocation(t *testing.T) {
	testCases := []struct {
		name           string
		isDiskSpaceLow bool
		maxVolumeCount int32
		currentVolumes int
		expected       bool
	}{
		{
			name:           "low disk space prevents allocation",
			isDiskSpaceLow: true,
			maxVolumeCount: 10,
			currentVolumes: 5,
			expected:       false,
		},
		{
			name:           "normal disk space and available volume count allows allocation",
			isDiskSpaceLow: false,
			maxVolumeCount: 10,
			currentVolumes: 5,
			expected:       true,
		},
		{
			name:           "volume count at max prevents allocation",
			isDiskSpaceLow: false,
			maxVolumeCount: 2,
			currentVolumes: 2,
			expected:       false,
		},
		{
			name:           "volume count over max prevents allocation",
			isDiskSpaceLow: false,
			maxVolumeCount: 2,
			currentVolumes: 3,
			expected:       false,
		},
		{
			name:           "volume count just under max allows allocation",
			isDiskSpaceLow: false,
			maxVolumeCount: 2,
			currentVolumes: 1,
			expected:       true,
		},
		{
			name:           "max volume count is 0 allows allocation",
			isDiskSpaceLow: false,
			maxVolumeCount: 0,
			currentVolumes: 100,
			expected:       true,
		},
		{
			name:           "max volume count is 0 but low disk space prevents allocation",
			isDiskSpaceLow: true,
			maxVolumeCount: 0,
			currentVolumes: 100,
			expected:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// setup
			diskLocation := &DiskLocation{
				volumes:        make(map[needle.VolumeId]*Volume),
				isDiskSpaceLow: tc.isDiskSpaceLow,
				MaxVolumeCount: tc.maxVolumeCount,
			}
			for i := 0; i < tc.currentVolumes; i++ {
				diskLocation.volumes[needle.VolumeId(i+1)] = &Volume{}
			}

			store := &Store{
				Locations: []*DiskLocation{diskLocation},
			}

			// act
			result := store.hasFreeDiskLocation(diskLocation)

			// assert
			if result != tc.expected {
				t.Errorf("Expected hasFreeDiskLocation() = %v; want %v for volumes:%d/%d, lowSpace:%v",
					result, tc.expected, len(diskLocation.volumes), diskLocation.MaxVolumeCount, diskLocation.isDiskSpaceLow)
			}
		})
	}
}

func TestCollectHeartbeatRespectsLowDiskSpace(t *testing.T) {
	diskType := types.ToDiskType("hdd")
	location := &DiskLocation{
		volumes:        make(map[needle.VolumeId]*Volume),
		isDiskSpaceLow: true,
		MaxVolumeCount: 10,
		DiskType:       diskType,
	}
	location.volumes[needle.VolumeId(1)] = &Volume{}
	location.volumes[needle.VolumeId(2)] = &Volume{}
	location.volumes[needle.VolumeId(3)] = &Volume{}

	store := &Store{
		Locations: []*DiskLocation{location},
	}

	hb := store.CollectHeartbeat()
	if got := hb.MaxVolumeCounts[string(diskType)]; got != 3 {
		t.Fatalf("expected low disk space to cap max volume count to used slots, got %d", got)
	}

	location.isDiskSpaceLow = false
	hb = store.CollectHeartbeat()
	if got := hb.MaxVolumeCounts[string(diskType)]; got != 10 {
		t.Fatalf("expected normal disk space to report configured max volume count, got %d", got)
	}
}
