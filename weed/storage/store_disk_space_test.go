package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
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

func newTestLocation(maxCount int32, isDiskLow bool, volCount int) *DiskLocation {
	location := &DiskLocation{
		volumes:        make(map[needle.VolumeId]*Volume),
		ecVolumes:      make(map[needle.VolumeId]*erasure_coding.EcVolume),
		MaxVolumeCount: maxCount,
		DiskType:       types.ToDiskType("hdd"),
		isDiskSpaceLow: isDiskLow,
	}
	for i := 1; i <= volCount; i++ {
		location.volumes[needle.VolumeId(i)] = &Volume{}
	}
	return location
}

func TestCollectHeartbeatRespectsLowDiskSpace(t *testing.T) {
	diskType := types.ToDiskType("hdd")

	t.Run("low disk space", func(t *testing.T) {
		location := newTestLocation(10, true, 3)
		store := &Store{Locations: []*DiskLocation{location}}

		hb := store.CollectHeartbeat()
		if got := hb.MaxVolumeCounts[string(diskType)]; got != 3 {
			t.Errorf("expected low disk space to cap max volume count to used slots, got %d", got)
		}
	})

	t.Run("normal disk space", func(t *testing.T) {
		location := newTestLocation(10, false, 3)
		store := &Store{Locations: []*DiskLocation{location}}

		hb := store.CollectHeartbeat()
		if got := hb.MaxVolumeCounts[string(diskType)]; got != 10 {
			t.Errorf("expected normal disk space to report configured max volume count, got %d", got)
		}
	})

	t.Run("low disk space zero volumes", func(t *testing.T) {
		location := newTestLocation(10, true, 0)
		store := &Store{Locations: []*DiskLocation{location}}

		hb := store.CollectHeartbeat()
		if got := hb.MaxVolumeCounts[string(diskType)]; got != 0 {
			t.Errorf("expected zero volumes to report zero capacity, got %d", got)
		}
	})

	t.Run("low disk space with ec shards", func(t *testing.T) {
		location := newTestLocation(10, true, 3)

		ecVolume := &erasure_coding.EcVolume{VolumeId: 1}
		const shardCount = 15
		for i := 0; i < shardCount; i++ {
			ecVolume.Shards = append(ecVolume.Shards, &erasure_coding.EcVolumeShard{
				ShardId: erasure_coding.ShardId(i),
			})
		}
		location.ecVolumes[ecVolume.VolumeId] = ecVolume
		store := &Store{Locations: []*DiskLocation{location}}

		hb := store.CollectHeartbeat()
		expectedSlots := len(location.volumes) + (shardCount+erasure_coding.DataShardsCount-1)/erasure_coding.DataShardsCount
		if got := hb.MaxVolumeCounts[string(diskType)]; got != uint32(expectedSlots) {
			t.Errorf("expected low disk space to include ec shard contribution, got %d want %d", got, expectedSlots)
		}
	})

	t.Run("low disk space with multiple ec volumes", func(t *testing.T) {
		location := newTestLocation(10, true, 2)

		totalShardCount := 0

		addEcVolume := func(vid needle.VolumeId, shardCount int) {
			ecVolume := &erasure_coding.EcVolume{VolumeId: vid}
			for i := 0; i < shardCount; i++ {
				ecVolume.Shards = append(ecVolume.Shards, &erasure_coding.EcVolumeShard{
					ShardId: erasure_coding.ShardId(i),
				})
			}
			location.ecVolumes[vid] = ecVolume
			totalShardCount += shardCount
		}

		addEcVolume(1, 12)
		addEcVolume(2, 6)

		store := &Store{Locations: []*DiskLocation{location}}

		hb := store.CollectHeartbeat()
		expectedSlots := len(location.volumes)
		expectedSlots += (totalShardCount + erasure_coding.DataShardsCount - 1) / erasure_coding.DataShardsCount

		if got := hb.MaxVolumeCounts[string(diskType)]; got != uint32(expectedSlots) {
			t.Errorf("expected multiple ec volumes to be counted, got %d want %d", got, expectedSlots)
		}
	})
}
