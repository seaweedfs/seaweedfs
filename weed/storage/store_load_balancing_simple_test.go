package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestLoadBalancingDistribution tests that volumes are evenly distributed
func TestLoadBalancingDistribution(t *testing.T) {
	// Create test store with 3 directories
	store := newTestStore(t, 3)

	// Create 9 volumes and verify they're evenly distributed
	volumesToCreate := 9
	for i := 1; i <= volumesToCreate; i++ {
		volumeId := needle.VolumeId(i)

		err := store.AddVolume(volumeId, "", NeedleMapInMemory, "000", "",
			0, needle.GetCurrentVersion(), 0, types.HardDriveType, 3)

		if err != nil {
			t.Fatalf("Failed to add volume %d: %v", volumeId, err)
		}
	}

	// Check distribution - should be 3 volumes per location
	for i, location := range store.Locations {
		localCount := location.LocalVolumesLen()
		if localCount != 3 {
			t.Errorf("Location %d: expected 3 local volumes, got %d", i, localCount)
		}
	}

	// Verify specific distribution pattern
	expected := map[int][]needle.VolumeId{
		0: {1, 4, 7},
		1: {2, 5, 8},
		2: {3, 6, 9},
	}

	for locIdx, expectedVols := range expected {
		location := store.Locations[locIdx]
		for _, vid := range expectedVols {
			if _, found := location.FindVolume(vid); !found {
				t.Errorf("Location %d: expected to find volume %d, but it's not there", locIdx, vid)
			}
		}
	}
}
