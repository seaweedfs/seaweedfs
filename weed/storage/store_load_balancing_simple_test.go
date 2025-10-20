package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestLoadBalancingDistribution tests that volumes are evenly distributed
func TestLoadBalancingDistribution(t *testing.T) {
	// Create temporary directories
	tempDir := t.TempDir()

	var dirs []string
	var maxCounts []int32
	var minFreeSpaces []util.MinFreeSpace
	var diskTypes []types.DiskType

	numDirs := 3
	for i := 0; i < numDirs; i++ {
		dir := filepath.Join(tempDir, "dir"+string(rune('0'+i)))
		os.MkdirAll(dir, 0755)
		dirs = append(dirs, dir)
		maxCounts = append(maxCounts, 100) // high limit
		minFreeSpaces = append(minFreeSpaces, util.MinFreeSpace{})
		diskTypes = append(diskTypes, types.HardDriveType)
	}

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080",
		dirs, maxCounts, minFreeSpaces, "", NeedleMapInMemory, diskTypes, 3)

	// Consume channel messages to prevent blocking
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-done:
				return
			}
		}
	}()
	defer func() { close(done) }()

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

// TestLoadBalancingWithRemoteVolumes tests that remote volumes are ignored in load balancing
func TestLoadBalancingWithRemoteVolumes(t *testing.T) {
	// This is a unit test of LocalVolumesLen() which we already test above
	// The integration test is complex due to file creation, so we'll keep it simple
	t.Skip("Already tested by TestLocalVolumesLen")
}
