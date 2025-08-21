package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestHasFreeDiskLocationWithLowDiskSpace(t *testing.T) {
	// Test that hasFreeDiskLocation returns false when disk space is low
	// This addresses the issue where volumes are allocated then immediately deleted
	// when max=0 and minFreeSpace is set

	minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 40, Raw: "40"}

	// Create a disk location with low disk space
	diskLocation := &DiskLocation{
		Directory:              "/test/",
		DirectoryUuid:          "1234",
		IdxDirectory:           "/test/",
		DiskType:               types.HddType,
		MaxVolumeCount:         10,  // Allow 10 volumes
		OriginalMaxVolumeCount: 0,   // max=0 case
		MinFreeSpace:           minFreeSpace,
		isDiskSpaceLow:         true, // Simulate low disk space
	}

	store := &Store{
		Locations: []*DiskLocation{diskLocation},
	}

	// Test: when disk space is low, hasFreeDiskLocation should return false
	// even if volume count is below max
	result := store.hasFreeDiskLocation(diskLocation)
	
	if result {
		t.Errorf("Expected hasFreeDiskLocation to return false when disk space is low, but got true")
	}

	// Test: when disk space is normal, hasFreeDiskLocation should work as before
	diskLocation.isDiskSpaceLow = false
	result = store.hasFreeDiskLocation(diskLocation)
	
	if !result {
		t.Errorf("Expected hasFreeDiskLocation to return true when disk space is normal and volume count is below max, but got false")
	}
}

func TestHasFreeDiskLocationVolumeCount(t *testing.T) {
	// Test the original volume count logic still works
	minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 10, Raw: "10"}

	diskLocation := &DiskLocation{
		Directory:              "/test/",
		DirectoryUuid:          "1234", 
		IdxDirectory:           "/test/",
		DiskType:               types.HddType,
		MaxVolumeCount:         2,  // Allow only 2 volumes
		OriginalMaxVolumeCount: 0,
		MinFreeSpace:           minFreeSpace,
		isDiskSpaceLow:         false, // Normal disk space
	}
	diskLocation.volumes = make(map[needle.VolumeId]*Volume)
	
	store := &Store{
		Locations: []*DiskLocation{diskLocation},
	}

	// Should return true when volume count is below max
	result := store.hasFreeDiskLocation(diskLocation)
	if !result {
		t.Errorf("Expected hasFreeDiskLocation to return true when volume count (0) < max (2), but got false")
	}

	// Add volumes to reach the limit
	// Note: We can't easily create real volumes in a unit test, so we'll mock the VolumesLen
	// For now, we'll test the disk space logic which is the main issue
}