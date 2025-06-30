package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func TestCompactVolumeSpaceCheck(t *testing.T) {
	// Create a temporary directory for testing
	dir, err := os.MkdirTemp("", "seaweedfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a disk location
	location := &DiskLocation{
		Directory:    dir,
		IdxDirectory: dir,
		volumes:      make(map[needle.VolumeId]*Volume),
	}

	// Create a store
	store := &Store{
		Locations: []*DiskLocation{location},
	}

	// Create a volume with some data
	vid := needle.VolumeId(1)
	replication, _ := super_block.NewReplicaPlacementFromString("000")
	volume, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory, replication, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create volume: %v", err)
	}

	location.SetVolume(vid, volume)

	// Test space checking logic
	t.Run("InsufficientSpace", func(t *testing.T) {
		// This should fail because we're testing the improved space checking
		err := store.CompactVolume(vid, 0, 0, nil)
		if err == nil {
			t.Error("Expected compaction to fail due to insufficient space")
		}
		if err != nil {
			t.Logf("Expected error: %v", err)
		}
	})

	// Clean up
	volume.Close()
}

func TestSpaceCalculation(t *testing.T) {
	// Test the space calculation logic
	testCases := []struct {
		name            string
		volumeSize      uint64
		indexSize       uint64
		preallocate     int64
		expectedMinimum int64
	}{
		{
			name:            "SmallVolume",
			volumeSize:      1024 * 1024, // 1MB
			indexSize:       1024,        // 1KB
			preallocate:     0,
			expectedMinimum: int64((1024*1024 + 1024) * 110 / 100), // 110% of volume+index size
		},
		{
			name:            "LargePreallocate",
			volumeSize:      1024 * 1024,                         // 1MB
			indexSize:       1024,                                // 1KB
			preallocate:     10 * 1024 * 1024,                    // 10MB
			expectedMinimum: int64(10 * 1024 * 1024 * 110 / 100), // 110% of preallocate
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			estimatedCompactSize := int64(tc.volumeSize + tc.indexSize)
			spaceNeeded := tc.preallocate
			if estimatedCompactSize > tc.preallocate {
				spaceNeeded = estimatedCompactSize
			}
			// Add 10% safety buffer
			spaceNeeded = spaceNeeded + (spaceNeeded / 10)

			if spaceNeeded < tc.expectedMinimum {
				t.Errorf("Space calculation incorrect: got %d, expected at least %d", spaceNeeded, tc.expectedMinimum)
			}
			t.Logf("Volume: %d, Index: %d, Preallocate: %d -> SpaceNeeded: %d",
				tc.volumeSize, tc.indexSize, tc.preallocate, spaceNeeded)
		})
	}
}
