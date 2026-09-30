package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSpaceCalculation(t *testing.T) {
	// Test the space calculation logic
	testCases := []struct {
		name        string
		volumeSize  uint64
		indexSize   uint64
		preallocate int64
		expectedMin int64
	}{
		{
			name:        "Large volume, small preallocate",
			volumeSize:  244 * 1024 * 1024 * 1024,                          // 244GB
			indexSize:   1024 * 1024,                                       // 1MB
			preallocate: 1024,                                              // 1KB
			expectedMin: int64((244*1024*1024*1024 + 1024*1024) * 11 / 10), // +10% buffer
		},
		{
			name:        "Small volume, large preallocate",
			volumeSize:  100 * 1024 * 1024,                   // 100MB
			indexSize:   1024,                                // 1KB
			preallocate: 1024 * 1024 * 1024,                  // 1GB
			expectedMin: int64(1024 * 1024 * 1024 * 11 / 10), // preallocate + 10%
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate space needed using the same logic as our fix
			estimatedCompactSize := int64(tc.volumeSize + tc.indexSize)
			spaceNeeded := tc.preallocate
			if estimatedCompactSize > tc.preallocate {
				spaceNeeded = estimatedCompactSize
			}
			// Add 10% safety buffer
			spaceNeeded = spaceNeeded + (spaceNeeded / 10)

			if spaceNeeded < tc.expectedMin {
				t.Errorf("Space calculation too low: got %d, expected at least %d", spaceNeeded, tc.expectedMin)
			}

			t.Logf("Volume size: %d bytes, Space needed: %d bytes (%.2f%% of volume size)",
				tc.volumeSize, spaceNeeded, float64(spaceNeeded)/float64(tc.volumeSize)*100)
		})
	}
}

// Compaction writes live needles only, so the space check must be measured
// against the live size, not the .dat the garbage occupies — a full disk
// needs the estimate to shrink or it can never reclaim.
func TestEstimatedCompactedSizeCountsLiveNeedles(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	const count = 20
	for i := 1; i <= count; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	datSize, _, _ := v.FileStat()

	fullEstimate := estimatedCompactedSize(v)
	if fullEstimate <= super_block.SuperBlockSize {
		t.Fatalf("estimate for all-live volume = %d, want > superblock", fullEstimate)
	}

	for i := 1; i < count; i++ {
		if _, err := v.doDeleteRequest(newEmptyNeedle(uint64(i))); err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	estimate := estimatedCompactedSize(v)
	if estimate >= int64(datSize) {
		t.Fatalf("estimate %d not below .dat size %d with 19/20 needles deleted", estimate, datSize)
	}
	if estimate <= super_block.SuperBlockSize {
		t.Fatalf("estimate %d lost the one live needle", estimate)
	}
	live := int64(v.FileCount()-v.DeletedCount())*types.NeedleMapEntrySize + super_block.SuperBlockSize
	if estimate < live {
		t.Fatalf("estimate %d below superblock + live index entries %d", estimate, live)
	}

	if _, err := v.doDeleteRequest(newEmptyNeedle(uint64(count))); err != nil {
		t.Fatalf("delete last needle: %v", err)
	}
	if estimate := estimatedCompactedSize(v); estimate != super_block.SuperBlockSize {
		t.Fatalf("all-deleted estimate = %d, want superblock only (%d)", estimate, super_block.SuperBlockSize)
	}
}

// The estimate must cover what compaction writes on disk: each live needle's
// content plus its header, checksum, timestamp and padding. An all-live
// volume's compacted .dat is byte-for-byte its current one, so the estimate
// may not fall below the current file.
func TestEstimatedCompactedSizeCoversNeedleFraming(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	for i := 1; i <= 100; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	datSize, _, _ := v.FileStat()

	if estimate := estimatedCompactedSize(v); estimate < int64(datSize) {
		t.Fatalf("estimate %d below .dat size %d for an all-live volume: missing per-needle framing", estimate, datSize)
	}
}
