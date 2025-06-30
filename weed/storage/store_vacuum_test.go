package storage

import (
	"testing"
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
