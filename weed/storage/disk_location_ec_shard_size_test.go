package storage

import (
	"testing"
)

func TestCalculateExpectedShardSize(t *testing.T) {
	tests := []struct {
		name              string
		datFileSize       int64
		expectedShardSize int64
		description       string
	}{
		{
			name:              "Exact 10GB (1 large batch)",
			datFileSize:       10 * 1024 * 1024 * 1024, // 10GB = 1 large batch
			expectedShardSize: 1 * 1024 * 1024 * 1024,  // 1GB per shard
			description:       "Exactly fits in large blocks",
		},
		{
			name:              "11GB (1 large batch + 103 small blocks)",
			datFileSize:       11 * 1024 * 1024 * 1024,          // 11GB
			expectedShardSize: 1*1024*1024*1024 + 103*1024*1024, // 1GB + 103MB (103 small blocks for 1GB remaining)
			description:       "1GB large + 1GB remaining needs 103 small blocks",
		},
		{
			name:              "5MB (requires 1 small block per shard)",
			datFileSize:       5 * 1024 * 1024, // 5MB
			expectedShardSize: 1 * 1024 * 1024, // 1MB per shard (rounded up)
			description:       "Small file rounds up to 1MB per shard",
		},
		{
			name:              "15MB (requires 2 small blocks per shard)",
			datFileSize:       15 * 1024 * 1024, // 15MB
			expectedShardSize: 2 * 1024 * 1024,  // 2MB per shard
			description:       "15MB needs 2 small blocks per shard",
		},
		{
			name:              "1KB (minimum size)",
			datFileSize:       1024,
			expectedShardSize: 1 * 1024 * 1024, // 1MB per shard (1 small block)
			description:       "Tiny file needs 1 small block",
		},
		{
			name:              "10.5GB (mixed)",
			datFileSize:       10*1024*1024*1024 + 512*1024*1024, // 10.5GB
			expectedShardSize: 1*1024*1024*1024 + 52*1024*1024,   // 1GB + 52MB (52 small blocks for 512MB remaining)
			description:       "1GB large + 512MB remaining needs 52 small blocks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualShardSize := calculateExpectedShardSize(tt.datFileSize)

			if actualShardSize != tt.expectedShardSize {
				t.Errorf("Expected shard size %d, got %d. %s",
					tt.expectedShardSize, actualShardSize, tt.description)
			}

			t.Logf("✓ File size: %d → Shard size: %d (%s)",
				tt.datFileSize, actualShardSize, tt.description)
		})
	}
}

// TestShardSizeValidationScenarios tests realistic scenarios
func TestShardSizeValidationScenarios(t *testing.T) {
	scenarios := []struct {
		name            string
		datFileSize     int64
		actualShardSize int64
		shouldBeValid   bool
	}{
		{
			name:            "Valid: exact match for 10GB",
			datFileSize:     10 * 1024 * 1024 * 1024, // 10GB
			actualShardSize: 1 * 1024 * 1024 * 1024,  // 1GB (exact)
			shouldBeValid:   true,
		},
		{
			name:            "Invalid: 1 byte too small",
			datFileSize:     10 * 1024 * 1024 * 1024, // 10GB
			actualShardSize: 1*1024*1024*1024 - 1,    // 1GB - 1 byte
			shouldBeValid:   false,
		},
		{
			name:            "Invalid: 1 byte too large",
			datFileSize:     10 * 1024 * 1024 * 1024, // 10GB
			actualShardSize: 1*1024*1024*1024 + 1,    // 1GB + 1 byte
			shouldBeValid:   false,
		},
		{
			name:            "Valid: small file exact match",
			datFileSize:     5 * 1024 * 1024, // 5MB
			actualShardSize: 1 * 1024 * 1024, // 1MB (exact)
			shouldBeValid:   true,
		},
		{
			name:            "Invalid: wrong size for small file",
			datFileSize:     5 * 1024 * 1024, // 5MB
			actualShardSize: 500 * 1024,      // 500KB (too small)
			shouldBeValid:   false,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			expectedSize := calculateExpectedShardSize(scenario.datFileSize)
			isValid := scenario.actualShardSize == expectedSize

			if isValid != scenario.shouldBeValid {
				t.Errorf("Expected validation result %v, got %v. Actual shard: %d, Expected: %d",
					scenario.shouldBeValid, isValid, scenario.actualShardSize, expectedSize)
			}
		})
	}
}
