package storage

import (
	"testing"
)

func TestCalculateExpectedShardSize(t *testing.T) {
	const (
		largeBlock     = 1024 * 1024 * 1024 // 1GB
		smallBlock     = 1024 * 1024        // 1MB
		dataShards     = 10
		largeBatchSize = largeBlock * dataShards // 10GB
		smallBatchSize = smallBlock * dataShards // 10MB
	)

	tests := []struct {
		name              string
		datFileSize       int64
		expectedShardSize int64
		description       string
	}{
		// Edge case: empty file
		{
			name:              "0 bytes (empty file)",
			datFileSize:       0,
			expectedShardSize: 0,
			description:       "Empty file has 0 shard size",
		},

		// Boundary tests: exact multiples of large block
		{
			name:              "Exact 10GB (1 large batch)",
			datFileSize:       largeBatchSize, // 10GB = 1 large batch
			expectedShardSize: largeBlock,     // 1GB per shard
			description:       "Exactly fits in large blocks",
		},
		{
			name:              "Exact 20GB (2 large batches)",
			datFileSize:       2 * largeBatchSize, // 20GB
			expectedShardSize: 2 * largeBlock,     // 2GB per shard
			description:       "2 complete large batches",
		},
		{
			name:              "Just under large batch (10GB - 1 byte)",
			datFileSize:       largeBatchSize - 1, // 10,737,418,239 bytes
			expectedShardSize: 1024 * smallBlock,  // 1024MB = 1GB (needs 1024 small blocks)
			description:       "Just under 10GB needs 1024 small blocks",
		},
		{
			name:              "Just over large batch (10GB + 1 byte)",
			datFileSize:       largeBatchSize + 1,      // 10GB + 1 byte
			expectedShardSize: largeBlock + smallBlock, // 1GB + 1MB
			description:       "Just over 10GB adds 1 small block",
		},

		// Boundary tests: exact multiples of small batch
		{
			name:              "Exact 10MB (1 small batch)",
			datFileSize:       smallBatchSize, // 10MB
			expectedShardSize: smallBlock,     // 1MB per shard
			description:       "Exactly fits in 1 small batch",
		},
		{
			name:              "Exact 20MB (2 small batches)",
			datFileSize:       2 * smallBatchSize, // 20MB
			expectedShardSize: 2 * smallBlock,     // 2MB per shard
			description:       "2 complete small batches",
		},
		{
			name:              "Just under small batch (10MB - 1 byte)",
			datFileSize:       smallBatchSize - 1, // 10MB - 1 byte
			expectedShardSize: smallBlock,         // Still needs 1MB per shard (rounds up)
			description:       "Just under 10MB rounds up to 1 small block",
		},
		{
			name:              "Just over small batch (10MB + 1 byte)",
			datFileSize:       smallBatchSize + 1, // 10MB + 1 byte
			expectedShardSize: 2 * smallBlock,     // 2MB per shard
			description:       "Just over 10MB needs 2 small blocks",
		},

		// Mixed: large batch + partial small batch
		{
			name:              "10GB + 1MB",
			datFileSize:       largeBatchSize + 1*1024*1024, // 10GB + 1MB
			expectedShardSize: largeBlock + smallBlock,      // 1GB + 1MB
			description:       "1 large batch + 1MB needs 1 small block",
		},
		{
			name:              "10GB + 5MB",
			datFileSize:       largeBatchSize + 5*1024*1024, // 10GB + 5MB
			expectedShardSize: largeBlock + smallBlock,      // 1GB + 1MB
			description:       "1 large batch + 5MB rounds up to 1 small block",
		},
		{
			name:              "10GB + 15MB",
			datFileSize:       largeBatchSize + 15*1024*1024, // 10GB + 15MB
			expectedShardSize: largeBlock + 2*smallBlock,     // 1GB + 2MB
			description:       "1 large batch + 15MB needs 2 small blocks",
		},

		// Original test cases
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
