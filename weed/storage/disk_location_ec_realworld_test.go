package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// TestCalculateExpectedShardSizeWithRealEncoding validates our shard size calculation
// by actually running EC encoding on real files and comparing the results
func TestCalculateExpectedShardSizeWithRealEncoding(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		datFileSize int64
		description string
	}{
		{
			name:        "5MB file",
			datFileSize: 5 * 1024 * 1024,
			description: "Small file that needs 1 small block per shard",
		},
		{
			name:        "10MB file (exactly 10 small blocks)",
			datFileSize: 10 * 1024 * 1024,
			description: "Exactly fits in 1MB small blocks",
		},
		{
			name:        "15MB file",
			datFileSize: 15 * 1024 * 1024,
			description: "Requires 2 small blocks per shard",
		},
		{
			name:        "50MB file",
			datFileSize: 50 * 1024 * 1024,
			description: "Requires 5 small blocks per shard",
		},
		{
			name:        "100MB file",
			datFileSize: 100 * 1024 * 1024,
			description: "Requires 10 small blocks per shard",
		},
		{
			name:        "512MB file",
			datFileSize: 512 * 1024 * 1024,
			description: "Requires 52 small blocks per shard (rounded up)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test .dat file with the specified size
			baseFileName := filepath.Join(tempDir, "test_volume")
			datFileName := baseFileName + ".dat"

			// Create .dat file with random data pattern (so it's compressible but realistic)
			datFile, err := os.Create(datFileName)
			if err != nil {
				t.Fatalf("Failed to create .dat file: %v", err)
			}

			// Write some pattern data (not all zeros, to be more realistic)
			pattern := make([]byte, 4096)
			for i := range pattern {
				pattern[i] = byte(i % 256)
			}

			written := int64(0)
			for written < tt.datFileSize {
				toWrite := tt.datFileSize - written
				if toWrite > int64(len(pattern)) {
					toWrite = int64(len(pattern))
				}
				n, err := datFile.Write(pattern[:toWrite])
				if err != nil {
					t.Fatalf("Failed to write to .dat file: %v", err)
				}
				written += int64(n)
			}
			datFile.Close()

			// Calculate expected shard size using our function
			expectedShardSize := calculateExpectedShardSize(tt.datFileSize)

			// Run actual EC encoding
			err = erasure_coding.WriteEcFiles(baseFileName)
			if err != nil {
				t.Fatalf("Failed to encode EC files: %v", err)
			}

			// Measure actual shard sizes
			for i := 0; i < erasure_coding.TotalShardsCount; i++ {
				shardFileName := baseFileName + erasure_coding.ToExt(i)
				shardInfo, err := os.Stat(shardFileName)
				if err != nil {
					t.Fatalf("Failed to stat shard file %s: %v", shardFileName, err)
				}

				actualShardSize := shardInfo.Size()

				// Verify actual size matches expected size
				if actualShardSize != expectedShardSize {
					t.Errorf("Shard %d size mismatch:\n"+
						"  .dat file size: %d bytes\n"+
						"  Expected shard size: %d bytes\n"+
						"  Actual shard size: %d bytes\n"+
						"  Difference: %d bytes\n"+
						"  %s",
						i, tt.datFileSize, expectedShardSize, actualShardSize,
						actualShardSize-expectedShardSize, tt.description)
				}
			}

			// If we got here, all shards match!
			t.Logf("✓ SUCCESS: .dat size %d → actual shard size %d matches calculated size (%s)",
				tt.datFileSize, expectedShardSize, tt.description)

			// Cleanup
			os.Remove(datFileName)
			for i := 0; i < erasure_coding.TotalShardsCount; i++ {
				os.Remove(baseFileName + erasure_coding.ToExt(i))
			}
		})
	}
}

// TestCalculateExpectedShardSizeEdgeCases tests edge cases with real encoding
func TestCalculateExpectedShardSizeEdgeCases(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		datFileSize int64
	}{
		{"1 byte file", 1},
		{"1KB file", 1024},
		{"10KB file", 10 * 1024},
		{"1MB file (1 small block)", 1024 * 1024},
		{"1MB + 1 byte", 1024*1024 + 1},
		{"9.9MB (almost 1 small block per shard)", 9*1024*1024 + 900*1024},
		{"10.1MB (just over 1 small block per shard)", 10*1024*1024 + 100*1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseFileName := filepath.Join(tempDir, tt.name)
			datFileName := baseFileName + ".dat"

			// Create .dat file
			datFile, err := os.Create(datFileName)
			if err != nil {
				t.Fatalf("Failed to create .dat file: %v", err)
			}

			// Write exactly the specified number of bytes
			data := make([]byte, tt.datFileSize)
			for i := range data {
				data[i] = byte(i % 256)
			}
			datFile.Write(data)
			datFile.Close()

			// Calculate expected
			expectedShardSize := calculateExpectedShardSize(tt.datFileSize)

			// Run actual EC encoding
			err = erasure_coding.WriteEcFiles(baseFileName)
			if err != nil {
				t.Fatalf("Failed to encode EC files: %v", err)
			}

			// Check first shard (all should be same size)
			shardFileName := baseFileName + erasure_coding.ToExt(0)
			shardInfo, err := os.Stat(shardFileName)
			if err != nil {
				t.Fatalf("Failed to stat shard file: %v", err)
			}

			actualShardSize := shardInfo.Size()

			if actualShardSize != expectedShardSize {
				t.Errorf("File size %d: expected shard %d, got %d (diff: %d)",
					tt.datFileSize, expectedShardSize, actualShardSize, actualShardSize-expectedShardSize)
			} else {
				t.Logf("✓ File size %d → shard size %d (correct)", tt.datFileSize, actualShardSize)
			}

			// Cleanup
			os.Remove(datFileName)
			for i := 0; i < erasure_coding.TotalShardsCount; i++ {
				os.Remove(baseFileName + erasure_coding.ToExt(i))
			}
		})
	}
}
