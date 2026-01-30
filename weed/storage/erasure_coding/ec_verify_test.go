package erasure_coding

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVerifyMultiCorruptShards(t *testing.T) {
	tests := []struct {
		name            string
		shardsToCorrupt []int
		expectFailure   bool
		expectSpecific  bool
	}{
		{"SingleCorruption", []int{3}, true, true},
		{"DoubleCorruption", []int{0, 5}, true, true},
		{"TripleCorruption", []int{0, 3, 12}, true, true},
		// Quadruple corruption is ambiguous for a single block,
		// but should still return false for 'verified'.
		{"QuadrupleCorruption", []int{0, 3, 7, 12}, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseFileName := "test_verify_" + tt.name
			bufferSize := 1024
			largeBlockSize := int64(1024 * 1024)
			smallBlockSize := int64(1024)
			ctx := NewDefaultECContext("", 0)

			defer removeGeneratedFiles(baseFileName, ctx)
			defer os.Remove(baseFileName + ".dat")

			f, _ := os.Create(baseFileName + ".dat")
			data := make([]byte, largeBlockSize)
			for i := range data {
				data[i] = byte(i % 256)
			}
			f.Write(data)
			f.Close()

			assert.Nil(t, generateEcFiles(baseFileName, bufferSize, largeBlockSize, smallBlockSize, ctx))

			for _, sid := range tt.shardsToCorrupt {
				corruptShard(t, baseFileName, ctx, sid)
			}

			files, err := openEcFiles(baseFileName, true, ctx)
			assert.Nil(t, err)
			defer closeEcFiles(files)

			shardReader := func(shardId uint32, offset int64, size int64) ([]byte, error) {
				data := make([]byte, size)
				n, err := files[shardId].ReadAt(data, offset)
				if err != nil {
					return nil, err
				}
				return data[:n], nil
			}

			expectedShardSize := largeBlockSize / int64(ctx.DataShards)
			verified, suspects, err := VerifyEcShards(baseFileName, expectedShardSize, shardReader)
			assert.Nil(t, err)
			assert.Equal(t, !tt.expectFailure, verified)

			if tt.expectSpecific {
				var expectedSuspects []uint32
				for _, s := range tt.shardsToCorrupt {
					expectedSuspects = append(expectedSuspects, uint32(s))
				}
				assert.ElementsMatch(t, expectedSuspects, suspects)
			} else if tt.expectFailure {
				assert.NotEmpty(t, suspects)
			}
		})
	}
}

func corruptShard(t *testing.T, baseFileName string, ctx *ECContext, shardId int) {
	fname := baseFileName + ctx.ToExt(shardId)
	f, err := os.OpenFile(fname, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open shard %d: %v", shardId, err)
	}
	defer f.Close()
	// Write unique garbage for 1MB
	garbage := make([]byte, 1024*1024)
	for i := range garbage {
		garbage[i] = byte(shardId + i%128)
	}
	f.WriteAt(garbage, 0)
}

func TestVerifyDistributedShards(t *testing.T) {
	baseFileName := "test_verify_distributed"
	bufferSize := 1024
	largeBlockSize := int64(1024 * 1024)
	smallBlockSize := int64(1024)
	ctx := NewDefaultECContext("", 0)

	defer removeGeneratedFiles(baseFileName, ctx)
	defer os.Remove(baseFileName + ".dat")

	// Generate clean EC files
	f, _ := os.Create(baseFileName + ".dat")
	data := make([]byte, largeBlockSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	f.Write(data)
	f.Close()
	assert.Nil(t, generateEcFiles(baseFileName, bufferSize, largeBlockSize, smallBlockSize, ctx))

	// Open all shard files to simulate distributed storage
	shardFiles := make([]*os.File, ctx.Total())
	for i := 0; i < ctx.Total(); i++ {
		fname := baseFileName + ctx.ToExt(i)
		file, err := os.Open(fname)
		assert.Nil(t, err)
		defer file.Close()
		shardFiles[i] = file
	}

	// Simulate distributed shard reader with network-like behavior
	// - Add artificial delays for some shards
	// - Simulate network errors for others (temporarily)
	networkErrors := map[int]bool{2: true, 7: true} // Shards 2 and 7 will have temporary network issues
	readAttempts := make(map[int]int)

	shardReader := func(shardId uint32, offset int64, size int64) ([]byte, error) {
		shardIdx := int(shardId)
		readAttempts[shardIdx]++

		// Simulate network errors on first read attempt for some shards
		if networkErrors[shardIdx] && readAttempts[shardIdx] == 1 {
			return nil, io.ErrUnexpectedEOF // Simulate network failure
		}

		// Add artificial delay for some shards to simulate network latency
		if shardIdx == 5 {
			time.Sleep(10 * time.Millisecond) // Simulate slow shard
		}

		data := make([]byte, size)
		n, err := shardFiles[shardIdx].ReadAt(data, offset)
		if err != nil && err != io.EOF {
			return nil, err
		}
		return data[:n], nil
	}

	// Determine expected shard size from a healthy shard
	stat, err := shardFiles[0].Stat()
	assert.Nil(t, err)
	expectedShardSize := stat.Size()

	// First verification attempt should handle network errors gracefully
	// Even with 2 shards having network errors initially, verification should succeed
	// because there are still 12 shards available (enough for RS verification)
	verified, suspects, err := VerifyEcShards(baseFileName, expectedShardSize, shardReader)
	assert.Nil(t, err)
	assert.True(t, verified, "Should pass verification despite network errors (fault-tolerant design)")
	assert.Empty(t, suspects, "Should have no suspect shards")

	// Second verification attempt should also succeed (network errors resolved)
	verified, suspects, err = VerifyEcShards(baseFileName, expectedShardSize, shardReader)
	assert.Nil(t, err)
	assert.True(t, verified, "Should pass verification on retry")
	assert.Empty(t, suspects, "Should have no suspect shards")

	// Test that network latency is handled gracefully
	// Read all shards to ensure no timeouts occurred
	for i := 0; i < ctx.Total(); i++ {
		if networkErrors[i] {
			assert.GreaterOrEqual(t, readAttempts[i], 2, "Shard %d should have been retried after network error", i)
		} else {
			assert.GreaterOrEqual(t, readAttempts[i], 1, "Shard %d should have been read", i)
		}
	}

	// Test partial shard availability (missing some shards)
	// Simulate that shards 1 and 8 are completely unavailable
	shardReaderMissing := func(shardId uint32, offset int64, size int64) ([]byte, error) {
		shardIdx := int(shardId)
		if shardIdx == 1 || shardIdx == 8 {
			return nil, os.ErrNotExist // Simulate completely missing shards
		}
		return shardReader(shardId, offset, size)
	}

	verified, suspects, err = VerifyEcShards(baseFileName, expectedShardSize, shardReaderMissing)
	assert.Nil(t, err)
	assert.True(t, verified, "Should pass verification with missing shards (as long as enough remain)")
	assert.Empty(t, suspects, "Should identify no specific corrupt shards when others are missing")
}

func TestVerifyOneMissingOneCorrupt(t *testing.T) {
	baseFileName := "test_verify_missing_corrupt"
	bufferSize := 1024
	largeBlockSize := int64(1024 * 1024)
	smallBlockSize := int64(1024)
	ctx := NewDefaultECContext("", 0)

	defer removeGeneratedFiles(baseFileName, ctx)
	defer os.Remove(baseFileName + ".dat")

	// Generate clean EC files
	f, _ := os.Create(baseFileName + ".dat")
	data := make([]byte, largeBlockSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	f.Write(data)
	f.Close()
	assert.Nil(t, generateEcFiles(baseFileName, bufferSize, largeBlockSize, smallBlockSize, ctx))

	// 1. DELETE one shard (Shard 0)
	os.Remove(baseFileName + ctx.ToExt(0))

	// 2. CORRUPT another shard (Shard 5)
	// We must write exactly the shard size to avoid resizing it if it's smaller than 1MB
	stat, err := os.Stat(baseFileName + ctx.ToExt(5))
	if err != nil {
		t.Fatalf("stat shard 5: %v", err)
	}
	f, err = os.OpenFile(baseFileName+ctx.ToExt(5), os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open shard 5: %v", err)
	}
	garbage := make([]byte, stat.Size())
	for i := range garbage {
		garbage[i] = byte(5 + i%128)
	}
	f.WriteAt(garbage, 0)
	f.Close()

	// Custom reader to handle the missing file
	shardReader := func(shardId uint32, offset int64, size int64) ([]byte, error) {
		if shardId == 0 {
			// Simulate missing shard
			return nil, os.ErrNotExist
		}
		fname := baseFileName + ctx.ToExt(int(shardId))
		f, err := os.OpenFile(fname, os.O_RDONLY, 0644)
		if err != nil {
			t.Logf("Failed to open shard %d: %v", shardId, err)
			return nil, err
		}
		defer f.Close()

		data := make([]byte, size)
		n, err := f.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			t.Logf("ReadAt error for shard %d: %v", shardId, err)
			return nil, err
		}
		return data[:n], nil
	}

	// Determine actual shard size from a healthy shard
	statHealthy, errHealthy := os.Stat(baseFileName + ctx.ToExt(1))
	if errHealthy != nil {
		t.Fatalf("Failed to stat shard 1: %v", errHealthy)
	}
	expectedShardSize := statHealthy.Size()

	verified, suspects, err := VerifyEcShards(baseFileName, expectedShardSize, shardReader)

	assert.Nil(t, err)
	assert.False(t, verified, "Should fail verification")
	assert.Contains(t, suspects, uint32(5), "Should identify corrupted shard 5")
	assert.NotContains(t, suspects, uint32(0), "Should NOT blame missing shard 0 as corrupt")
	assert.Equal(t, 1, len(suspects), "Should find exactly 1 corrupted shard")
}
