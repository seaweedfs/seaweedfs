package erasure_coding

import (
	"io"
	"os"
	"testing"

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
