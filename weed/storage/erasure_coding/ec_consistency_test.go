package erasure_coding

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestEcConsistency_WritesBetweenEncodeAndEcx reproduces a race condition that
// existed in VolumeEcShardsGenerate before the fix in this PR.
//
// Previously, the order was:
//   1. WriteEcFilesWithContext(baseFileName, ecCtx)        — EC shards from .dat
//   2. WriteSortedFileFromIdx(v.IndexFileName(), ".ecx")   — .ecx from .idx
//
// If a write appended data to .dat/.idx between steps 1 and 2, the .ecx would
// have entries pointing to data that doesn't exist in the EC shards.
//
// The fix reverses the order (write .ecx first, then generate EC shards), so
// that .ecx is always a subset of what the EC shards contain.
//
// This test simulates the old buggy sequence to validate that the problem is real.
func TestEcConsistency_WritesBetweenEncodeAndEcx(t *testing.T) {
	dir := t.TempDir()
	baseFileName := dir + "/consistency"

	ctx := NewDefaultECContext("", 0)

	// Phase 1: Create initial .dat and .idx with known data
	datSize := int64(largeBlockSize*DataShardsCount + smallBlockSize*DataShardsCount*3) // 1 large row + 3 small rows
	originalData := make([]byte, datSize)
	rand.Read(originalData)

	err := os.WriteFile(baseFileName+".dat", originalData, 0644)
	require.NoError(t, err)

	// Create a minimal .idx with one entry pointing to the data
	createTestIdx(t, baseFileName+".idx", []idxEntry{
		{id: 1, offset: 0, size: types.Size(datSize)},
	})

	// Phase 2: EC encode — generates .ec00-.ec13 from current .dat
	err = generateEcFiles(baseFileName, int(smallBlockSize), largeBlockSize, smallBlockSize, ctx)
	require.NoError(t, err, "EC encoding")

	// Phase 3: SIMULATE a write between EC encoding and .ecx generation
	// (reproducing the old buggy order where .ecx was generated after EC shards)
	extraData := make([]byte, 5000)
	rand.Read(extraData)

	f, err := os.OpenFile(baseFileName+".dat", os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write(extraData)
	require.NoError(t, err)
	f.Close()

	// Update .idx with the new entry
	createTestIdx(t, baseFileName+".idx", []idxEntry{
		{id: 1, offset: 0, size: types.Size(datSize)},
		{id: 2, offset: datSize, size: types.Size(len(extraData))},
	})

	// Phase 4: Generate .ecx from the UPDATED .idx (as the old buggy code did)
	err = WriteSortedFileFromIdx(baseFileName, ".ecx")
	require.NoError(t, err, "WriteSortedFileFromIdx")

	// Phase 5: Now try to read needle 2 via EC shards — it should fail
	// because the EC shards were generated from the OLD .dat (without the extra data)
	ecFiles, err := openEcFiles(baseFileName, true, ctx)
	require.NoError(t, err)
	defer closeEcFiles(ecFiles)

	ecStat, err := ecFiles[0].Stat()
	require.NoError(t, err)
	shardSize := ecStat.Size()

	// Read needle 2 (the one added after EC encoding) using LocateData
	actualSize := needle.GetActualSize(types.Size(len(extraData)), needle.Version3)
	intervals := LocateData(largeBlockSize, smallBlockSize, shardSize-1, datSize, types.Size(actualSize))

	t.Logf("Trying to read needle 2 at offset %d size %d from EC shards (shardSize=%d)", datSize, actualSize, shardSize)
	t.Logf("Intervals: %+v", intervals)

	// Try to read — this will either fail with an error (offset out of bounds)
	// or return garbage data (the padded zeros from EC encoding)
	ecData, readErr := assembleFromIntervalsAllowError(ecFiles, intervals, largeBlockSize, smallBlockSize)

	if readErr != nil {
		t.Logf("CONFIRMED: Read error for needle written after EC encoding: %v", readErr)
	} else {
		// If we got data, it should be zeros (padding) or garbage, not the actual extraData
		isAllZeros := true
		for _, b := range ecData {
			if b != 0 {
				isAllZeros = false
				break
			}
		}
		if isAllZeros {
			t.Logf("CONFIRMED: Read returned zero-padded data (EC shards don't have the needle)")
		} else if !bytes.Equal(ecData[:len(extraData)], extraData) {
			t.Logf("CONFIRMED: Read returned wrong data (EC shards don't have the needle)")
		} else {
			t.Error("UNEXPECTED: Read returned correct data — needle should NOT be in EC shards")
		}
	}

	// Phase 6: Verify needle 1 (the original) still reads correctly
	intervals1 := LocateData(largeBlockSize, smallBlockSize, shardSize-1, 0, types.Size(needle.GetActualSize(types.Size(datSize), needle.Version3)))
	ecData1, err := assembleFromIntervalsAllowError(ecFiles, intervals1, largeBlockSize, smallBlockSize)

	if err == nil {
		// The assembled EC data should match the original .dat data
		assert.True(t, bytes.Equal(originalData, ecData1[:datSize]),
			"Original needle data should match EC shard data")
		t.Logf("Original needle reads correctly from EC shards")
	} else {
		t.Logf("Error reading original needle: %v", err)
	}
}

// TestEcConsistency_ExactLargeRowEncoding verifies that generateEcFiles correctly
// encodes a .dat file whose size is exactly one large row (DataShardsCount *
// largeBlockSize), producing shards of exactly largeBlockSize each, and that
// every chunk of the encoded data can be read back correctly via LocateData.
func TestEcConsistency_ExactLargeRowEncoding(t *testing.T) {
	dir := t.TempDir()
	baseFileName := dir + "/exact"
	ctx := NewDefaultECContext("", 0)

	datSize := int64(largeBlockSize * DataShardsCount) // exactly 1 large row
	data := make([]byte, datSize)
	rand.Read(data)
	err := os.WriteFile(baseFileName+".dat", data, 0644)
	require.NoError(t, err)

	// EC encode
	err = generateEcFiles(baseFileName, int(smallBlockSize), largeBlockSize, smallBlockSize, ctx)
	require.NoError(t, err)

	// Check shard sizes — each shard should be exactly largeBlockSize
	ecFiles, err := openEcFiles(baseFileName, true, ctx)
	require.NoError(t, err)
	defer closeEcFiles(ecFiles)

	for i := 0; i < ctx.DataShards; i++ {
		stat, err := ecFiles[i].Stat()
		require.NoError(t, err, "stat shard %d", i)
		assert.Equal(t, int64(largeBlockSize), stat.Size(),
			"data shard %d should be exactly largeBlockSize", i)
	}

	// Verify data reads correctly at every smallBlockSize offset via LocateData
	shardDatSize := datSize / int64(ctx.DataShards)
	readSize := types.Size(smallBlockSize)
	for offset := int64(0); offset+int64(readSize) <= datSize; offset += int64(readSize) {
		intervals := LocateData(largeBlockSize, smallBlockSize, shardDatSize, offset, readSize)
		ecData, err := assembleFromIntervalsAllowError(ecFiles, intervals, largeBlockSize, smallBlockSize)
		require.NoError(t, err, "reading at offset %d", offset)
		expected := data[offset : offset+int64(readSize)]
		assert.True(t, bytes.Equal(expected, ecData),
			"data mismatch at offset %d", offset)
	}
}

type idxEntry struct {
	id     types.NeedleId
	offset int64
	size   types.Size
}

func createTestIdx(t *testing.T, filename string, entries []idxEntry) {
	t.Helper()
	f, err := os.Create(filename)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, types.NeedleMapEntrySize)
	for _, e := range entries {
		types.NeedleIdToBytes(buf[:types.NeedleIdSize], e.id)
		types.OffsetToBytes(buf[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], types.ToOffset(e.offset))
		types.SizeToBytes(buf[types.NeedleIdSize+types.OffsetSize:], e.size)
		_, err := f.Write(buf)
		require.NoError(t, err)
	}
}

func assembleFromIntervalsAllowError(ecFiles []*os.File, intervals []Interval, large, small int64) ([]byte, error) {
	var data []byte
	for _, interval := range intervals {
		shardId, shardOffset := interval.ToShardIdAndOffset(large, small)
		if int(shardId) >= len(ecFiles) {
			return nil, fmt.Errorf("shard %d out of range (have %d files)", shardId, len(ecFiles))
		}
		stat, err := ecFiles[shardId].Stat()
		if err != nil {
			return nil, fmt.Errorf("stat shard %d: %v", shardId, err)
		}
		if shardOffset+int64(interval.Size) > stat.Size() {
			return nil, fmt.Errorf("read past end of shard %d: offset %d + size %d > fileSize %d",
				shardId, shardOffset, interval.Size, stat.Size())
		}
		chunk := make([]byte, interval.Size)
		n, err := ecFiles[shardId].ReadAt(chunk, shardOffset)
		if err != nil {
			return nil, fmt.Errorf("read shard %d offset %d: %v", shardId, shardOffset, err)
		}
		if n != int(interval.Size) {
			return nil, fmt.Errorf("short read shard %d: got %d want %d", shardId, n, interval.Size)
		}
		data = append(data, chunk...)
	}
	return data, nil
}
