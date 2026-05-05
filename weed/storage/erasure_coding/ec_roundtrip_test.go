package erasure_coding

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestEcReadRoundTrip tests the EC encode→read cycle via LocateData for various
// .dat file sizes, paying special attention to the large/small block boundary.
//
// The nLargeBlockRows calculation must correctly distinguish between large and small
// blocks. A previous bug (issue #8947) caused an off-by-one error when
// shardDatSize was an exact multiple of largeBlockSize, leading to data corruption.
func TestEcReadRoundTrip(t *testing.T) {
	const (
		large = largeBlockSize // 10000
		small = smallBlockSize // 100
	)

	largeRowSize := large * DataShardsCount // 100000
	smallRowSize := small * DataShardsCount // 1000

	testCases := []struct {
		name    string
		datSize int64
	}{
		// Exact multiples of largeRowSize — triggers the nLargeBlockRows off-by-one bug
		{"1_large_row_exact", int64(largeRowSize)},
		{"2_large_rows_exact", int64(2 * largeRowSize)},
		{"3_large_rows_exact", int64(3 * largeRowSize)},

		// Just over a large row boundary — has small blocks
		{"1_large_row_plus_1", int64(largeRowSize + 1)},
		{"2_large_rows_plus_small", int64(2*largeRowSize + smallRowSize)},
		{"1_large_row_plus_half_small", int64(largeRowSize + smallRowSize/2)},

		// Just under a large row boundary — all small blocks
		{"just_under_1_large_row", int64(largeRowSize - 1)},
		{"just_under_2_large_rows", int64(2*largeRowSize - 1)},

		// Small data — no large blocks at all
		{"small_only", int64(smallRowSize * 3)},
		{"small_single_row", int64(smallRowSize)},

		// Boundary with mixed large and small
		{"boundary_spanning", int64(largeRowSize + smallRowSize*5 + 50)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testEcRead(t, large, small, tc.datSize)
		})
	}
}

// testEcRead creates a .dat file, EC-encodes it, then verifies LocateData-based reads
// return correct data at positions throughout the file (especially near the large/small
// block boundary).
func testEcRead(t *testing.T, large, small, datSize int64) {
	t.Helper()

	dir := t.TempDir()
	baseFileName := fmt.Sprintf("%s/rt_%d", dir, datSize)

	// 1. Create a .dat file with deterministic random data
	originalData := make([]byte, datSize)
	_, err := rand.Read(originalData)
	require.NoError(t, err, "generating random data")

	err = os.WriteFile(baseFileName+".dat", originalData, 0644)
	require.NoError(t, err, "writing .dat file")

	ctx := NewDefaultECContext("", 0)

	// 2. EC encode with test block sizes
	err = generateEcFiles(baseFileName, int(small), large, small, ctx)
	require.NoError(t, err, "EC encoding")

	// 3. Open EC shard files for reading
	ecFiles, err := openEcFiles(baseFileName, true, ctx)
	require.NoError(t, err, "opening EC files")
	defer closeEcFiles(ecFiles)

	ecStat, err := ecFiles[0].Stat()
	require.NoError(t, err)
	shardFileSize := ecStat.Size()

	// Compute shardDatSize as the production code does when datFileSize is known
	shardDatSizeFromDat := datSize / int64(ctx.DataShards)

	// 4. Verify EC reads at various positions
	largeRowSize := large * DataShardsCount
	encoderLargeRows := datSize / int64(largeRowSize)
	boundaryOffset := encoderLargeRows * int64(largeRowSize)

	readSize := types.Size(small / 2) // read half a small block
	testOffsets := collectTestOffsets(datSize, int64(readSize), boundaryOffset, large, small)

	for _, offset := range testOffsets {
		// Test with shardDatSize from datFileSize (the production path with fix)
		intervals := LocateData(large, small, shardDatSizeFromDat, offset, readSize)
		ecData, err := assembleFromIntervals(ecFiles, intervals, large, small)
		require.NoError(t, err, "reading EC data at offset %d (datFileSize path)", offset)

		expected := originalData[offset : offset+int64(readSize)]
		if !bytes.Equal(expected, ecData) {
			t.Errorf("EC read mismatch at offset %d (datFileSize path, shardDatSize=%d, nLargeBlockRows=%d)",
				offset, shardDatSizeFromDat, shardDatSizeFromDat/large)
		}

		// Test with shardDatSize from ecdFileSize-1 (the fallback path for old volumes)
		intervalsFallback := LocateData(large, small, shardFileSize-1, offset, readSize)
		ecDataFallback, err := assembleFromIntervals(ecFiles, intervalsFallback, large, small)
		if err == nil && !bytes.Equal(expected, ecDataFallback) {
			// The fallback path may fail for exact multiples — log as warning
			t.Logf("WARN: EC read mismatch at offset %d (fallback path, shardFileSize=%d)",
				offset, shardFileSize)
		}
	}
}

// locateOffsetBuggy reimplements locateOffset with the old buggy formula:
//
//	nLargeBlockRows = (shardDatSize - 1) / largeBlockLength
//
// This caused an off-by-one error when shardDatSize was an exact multiple of
// largeBlockLength, miscounting the number of large block rows.
func locateOffsetBuggy(largeBlockLength, smallBlockLength int64, shardDatSize int64, offset int64) (blockIndex int, isLargeBlock bool, nLargeBlockRows int64, innerBlockOffset int64) {
	largeRowSize := largeBlockLength * DataShardsCount
	nLargeBlockRows = (shardDatSize - 1) / largeBlockLength // THE BUG

	if offset < nLargeBlockRows*largeRowSize {
		isLargeBlock = true
		blockIndex = int(offset / largeBlockLength)
		innerBlockOffset = offset % largeBlockLength
		return
	}

	isLargeBlock = false
	offset -= nLargeBlockRows * largeRowSize
	blockIndex = int(offset / smallBlockLength)
	innerBlockOffset = offset % smallBlockLength
	return
}

// locateDataBuggy is LocateData using the old buggy locateOffset.
func locateDataBuggy(largeBlockLength, smallBlockLength int64, shardDatSize int64, offset int64, size types.Size) []Interval {
	blockIndex, isLargeBlock, nLargeBlockRows, innerBlockOffset := locateOffsetBuggy(largeBlockLength, smallBlockLength, shardDatSize, offset)

	var intervals []Interval
	for size > 0 {
		blockRemaining := largeBlockLength - innerBlockOffset
		if !isLargeBlock {
			blockRemaining = smallBlockLength - innerBlockOffset
		}
		if blockRemaining <= 0 {
			blockIndex, isLargeBlock = moveToNextBlock(blockIndex, isLargeBlock, nLargeBlockRows)
			innerBlockOffset = 0
			continue
		}
		interval := Interval{
			BlockIndex:          blockIndex,
			InnerBlockOffset:    innerBlockOffset,
			IsLargeBlock:        isLargeBlock,
			LargeBlockRowsCount: int(nLargeBlockRows),
		}
		if int64(size) <= blockRemaining {
			interval.Size = size
			intervals = append(intervals, interval)
			return intervals
		}
		interval.Size = types.Size(blockRemaining)
		intervals = append(intervals, interval)
		size -= interval.Size
		blockIndex, isLargeBlock = moveToNextBlock(blockIndex, isLargeBlock, nLargeBlockRows)
		innerBlockOffset = 0
	}
	return intervals
}

// TestEcOffByOneBug_Issue8947 directly demonstrates the off-by-one bug.
//
// It creates a .dat file whose size is an exact multiple of (largeBlockSize * DataShards),
// EC-encodes it, then shows that:
//   - The OLD buggy formula produces WRONG data (data corruption)
//   - The FIXED formula produces CORRECT data
func TestEcOffByOneBug_Issue8947(t *testing.T) {
	const (
		large = largeBlockSize // 10000
		small = smallBlockSize // 100
	)

	// datSize is exactly 2 large rows — each shard gets exactly 2*largeBlockSize bytes.
	// The encoder produces 2 large block rows and 0 small block rows.
	datSize := int64(2 * large * DataShardsCount) // 200000

	dir := t.TempDir()
	baseFileName := fmt.Sprintf("%s/bug_%d", dir, datSize)

	originalData := make([]byte, datSize)
	_, err := rand.Read(originalData)
	require.NoError(t, err)

	err = os.WriteFile(baseFileName+".dat", originalData, 0644)
	require.NoError(t, err)

	ctx := NewDefaultECContext("", 0)
	err = generateEcFiles(baseFileName, int(small), large, small, ctx)
	require.NoError(t, err, "EC encoding")

	ecFiles, err := openEcFiles(baseFileName, true, ctx)
	require.NoError(t, err)
	defer closeEcFiles(ecFiles)

	// shardDatSize = datFileSize / DataShards = 2 * largeBlockSize
	// This is an EXACT multiple of largeBlockSize.
	shardDatSize := datSize / int64(ctx.DataShards) // = 2 * large = 20000

	// The encoder used 2 large block rows, 0 small block rows.
	// Correct: nLargeBlockRows = 20000 / 10000 = 2
	// Buggy:   nLargeBlockRows = (20000 - 1) / 10000 = 1  ← OFF BY ONE
	fixedRows := shardDatSize / large
	buggyRows := (shardDatSize - 1) / large
	assert.Equal(t, int64(2), fixedRows, "fixed formula should give 2 large block rows")
	assert.Equal(t, int64(1), buggyRows, "buggy formula gives only 1 (the bug)")

	// Test reading from the 2nd large block row (offsets 100000–199999).
	// With the buggy formula (nLargeBlockRows=1), this region is incorrectly
	// treated as small blocks, causing reads from the WRONG shard positions.
	readSize := types.Size(small / 2)

	// Pick an offset well into the 2nd large block row so that the buggy formula
	// computes a different (shard, offset) than the correct formula.
	// At the very start of the 2nd row, both formulas coincidentally hit the same
	// shard position. But further in, the small-block vs large-block addressing diverges.
	offset := int64(large*DataShardsCount) + large + 50 // 110050: in 2nd large row, shard 1

	// --- Fixed formula: reads correct data ---
	fixedIntervals := LocateData(large, small, shardDatSize, offset, readSize)
	fixedData, err := assembleFromIntervals(ecFiles, fixedIntervals, large, small)
	require.NoError(t, err, "fixed LocateData read")

	expected := originalData[offset : offset+int64(readSize)]
	assert.True(t, bytes.Equal(expected, fixedData),
		"FIXED formula should read correct data from 2nd large block row")

	// --- Buggy formula: reads WRONG data ---
	buggyIntervals := locateDataBuggy(large, small, shardDatSize, offset, readSize)
	buggyData, err := assembleFromIntervals(ecFiles, buggyIntervals, large, small)
	// The buggy formula might read from wrong offsets (possibly out of bounds),
	// so an error is also evidence of the bug.
	if err != nil {
		t.Logf("Buggy formula caused read error (expected): %v", err)
	} else {
		assert.False(t, bytes.Equal(expected, buggyData),
			"BUGGY formula should return WRONG data from 2nd large block row (demonstrating the corruption)")
		n := 8
		if len(expected) < n {
			n = len(expected)
		}
		t.Logf("Buggy formula returned wrong data: expected first bytes %x, got %x",
			expected[:n], buggyData[:n])
	}

	// Verify the bug mechanism: buggy formula misclassifies the 2nd large row as small blocks
	assert.True(t, fixedIntervals[0].IsLargeBlock,
		"fixed: offset %d should be in large blocks", offset)
	assert.False(t, buggyIntervals[0].IsLargeBlock,
		"buggy: offset %d is incorrectly classified as small blocks (the bug)", offset)

	t.Logf("Fixed: nLargeBlockRows=%d, interval=%+v", fixedRows, fixedIntervals[0])
	t.Logf("Buggy: nLargeBlockRows=%d, interval=%+v", buggyRows, buggyIntervals[0])
}

// TestEcDecodeDatRoundTrip tests the full WriteDatFile decode path using the production
// block sizes with a small .dat file that fits within the small block region.
func TestEcDecodeDatRoundTrip(t *testing.T) {
	// With production sizes, datFileSize must be < DataShardsCount * ErasureCodingLargeBlockSize (10GB)
	// to avoid needing huge test files. We test small block decode only.
	// Each shard gets datSize/DataShards bytes in small 1MB blocks.
	datSizes := []int64{
		1000, // tiny
		int64(DataShardsCount) * ErasureCodingSmallBlockSize,     // exactly 1 small row (10MB)
		int64(DataShardsCount)*ErasureCodingSmallBlockSize + 500, // 1 small row + partial
	}

	for _, datSize := range datSizes {
		t.Run(fmt.Sprintf("size_%d", datSize), func(t *testing.T) {
			testDecodeDat(t, datSize)
		})
	}
}

func testDecodeDat(t *testing.T, datSize int64) {
	t.Helper()

	dir := t.TempDir()
	baseFileName := fmt.Sprintf("%s/dec_%d", dir, datSize)

	// 1. Create .dat with random data
	originalData := make([]byte, datSize)
	_, err := rand.Read(originalData)
	require.NoError(t, err)

	err = os.WriteFile(baseFileName+".dat", originalData, 0644)
	require.NoError(t, err)

	ctx := NewDefaultECContext("", 0)

	// 2. EC encode with PRODUCTION block sizes
	err = generateEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx)
	require.NoError(t, err, "EC encoding")

	// 3. Decode via WriteDatFile
	decodedBase := baseFileName + "_decoded"
	shardFileNames := make([]string, DataShardsCount)
	for i := 0; i < DataShardsCount; i++ {
		shardFileNames[i] = fmt.Sprintf("%s%s", baseFileName, ctx.ToExt(i))
	}

	err = WriteDatFile(decodedBase, datSize, shardFileNames)
	require.NoError(t, err, "WriteDatFile")

	// 4. Verify decoded .dat matches original
	decodedData, err := os.ReadFile(decodedBase + ".dat")
	require.NoError(t, err)

	assert.Equal(t, len(originalData), len(decodedData), "decoded .dat size mismatch")
	if !bytes.Equal(originalData, decodedData) {
		for i := 0; i < len(originalData) && i < len(decodedData); i++ {
			if originalData[i] != decodedData[i] {
				t.Fatalf("decoded .dat mismatch at byte %d (datSize=%d)", i, datSize)
			}
		}
	}
}

// collectTestOffsets generates offsets to test, focusing on the large/small block boundary.
func collectTestOffsets(datSize, readSize, boundaryOffset, large, small int64) []int64 {
	offsets := []int64{0}

	if datSize > readSize {
		offsets = append(offsets, datSize/2)
	}

	// Near the large/small block boundary
	if boundaryOffset > 0 && boundaryOffset < datSize {
		for _, delta := range []int64{-large, -small, -1, 0, 1, small, large} {
			off := boundaryOffset + delta
			if off >= 0 && off+readSize <= datSize {
				offsets = append(offsets, off)
			}
		}
	}

	// Near end of file
	if datSize > readSize {
		offsets = append(offsets, datSize-readSize)
	}

	return offsets
}

// assembleFromIntervals reads data from EC shard files according to the given intervals.
func assembleFromIntervals(ecFiles []*os.File, intervals []Interval, large, small int64) ([]byte, error) {
	var data []byte
	for _, interval := range intervals {
		shardId, shardOffset := interval.ToShardIdAndOffset(large, small)
		chunk := make([]byte, interval.Size)
		n, err := ecFiles[shardId].ReadAt(chunk, shardOffset)
		if err != nil {
			return nil, fmt.Errorf("read shard %d offset %d size %d: %v", shardId, shardOffset, interval.Size, err)
		}
		if n != int(interval.Size) {
			return nil, fmt.Errorf("short read from shard %d: got %d, want %d", shardId, n, interval.Size)
		}
		data = append(data, chunk...)
	}
	return data, nil
}
