package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSortedFileGeneration(t *testing.T) {
	// Test with the provided test data files
	testFiles := []string{
		"../../test/data/iceberg_173",
		"../../test/data/iceberg_674",
	}

	for _, baseFileName := range testFiles {
		t.Run(fmt.Sprintf("TestSortedFileGeneration_%s", filepath.Base(baseFileName)), func(t *testing.T) {
			testSortedFileGeneration(t, baseFileName)
		})
	}
}

func testSortedFileGeneration(t *testing.T, baseFileName string) {
	// Open the original idx file
	idxFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open %s.idx: %v", baseFileName, err)
	}
	defer idxFile.Close()

	// Generate sorted file
	sdxFileName := baseFileName + ".sdx"
	err = erasure_coding.WriteSortedFileFromIdx(baseFileName, ".sdx")
	if err != nil {
		t.Fatalf("Failed to generate sorted file for %s: %v", baseFileName, err)
	}
	defer os.Remove(sdxFileName) // Clean up

	// Verify the sorted file was created
	if _, err := os.Stat(sdxFileName); os.IsNotExist(err) {
		t.Fatalf("Sorted file %s was not created", sdxFileName)
	}

	// Read original entries
	originalEntries, err := readIndexFileEntries(idxFile)
	if err != nil {
		t.Fatalf("Failed to read original entries from %s: %v", baseFileName, err)
	}

	// Read sorted entries
	sdxFile, err := os.OpenFile(sdxFileName, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open sorted file %s: %v", sdxFileName, err)
	}
	defer sdxFile.Close()

	sortedEntries, err := readIndexFileEntries(sdxFile)
	if err != nil {
		t.Fatalf("Failed to read sorted entries from %s: %v", sdxFileName, err)
	}

	// Build final state map from original entries (same logic as readNeedleMap)
	finalStateMap := make(map[NeedleId]needle_map.NeedleValue)
	for _, entry := range originalEntries {
		if !entry.Offset.IsZero() && !entry.Size.IsDeleted() {
			finalStateMap[entry.Key] = entry
		} else {
			delete(finalStateMap, entry.Key)
		}
	}

	// Convert map to slice for comparison
	validOriginalEntries := make([]needle_map.NeedleValue, 0, len(finalStateMap))
	for _, entry := range finalStateMap {
		validOriginalEntries = append(validOriginalEntries, entry)
	}

	// Verify the number of valid entries matches
	if len(validOriginalEntries) != len(sortedEntries) {
		t.Fatalf("Valid entry count mismatch: original=%d, sorted=%d", len(validOriginalEntries), len(sortedEntries))
	}

	// Verify all valid entries are present
	err = verifyAllEntriesPresent(validOriginalEntries, sortedEntries)
	if err != nil {
		t.Fatalf("Data verification failed: %v", err)
	}

	// Verify the sorted file is actually sorted
	err = verifySortedOrder(sortedEntries)
	if err != nil {
		t.Fatalf("Sorted order verification failed: %v", err)
	}

	// Test binary search functionality
	err = testBinarySearch(t, sdxFile, sortedEntries)
	if err != nil {
		t.Fatalf("Binary search test failed: %v", err)
	}

	t.Logf("Successfully verified sorted file generation for %s: %d valid entries out of %d total", baseFileName, len(sortedEntries), len(originalEntries))
}

func testSortedFileGenerationWithDeletedEntries(t *testing.T, baseFileName string) {
	// Open the original idx file
	idxFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open %s.idx: %v", baseFileName, err)
	}
	defer idxFile.Close()

	// Generate sorted file
	sdxFileName := baseFileName + ".sdx"
	err = erasure_coding.WriteSortedFileFromIdx(baseFileName, ".sdx")
	if err != nil {
		t.Fatalf("Failed to generate sorted file for %s: %v", baseFileName, err)
	}
	defer os.Remove(sdxFileName) // Clean up

	// Verify the sorted file was created
	if _, err := os.Stat(sdxFileName); os.IsNotExist(err) {
		t.Fatalf("Sorted file %s was not created", sdxFileName)
	}

	// Read original entries
	originalEntries, err := readIndexFileEntries(idxFile)
	if err != nil {
		t.Fatalf("Failed to read original entries from %s: %v", baseFileName, err)
	}

	// Read sorted entries
	sdxFile, err := os.OpenFile(sdxFileName, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open sorted file %s: %v", sdxFileName, err)
	}
	defer sdxFile.Close()

	sortedEntries, err := readIndexFileEntries(sdxFile)
	if err != nil {
		t.Fatalf("Failed to read sorted entries from %s: %v", sdxFileName, err)
	}

	// Build final state map from original entries (same logic as readNeedleMap)
	finalStateMap := make(map[NeedleId]needle_map.NeedleValue)
	for _, entry := range originalEntries {
		if !entry.Offset.IsZero() && !entry.Size.IsDeleted() {
			finalStateMap[entry.Key] = entry
		} else {
			delete(finalStateMap, entry.Key)
		}
	}

	// Convert map to slice for comparison
	validOriginalEntries := make([]needle_map.NeedleValue, 0, len(finalStateMap))
	for _, entry := range finalStateMap {
		validOriginalEntries = append(validOriginalEntries, entry)
	}

	// Verify the number of valid entries matches
	if len(validOriginalEntries) != len(sortedEntries) {
		t.Fatalf("Valid entry count mismatch: original=%d, sorted=%d", len(validOriginalEntries), len(sortedEntries))
	}

	// Verify all valid entries are present
	err = verifyAllEntriesPresent(validOriginalEntries, sortedEntries)
	if err != nil {
		t.Fatalf("Data verification failed: %v", err)
	}

	// Verify the sorted file is actually sorted
	err = verifySortedOrder(sortedEntries)
	if err != nil {
		t.Fatalf("Sorted order verification failed: %v", err)
	}

	// Test binary search functionality
	err = testBinarySearch(t, sdxFile, sortedEntries)
	if err != nil {
		t.Fatalf("Binary search test failed: %v", err)
	}

	t.Logf("Successfully verified sorted file generation with deleted entries for %s: %d valid entries out of %d total", baseFileName, len(sortedEntries), len(originalEntries))
}

func TestSortedFileGenerationWith5ByteOffset(t *testing.T) {
	// Create a test idx file with 5-byte offset entries
	tempDir := t.TempDir()
	baseFileName := filepath.Join(tempDir, "test_5bytes")

	// Create test data with both 4-byte and 5-byte compatible entries
	testEntries := []needle_map.NeedleValue{
		{Key: NeedleId(1), Offset: ToOffset(1024), Size: Size(100)},
		{Key: NeedleId(5), Offset: ToOffset(2048), Size: Size(200)},
		{Key: NeedleId(3), Offset: ToOffset(4096), Size: Size(300)},
		{Key: NeedleId(7), Offset: ToOffset(8192), Size: Size(400)},
		{Key: NeedleId(2), Offset: ToOffset(16384), Size: Size(500)},
	}

	// Create the test idx file
	err := createTestIdxFile(baseFileName+".idx", testEntries)
	if err != nil {
		t.Fatalf("Failed to create test idx file: %v", err)
	}

	// Test sorted file generation
	testSortedFileGeneration(t, baseFileName)
}

func TestSortedFileGenerationWithLargeOffsets(t *testing.T) {
	// Test with very large offsets that would require 5-byte offset support
	tempDir := t.TempDir()
	baseFileName := filepath.Join(tempDir, "test_large_offsets")

	// Create test data with large offsets
	testEntries := []needle_map.NeedleValue{
		{Key: NeedleId(1), Offset: ToOffset(1024 * 1024 * 1024), Size: Size(100)},             // 1GB
		{Key: NeedleId(5), Offset: ToOffset(2 * 1024 * 1024 * 1024), Size: Size(200)},         // 2GB
		{Key: NeedleId(3), Offset: ToOffset(int64(5) * 1024 * 1024 * 1024), Size: Size(300)},  // 5GB
		{Key: NeedleId(7), Offset: ToOffset(int64(10) * 1024 * 1024 * 1024), Size: Size(400)}, // 10GB
		{Key: NeedleId(2), Offset: ToOffset(int64(20) * 1024 * 1024 * 1024), Size: Size(500)}, // 20GB
	}

	// Create the test idx file
	err := createTestIdxFile(baseFileName+".idx", testEntries)
	if err != nil {
		t.Fatalf("Failed to create test idx file with large offsets: %v", err)
	}

	// Test sorted file generation
	testSortedFileGeneration(t, baseFileName)

	t.Logf("Successfully tested sorted file generation with large offsets up to 20GB")
}

func TestSortedFileGenerationCornerCases(t *testing.T) {
	tempDir := t.TempDir()

	// Test with empty file
	t.Run("EmptyFile", func(t *testing.T) {
		baseFileName := filepath.Join(tempDir, "empty")
		err := createTestIdxFile(baseFileName+".idx", []needle_map.NeedleValue{})
		if err != nil {
			t.Fatalf("Failed to create empty idx file: %v", err)
		}
		testSortedFileGeneration(t, baseFileName)
	})

	// Test with single entry
	t.Run("SingleEntry", func(t *testing.T) {
		baseFileName := filepath.Join(tempDir, "single")
		entries := []needle_map.NeedleValue{
			{Key: NeedleId(42), Offset: ToOffset(1024), Size: Size(100)},
		}
		err := createTestIdxFile(baseFileName+".idx", entries)
		if err != nil {
			t.Fatalf("Failed to create single entry idx file: %v", err)
		}
		testSortedFileGeneration(t, baseFileName)
	})

	// Test with deleted entries
	t.Run("WithDeletedEntries", func(t *testing.T) {
		baseFileName := filepath.Join(tempDir, "deleted")
		entries := []needle_map.NeedleValue{
			{Key: NeedleId(1), Offset: ToOffset(1024), Size: Size(100)},
			{Key: NeedleId(2), Offset: Offset{}, Size: TombstoneFileSize}, // Deleted
			{Key: NeedleId(3), Offset: ToOffset(2048), Size: Size(200)},
		}
		err := createTestIdxFile(baseFileName+".idx", entries)
		if err != nil {
			t.Fatalf("Failed to create idx file with deleted entries: %v", err)
		}
		testSortedFileGenerationWithDeletedEntries(t, baseFileName)
	})
}

func TestSortedFileNeedleMapIntegration(t *testing.T) {
	// Test with the provided test data files
	testFiles := []string{
		"../../test/data/iceberg_173",
		"../../test/data/iceberg_674",
	}

	for _, baseFileName := range testFiles {
		t.Run(fmt.Sprintf("TestSortedFileNeedleMapIntegration_%s", filepath.Base(baseFileName)), func(t *testing.T) {
			testSortedFileNeedleMapIntegration(t, baseFileName)
		})
	}

	// Also test with valid synthetic data
	t.Run("SyntheticData", func(t *testing.T) {
		tempDir := t.TempDir()
		baseFileName := filepath.Join(tempDir, "synthetic")

		// Create test data with various scenarios
		testEntries := []needle_map.NeedleValue{
			{Key: NeedleId(1), Offset: ToOffset(1024), Size: Size(100)},
			{Key: NeedleId(5), Offset: ToOffset(2048), Size: Size(200)},
			{Key: NeedleId(3), Offset: ToOffset(4096), Size: Size(300)},
			{Key: NeedleId(7), Offset: ToOffset(8192), Size: Size(400)},
			{Key: NeedleId(2), Offset: ToOffset(16384), Size: Size(500)},
			{Key: NeedleId(10), Offset: Offset{}, Size: TombstoneFileSize}, // Deleted
			{Key: NeedleId(8), Offset: ToOffset(32768), Size: Size(600)},
		}

		err := createTestIdxFile(baseFileName+".idx", testEntries)
		if err != nil {
			t.Fatalf("Failed to create synthetic test idx file: %v", err)
		}

		testSortedFileNeedleMapIntegration(t, baseFileName)
	})
}

func TestSortedFileNeedleMapIntegration5ByteMode(t *testing.T) {
	// Test specifically for 5-byte mode functionality with large offsets
	tempDir := t.TempDir()
	baseFileName := filepath.Join(tempDir, "test_5byte_integration")

	// Create test data with large offsets that benefit from 5-byte offset support
	testEntries := []needle_map.NeedleValue{
		{Key: NeedleId(1), Offset: ToOffset(1024), Size: Size(100)},
		{Key: NeedleId(100), Offset: ToOffset(int64(5) * 1024 * 1024 * 1024), Size: Size(200)}, // 5GB offset
		{Key: NeedleId(50), Offset: ToOffset(2048), Size: Size(150)},
		{Key: NeedleId(200), Offset: ToOffset(int64(10) * 1024 * 1024 * 1024), Size: Size(300)}, // 10GB offset
		{Key: NeedleId(25), Offset: ToOffset(4096), Size: Size(250)},
		{Key: NeedleId(75), Offset: Offset{}, Size: TombstoneFileSize},                         // Deleted
		{Key: NeedleId(150), Offset: ToOffset(int64(2) * 1024 * 1024 * 1024), Size: Size(400)}, // 2GB offset
	}

	err := createTestIdxFile(baseFileName+".idx", testEntries)
	if err != nil {
		t.Fatalf("Failed to create 5-byte test idx file: %v", err)
	}

	// The test should work regardless of whether we're in 4-byte or 5-byte mode
	testSortedFileNeedleMapIntegration(t, baseFileName)

	// Additional verification: ensure large offsets are handled correctly
	idxFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open test idx file: %v", err)
	}
	defer idxFile.Close()

	sortedMap, err := NewSortedFileNeedleMap(baseFileName, idxFile)
	if err != nil {
		t.Fatalf("Failed to create SortedFileNeedleMap for large offset test: %v", err)
	}
	defer sortedMap.Close()

	// Test specific large offset entries
	largeOffsetTests := []struct {
		key    NeedleId
		offset Offset
		size   Size
	}{
		{NeedleId(100), ToOffset(int64(5) * 1024 * 1024 * 1024), Size(200)},
		{NeedleId(200), ToOffset(int64(10) * 1024 * 1024 * 1024), Size(300)},
		{NeedleId(150), ToOffset(int64(2) * 1024 * 1024 * 1024), Size(400)},
	}

	for _, test := range largeOffsetTests {
		value, ok := sortedMap.Get(test.key)
		if !ok {
			t.Errorf("Failed to get needle %d with large offset from sorted map", test.key)
			continue
		}
		if value.Offset != test.offset || value.Size != test.size {
			t.Errorf("Large offset needle %d mismatch: expected (%s, %d), got (%s, %d)",
				test.key, test.offset.String(), test.size, value.Offset.String(), value.Size)
		}
	}

	t.Logf("Successfully verified large offset handling in current mode (NeedleMapEntrySize: %d)", NeedleMapEntrySize)
}

func testSortedFileNeedleMapIntegration(t *testing.T, baseFileName string) {
	// Open the original idx file
	idxFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open %s.idx: %v", baseFileName, err)
	}
	defer idxFile.Close()

	// Check if the file has a valid size before proceeding
	stat, err := idxFile.Stat()
	if err != nil {
		t.Fatalf("Failed to stat idx file: %v", err)
	}

	// Skip test files with real data when running in incompatible mode
	// The test data files iceberg_173.idx and iceberg_674.idx were created with 5-byte offsets
	if strings.Contains(baseFileName, "iceberg_") {
		if NeedleMapEntrySize == 16 {
			t.Skipf("Skipping real test data file %s: created with 5-byte format but running in 4-byte mode", baseFileName)
			return
		}
	}

	if stat.Size()%int64(NeedleMapEntrySize) != 0 {
		// Check if it would be compatible with the other entry size
		otherEntrySize := 16
		if NeedleMapEntrySize == 16 {
			otherEntrySize = 17 // 5-byte mode
		} else {
			otherEntrySize = 16 // 4-byte mode
		}

		if stat.Size()%int64(otherEntrySize) == 0 {
			t.Skipf("Skipping test for %s: file size %d is compatible with %d-byte entries but current mode uses %d-byte entries",
				baseFileName, stat.Size(), otherEntrySize, NeedleMapEntrySize)
		} else {
			t.Skipf("Skipping test for %s: file size %d is not a multiple of any known NeedleMapEntrySize (current: %d, alternative: %d)",
				baseFileName, stat.Size(), NeedleMapEntrySize, otherEntrySize)
		}
		return
	}

	// Create SortedFileNeedleMap which will generate the sorted file
	sortedMap, err := NewSortedFileNeedleMap(baseFileName, idxFile)
	if err != nil {
		t.Fatalf("Failed to create SortedFileNeedleMap: %v", err)
	}
	defer sortedMap.Close()

	// Read original entries for verification
	originalEntries, err := readIndexFileEntries(idxFile)
	if err != nil {
		t.Fatalf("Failed to read original entries: %v", err)
	}

	// Build final state map from original entries (same logic as readNeedleMap)
	finalStateMap := make(map[NeedleId]needle_map.NeedleValue)
	for _, entry := range originalEntries {
		if !entry.Offset.IsZero() && !entry.Size.IsDeleted() {
			finalStateMap[entry.Key] = entry
		} else {
			delete(finalStateMap, entry.Key)
		}
	}

	// Test Get operations for all final valid entries
	validEntries := 0
	for _, entry := range finalStateMap {
		validEntries++
		value, ok := sortedMap.Get(entry.Key)
		if !ok {
			t.Errorf("Failed to get needle %d from sorted map", entry.Key)
			continue
		}
		if value.Key != entry.Key || value.Offset != entry.Offset || value.Size != entry.Size {
			t.Errorf("Needle %d mismatch: expected (%d, %s, %d), got (%d, %s, %d)",
				entry.Key, entry.Key, entry.Offset.String(), entry.Size,
				value.Key, value.Offset.String(), value.Size)
		}
	}

	t.Logf("Successfully verified %d valid entries out of %d total entries", validEntries, len(originalEntries))
}

// Helper functions

func readIndexFileEntries(file *os.File) ([]needle_map.NeedleValue, error) {
	var entries []needle_map.NeedleValue

	err := idx.WalkIndexFile(file, 0, func(key NeedleId, offset Offset, size Size) error {
		entries = append(entries, needle_map.NeedleValue{
			Key:    key,
			Offset: offset,
			Size:   size,
		})
		return nil
	})

	return entries, err
}

func verifyAllEntriesPresent(original, sorted []needle_map.NeedleValue) error {
	// Create maps for efficient lookup
	originalMap := make(map[NeedleId]needle_map.NeedleValue)
	sortedMap := make(map[NeedleId]needle_map.NeedleValue)

	for _, entry := range original {
		originalMap[entry.Key] = entry
	}

	for _, entry := range sorted {
		sortedMap[entry.Key] = entry
	}

	// Verify each original entry is present in sorted
	for key, originalEntry := range originalMap {
		sortedEntry, exists := sortedMap[key]
		if !exists {
			return fmt.Errorf("entry with key %d missing in sorted file", key)
		}
		if originalEntry.Offset != sortedEntry.Offset || originalEntry.Size != sortedEntry.Size {
			return fmt.Errorf("entry %d data mismatch: original(%s, %d) vs sorted(%s, %d)",
				key, originalEntry.Offset.String(), originalEntry.Size,
				sortedEntry.Offset.String(), sortedEntry.Size)
		}
	}

	return nil
}

func verifySortedOrder(entries []needle_map.NeedleValue) error {
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Key >= entries[i].Key {
			return fmt.Errorf("entries not sorted: entry[%d].Key=%d >= entry[%d].Key=%d",
				i-1, entries[i-1].Key, i, entries[i].Key)
		}
	}
	return nil
}

func testBinarySearch(t *testing.T, sdxFile *os.File, sortedEntries []needle_map.NeedleValue) error {
	stat, err := sdxFile.Stat()
	if err != nil {
		return err
	}

	// Test binary search with existing keys
	for i := 0; i < len(sortedEntries) && i < 10; i++ { // Test first 10 entries
		key := sortedEntries[i].Key
		offset, size, err := erasure_coding.SearchNeedleFromSortedIndex(sdxFile, stat.Size(), key, nil)
		if err != nil {
			return fmt.Errorf("binary search failed for key %d: %v", key, err)
		}
		if offset != sortedEntries[i].Offset || size != sortedEntries[i].Size {
			return fmt.Errorf("binary search returned wrong values for key %d: expected (%s, %d), got (%s, %d)",
				key, sortedEntries[i].Offset.String(), sortedEntries[i].Size,
				offset.String(), size)
		}
	}

	// Test binary search with non-existent keys
	nonExistentKey := NeedleId(999999999)
	_, _, err = erasure_coding.SearchNeedleFromSortedIndex(sdxFile, stat.Size(), nonExistentKey, nil)
	if err != erasure_coding.NotFoundError {
		return fmt.Errorf("expected NotFoundError for non-existent key, got: %v", err)
	}

	return nil
}

func createTestIdxFile(filename string, entries []needle_map.NeedleValue) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, entry := range entries {
		bytes := entry.ToBytes()
		if _, err := file.Write(bytes); err != nil {
			return err
		}
	}

	return nil
}

func BenchmarkSortedFileGeneration(b *testing.B) {
	testFiles := []string{
		"../../test/data/iceberg_173",
		"../../test/data/iceberg_674",
	}

	for _, baseFileName := range testFiles {
		b.Run(fmt.Sprintf("BenchmarkSortedFileGeneration_%s", filepath.Base(baseFileName)), func(b *testing.B) {
			// Check if idx file exists before running benchmark
			if _, err := os.Stat(baseFileName + ".idx"); os.IsNotExist(err) {
				b.Skipf("Idx file does not exist: %s.idx", baseFileName)
				return
			}

			// Cleanup at the end of benchmark
			defer func() {
				os.Remove(baseFileName + ".sdx")
			}()

			for i := 0; i < b.N; i++ {
				sdxFileName := baseFileName + ".sdx"
				// Remove existing file if any
				os.Remove(sdxFileName)

				b.StartTimer()
				err := erasure_coding.WriteSortedFileFromIdx(baseFileName, ".sdx")
				b.StopTimer()

				if err != nil {
					// Clean up before failing
					os.Remove(sdxFileName)
					b.Fatalf("Failed to generate sorted file: %v", err)
				}

				// Clean up immediately after timing
				os.Remove(sdxFileName)
			}
		})
	}

	// Also benchmark with synthetic data for consistent results
	b.Run("SyntheticData", func(b *testing.B) {
		tempDir := b.TempDir()
		baseFileName := filepath.Join(tempDir, "bench")

		// Create test data for benchmarking
		testEntries := make([]needle_map.NeedleValue, 1000)
		for i := 0; i < 1000; i++ {
			testEntries[i] = needle_map.NeedleValue{
				Key:    NeedleId(i + 1),
				Offset: ToOffset(int64((i + 1) * 1024)),
				Size:   Size(100 + i%900),
			}
		}

		err := createTestIdxFile(baseFileName+".idx", testEntries)
		if err != nil {
			b.Fatalf("Failed to create benchmark idx file: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sdxFileName := baseFileName + ".sdx"
			os.Remove(sdxFileName)

			b.StartTimer()
			err := erasure_coding.WriteSortedFileFromIdx(baseFileName, ".sdx")
			b.StopTimer()

			if err != nil {
				b.Fatalf("Failed to generate sorted file: %v", err)
			}

			os.Remove(sdxFileName)
		}
	})
}

func TestSortedFileGenerationConcurrency(t *testing.T) {
	// Test concurrent sorted file generation
	tempDir := t.TempDir()

	// Create test data
	testEntries := []needle_map.NeedleValue{
		{Key: NeedleId(1), Offset: ToOffset(1024), Size: Size(100)},
		{Key: NeedleId(5), Offset: ToOffset(2048), Size: Size(200)},
		{Key: NeedleId(3), Offset: ToOffset(4096), Size: Size(300)},
		{Key: NeedleId(7), Offset: ToOffset(8192), Size: Size(400)},
		{Key: NeedleId(2), Offset: ToOffset(16384), Size: Size(500)},
	}

	baseFileName := filepath.Join(tempDir, "concurrent")
	err := createTestIdxFile(baseFileName+".idx", testEntries)
	if err != nil {
		t.Fatalf("Failed to create test idx file: %v", err)
	}

	// Test concurrent access to the same source file
	const numGoroutines = 5
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			concurrentBaseFileName := filepath.Join(tempDir, fmt.Sprintf("concurrent_%d", id))
			err := createTestIdxFile(concurrentBaseFileName+".idx", testEntries)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: failed to create idx file: %v", id, err)
				return
			}

			// Generate sorted file
			err = erasure_coding.WriteSortedFileFromIdx(concurrentBaseFileName, ".sdx")
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: failed to generate sorted file: %v", id, err)
				return
			}

			// Verify the sorted file
			sdxFile, err := os.OpenFile(concurrentBaseFileName+".sdx", os.O_RDONLY, 0644)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: failed to open sorted file: %v", id, err)
				return
			}
			defer sdxFile.Close()

			sortedEntries, err := readIndexFileEntries(sdxFile)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: failed to read sorted entries: %v", id, err)
				return
			}

			if len(sortedEntries) != len(testEntries) {
				errChan <- fmt.Errorf("goroutine %d: entry count mismatch", id)
				return
			}

			// Verify sorted order
			err = verifySortedOrder(sortedEntries)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: sorted order verification failed: %v", id, err)
				return
			}

			errChan <- nil
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("Successfully tested concurrent sorted file generation with %d goroutines", numGoroutines)
}

func TestSortedFileErrorHandling(t *testing.T) {
	tempDir := t.TempDir()

	// Test with non-existent idx file
	t.Run("NonExistentFile", func(t *testing.T) {
		baseFileName := filepath.Join(tempDir, "nonexistent")
		err := erasure_coding.WriteSortedFileFromIdx(baseFileName, ".sdx")
		if err == nil {
			t.Fatal("Expected error for non-existent file, got nil")
		}
	})

	// Test with empty idx file
	t.Run("EmptyIdxFile", func(t *testing.T) {
		baseFileName := filepath.Join(tempDir, "empty")
		// Create an empty idx file
		file, err := os.Create(baseFileName + ".idx")
		if err != nil {
			t.Fatalf("Failed to create empty idx file: %v", err)
		}
		file.Close()

		// Should succeed with empty file
		err = erasure_coding.WriteSortedFileFromIdx(baseFileName, ".sdx")
		if err != nil {
			t.Fatalf("Unexpected error for empty file: %v", err)
		}

		// Verify empty sorted file was created
		sdxFile, err := os.OpenFile(baseFileName+".sdx", os.O_RDONLY, 0644)
		if err != nil {
			t.Fatalf("Failed to open empty sorted file: %v", err)
		}
		defer sdxFile.Close()

		stat, err := sdxFile.Stat()
		if err != nil {
			t.Fatalf("Failed to stat empty sorted file: %v", err)
		}

		if stat.Size() != 0 {
			t.Fatalf("Expected empty sorted file, got size %d", stat.Size())
		}

		// Clean up
		os.Remove(baseFileName + ".sdx")
	})
}
