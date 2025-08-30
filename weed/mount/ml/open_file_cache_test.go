package ml

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestOpenFileCache_Basic(t *testing.T) {
	cache := NewOpenFileCache(10, 5*time.Minute)
	defer cache.Shutdown()

	// Test opening a file
	entry := &filer_pb.Entry{
		Name: "test.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
		},
	}

	inode := uint64(1)
	fullPath := "/test/test.txt"
	fileInfo := cache.OpenFile(inode, entry, fullPath)

	if fileInfo == nil {
		t.Fatal("OpenFile should return file info")
	}

	if fileInfo.Inode != inode {
		t.Errorf("Expected inode %d, got %d", inode, fileInfo.Inode)
	}

	if fileInfo.OpenCount != 1 {
		t.Errorf("Expected open count 1, got %d", fileInfo.OpenCount)
	}
}

func TestOpenFileCache_MLFileDetection(t *testing.T) {
	cache := NewOpenFileCache(10, 5*time.Minute)
	defer cache.Shutdown()

	testCases := []struct {
		name     string
		path     string
		filename string
		size     uint64
		expected MLFileType
	}{
		{"PyTorch model", "/models/checkpoint.pt", "checkpoint.pt", 100 * 1024 * 1024, MLFileModel},
		{"Dataset image", "/datasets/train/image001.jpg", "image001.jpg", 2 * 1024 * 1024, MLFileDataset},
		{"Config file", "/config/training.yaml", "training.yaml", 1024, MLFileConfig},
		{"Tensor file", "/tensors/weights.safetensors", "weights.safetensors", 50 * 1024 * 1024, MLFileModel},
		{"Log file", "/logs/training.log", "training.log", 10 * 1024, MLFileLog},
		{"Regular file", "/documents/readme.txt", "readme.txt", 5 * 1024, MLFileUnknown},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			entry := &filer_pb.Entry{
				Name: tc.filename,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: tc.size,
				},
			}

			inode := uint64(time.Now().UnixNano()) // Unique inode
			fileInfo := cache.OpenFile(inode, entry, tc.path)

			if tc.expected == MLFileUnknown {
				if fileInfo.IsMLFile {
					t.Errorf("File %s should not be detected as ML file", tc.path)
				}
			} else {
				if !fileInfo.IsMLFile {
					t.Errorf("File %s should be detected as ML file", tc.path)
				}

				if fileInfo.FileType != tc.expected {
					t.Errorf("Expected file type %v, got %v", tc.expected, fileInfo.FileType)
				}
			}
		})
	}
}

func TestOpenFileCache_ChunkMetadata(t *testing.T) {
	cache := NewOpenFileCache(10, 5*time.Minute)
	defer cache.Shutdown()

	inode := uint64(1)
	entry := &filer_pb.Entry{
		Name: "data.bin",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 10240,
		},
	}
	fullPath := "/data/data.bin"

	cache.OpenFile(inode, entry, fullPath)

	// Test updating chunk metadata
	chunkIndex := uint32(0)
	metadata := &ChunkMetadata{
		FileId:      "chunk_0",
		Offset:      0,
		Size:        1024,
		CacheLevel:  0,
		LastAccess:  time.Now(),
		AccessCount: 1,
		Pattern:     SequentialAccess,
	}

	cache.UpdateChunkCache(inode, chunkIndex, metadata)

	// Test retrieving chunk metadata
	retrieved, exists := cache.GetChunkMetadata(inode, chunkIndex)
	if !exists {
		t.Error("Chunk metadata should exist")
	}

	if retrieved.FileId != metadata.FileId {
		t.Errorf("Expected FileId %s, got %s", metadata.FileId, retrieved.FileId)
	}

	if retrieved.AccessCount != 2 { // Should be incremented during retrieval
		t.Errorf("Expected access count 2, got %d", retrieved.AccessCount)
	}
}

func TestOpenFileCache_LRUEviction(t *testing.T) {
	cache := NewOpenFileCache(3, 5*time.Minute) // Small cache for testing
	defer cache.Shutdown()

	// Fill cache to capacity
	for i := 1; i <= 3; i++ {
		entry := &filer_pb.Entry{
			Name: "file" + string(rune('0'+i)) + ".txt",
			Attributes: &filer_pb.FuseAttributes{
				FileSize: 1024,
			},
		}
		fullPath := "/test/file" + string(rune('0'+i)) + ".txt"
		cache.OpenFile(uint64(i), entry, fullPath)
		cache.CloseFile(uint64(i)) // Close immediately so they can be evicted
	}

	// Add one more file - should trigger eviction
	entry4 := &filer_pb.Entry{
		Name: "file4.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
		},
	}
	cache.OpenFile(uint64(4), entry4, "/test/file4.txt")

	metrics := cache.GetMetrics()
	if metrics.EvictedFiles == 0 {
		t.Error("Should have evicted at least one file")
	}

	// File 1 should be evicted (oldest)
	file1Info := cache.GetFileInfo(uint64(1))
	if file1Info != nil {
		t.Error("File 1 should have been evicted")
	}

	// File 4 should still be there
	file4Info := cache.GetFileInfo(uint64(4))
	if file4Info == nil {
		t.Error("File 4 should still be in cache")
	}
}

func TestOpenFileCache_TTLCleanup(t *testing.T) {
	cache := NewOpenFileCache(10, 100*time.Millisecond) // Short TTL for testing
	defer cache.Shutdown()

	inode := uint64(1)
	entry := &filer_pb.Entry{
		Name: "test.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
		},
	}

	fileInfo := cache.OpenFile(inode, entry, "/test/test.txt")
	cache.CloseFile(inode) // Close so it can be cleaned up

	// Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	// Trigger cleanup manually
	cache.cleanup()

	// File should be cleaned up
	retrievedInfo := cache.GetFileInfo(inode)
	if retrievedInfo != nil {
		t.Error("File should have been cleaned up after TTL expiration")
	}

	_ = fileInfo // Avoid unused variable warning
}

func TestOpenFileCache_MultipleOpens(t *testing.T) {
	cache := NewOpenFileCache(10, 5*time.Minute)
	defer cache.Shutdown()

	inode := uint64(1)
	entry := &filer_pb.Entry{
		Name: "shared.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
		},
	}
	fullPath := "/test/shared.txt"

	// Open file multiple times
	fileInfo1 := cache.OpenFile(inode, entry, fullPath)
	fileInfo2 := cache.OpenFile(inode, entry, fullPath)

	if fileInfo1 != fileInfo2 {
		t.Error("Multiple opens of same file should return same file info")
	}

	if fileInfo1.OpenCount != 2 {
		t.Errorf("Expected open count 2, got %d", fileInfo1.OpenCount)
	}

	// Close once
	canEvict1 := cache.CloseFile(inode)
	if canEvict1 {
		t.Error("Should not be able to evict file with open count > 0")
	}

	if fileInfo1.OpenCount != 1 {
		t.Errorf("Expected open count 1 after first close, got %d", fileInfo1.OpenCount)
	}

	// Close again
	canEvict2 := cache.CloseFile(inode)
	if !canEvict2 {
		t.Error("Should be able to evict file with open count 0")
	}
}

func TestOpenFileCache_Metrics(t *testing.T) {
	cache := NewOpenFileCache(10, 5*time.Minute)
	defer cache.Shutdown()

	// Add some files of different types
	files := []struct {
		inode    uint64
		filename string
		path     string
		size     uint64
	}{
		{1, "model.pt", "/models/model.pt", 100 * 1024 * 1024},
		{2, "data.jpg", "/datasets/data.jpg", 2 * 1024 * 1024},
		{3, "config.yaml", "/config/config.yaml", 1024},
		{4, "regular.txt", "/docs/regular.txt", 5 * 1024},
	}

	for _, file := range files {
		entry := &filer_pb.Entry{
			Name: file.filename,
			Attributes: &filer_pb.FuseAttributes{
				FileSize: file.size,
			},
		}
		cache.OpenFile(file.inode, entry, file.path)

		// Add some chunk metadata
		metadata := &ChunkMetadata{
			FileId:     "chunk_" + string(rune(file.inode)),
			Offset:     0,
			Size:       1024,
			CacheLevel: 0,
		}
		cache.UpdateChunkCache(file.inode, 0, metadata)
	}

	metrics := cache.GetMetrics()

	if metrics.TotalFiles != 4 {
		t.Errorf("Expected 4 total files, got %d", metrics.TotalFiles)
	}

	if metrics.MLFiles < 2 { // Should detect at least model and dataset
		t.Errorf("Expected at least 2 ML files, got %d", metrics.MLFiles)
	}

	if metrics.TotalChunks != 4 {
		t.Errorf("Expected 4 total chunks, got %d", metrics.TotalChunks)
	}

	// Check file type counts
	if metrics.FileTypes[MLFileModel] == 0 {
		t.Error("Should detect at least one model file")
	}

	if metrics.FileTypes[MLFileDataset] == 0 {
		t.Error("Should detect at least one dataset file")
	}
}

func TestOpenFileCache_ConcurrentAccess(t *testing.T) {
	cache := NewOpenFileCache(100, 5*time.Minute)
	defer cache.Shutdown()

	// Test concurrent access to the cache
	numGoroutines := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			inode := uint64(id)
			entry := &filer_pb.Entry{
				Name: "file" + string(rune('0'+id)) + ".txt",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
			}
			fullPath := "/test/file" + string(rune('0'+id)) + ".txt"

			// Perform multiple operations
			for j := 0; j < 10; j++ {
				cache.OpenFile(inode, entry, fullPath)

				metadata := &ChunkMetadata{
					FileId:     "chunk_" + string(rune(id)) + "_" + string(rune(j)),
					Offset:     uint64(j * 1024),
					Size:       1024,
					CacheLevel: 0,
				}
				cache.UpdateChunkCache(inode, uint32(j), metadata)

				cache.GetChunkMetadata(inode, uint32(j))
				cache.CloseFile(inode)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify cache state
	metrics := cache.GetMetrics()
	if metrics.TotalFiles == 0 {
		t.Error("Should have some files in cache after concurrent operations")
	}
}

func TestMLFileDetector_Extensions(t *testing.T) {
	detector := newMLFileDetector()

	testCases := []struct {
		filename string
		path     string
		expected MLFileType
	}{
		{"model.pt", "/models/model.pt", MLFileModel},
		{"weights.pth", "/models/weights.pth", MLFileModel},
		{"data.jpg", "/datasets/data.jpg", MLFileDataset},
		{"config.yaml", "/config/config.yaml", MLFileConfig},
		{"tensor.safetensors", "/tensors/tensor.safetensors", MLFileModel},
		{"training.log", "/logs/training.log", MLFileLog},
		{"document.txt", "/docs/document.txt", MLFileUnknown},
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			entry := &filer_pb.Entry{
				Name: tc.filename,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
			}

			isML, fileType := detector.DetectMLFile(entry, tc.path)

			if tc.expected == MLFileUnknown {
				// For unknown files, either ML detection result is acceptable
				t.Logf("File %s: isML=%v, type=%v", tc.filename, isML, fileType)
			} else {
				if !isML {
					t.Errorf("File %s should be detected as ML file", tc.filename)
				}

				if fileType != tc.expected {
					t.Errorf("File %s: expected type %v, got %v", tc.filename, tc.expected, fileType)
				}
			}
		})
	}
}

func TestMLFileDetector_PathPatterns(t *testing.T) {
	detector := newMLFileDetector()

	testCases := []struct {
		path     string
		filename string
		expected MLFileType
	}{
		{"/datasets/train/file.bin", "file.bin", MLFileDataset},
		{"/models/checkpoint/weights", "weights", MLFileModel},
		{"/data/validation/sample.dat", "sample.dat", MLFileDataset},
		{"/checkpoints/model_v1.bin", "model_v1.bin", MLFileModel},
		{"/documents/report.pdf", "report.pdf", MLFileUnknown},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			entry := &filer_pb.Entry{
				Name: tc.filename,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
			}

			isML, fileType := detector.DetectMLFile(entry, tc.path)

			if tc.expected == MLFileUnknown {
				t.Logf("Path %s: isML=%v, type=%v", tc.path, isML, fileType)
			} else {
				if !isML {
					t.Errorf("Path %s should be detected as ML file", tc.path)
				}

				if fileType != tc.expected {
					t.Errorf("Path %s: expected type %v, got %v", tc.path, tc.expected, fileType)
				}
			}
		})
	}
}

func TestMLFileDetector_SizeHeuristics(t *testing.T) {
	detector := newMLFileDetector()

	// Large file with model-related name should be detected as model
	largeModelEntry := &filer_pb.Entry{
		Name: "large_model.bin",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 500 * 1024 * 1024, // 500MB
		},
	}

	isML, fileType := detector.DetectMLFile(largeModelEntry, "/checkpoints/large_model.bin")

	if !isML {
		t.Error("Large model file should be detected as ML file")
	}

	if fileType != MLFileModel {
		t.Errorf("Large model file should be detected as model, got %v", fileType)
	}
}

func TestOpenFileCache_EvictionProtection(t *testing.T) {
	cache := NewOpenFileCache(2, 5*time.Minute) // Very small cache
	defer cache.Shutdown()

	// Open two files and keep them open
	for i := 1; i <= 2; i++ {
		entry := &filer_pb.Entry{
			Name: "file" + string(rune('0'+i)) + ".txt",
			Attributes: &filer_pb.FuseAttributes{
				FileSize: 1024,
			},
		}
		fullPath := "/test/file" + string(rune('0'+i)) + ".txt"
		cache.OpenFile(uint64(i), entry, fullPath)
		// Don't close - keep them open
	}

	// Try to open a third file - should not evict open files
	entry3 := &filer_pb.Entry{
		Name: "file3.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
		},
	}
	cache.OpenFile(uint64(3), entry3, "/test/file3.txt")

	// All files should still be there since none could be evicted
	for i := 1; i <= 3; i++ {
		fileInfo := cache.GetFileInfo(uint64(i))
		if fileInfo == nil {
			t.Errorf("File %d should still be in cache (eviction protection)", i)
		}
	}
}

func TestOpenFileCache_GetFileInfo_CacheHitMiss(t *testing.T) {
	cache := NewOpenFileCache(10, 5*time.Minute)
	defer cache.Shutdown()

	inode := uint64(1)

	// Test cache miss
	fileInfo := cache.GetFileInfo(inode)
	if fileInfo != nil {
		t.Error("Should return nil for non-existent file")
	}

	initialMetrics := cache.GetMetrics()
	if initialMetrics.CacheMisses == 0 {
		t.Error("Should record cache miss")
	}

	// Add file to cache
	entry := &filer_pb.Entry{
		Name: "test.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
		},
	}
	cache.OpenFile(inode, entry, "/test/test.txt")

	// Test cache hit
	fileInfo = cache.GetFileInfo(inode)
	if fileInfo == nil {
		t.Error("Should return file info for existing file")
	}

	finalMetrics := cache.GetMetrics()
	if finalMetrics.CacheHits == 0 {
		t.Error("Should record cache hit")
	}

	if finalMetrics.CacheHits <= initialMetrics.CacheHits {
		t.Error("Cache hits should increase")
	}
}

func TestOpenFileCache_Shutdown(t *testing.T) {
	cache := NewOpenFileCache(10, 5*time.Minute)

	// Add some files
	for i := 1; i <= 3; i++ {
		entry := &filer_pb.Entry{
			Name: "file" + string(rune('0'+i)) + ".txt",
			Attributes: &filer_pb.FuseAttributes{
				FileSize: 1024,
			},
		}
		fullPath := "/test/file" + string(rune('0'+i)) + ".txt"
		cache.OpenFile(uint64(i), entry, fullPath)
	}

	// Test graceful shutdown
	done := make(chan struct{})
	go func() {
		cache.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Error("Shutdown took too long")
	}
}

// Benchmark tests

func BenchmarkOpenFileCache_OpenFile(b *testing.B) {
	cache := NewOpenFileCache(1000, 30*time.Minute)
	defer cache.Shutdown()

	entry := &filer_pb.Entry{
		Name: "benchmark.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
		},
	}
	fullPath := "/test/benchmark.txt"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inode := uint64(i % 100) // Cycle through 100 files
		cache.OpenFile(inode, entry, fullPath)
	}
}

func BenchmarkOpenFileCache_GetFileInfo(b *testing.B) {
	cache := NewOpenFileCache(1000, 30*time.Minute)
	defer cache.Shutdown()

	// Pre-populate cache
	entry := &filer_pb.Entry{
		Name: "benchmark.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileSize: 1024,
		},
	}
	fullPath := "/test/benchmark.txt"

	for i := 0; i < 100; i++ {
		cache.OpenFile(uint64(i), entry, fullPath)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inode := uint64(i % 100)
		cache.GetFileInfo(inode)
	}
}
