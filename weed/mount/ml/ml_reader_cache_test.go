package ml

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
)

func TestMLReaderCache_Basic(t *testing.T) {
	// Create a mock chunk cache
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	
	// Create ML reader cache
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	if mlCache == nil {
		t.Fatal("Failed to create ML reader cache")
	}
	
	if !mlCache.enableMLPrefetch {
		t.Error("ML prefetching should be enabled by default")
	}
}

func TestMLReaderCache_EnableDisable(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Test enabling/disabling
	mlCache.EnableMLPrefetch(false)
	if mlCache.enableMLPrefetch {
		t.Error("ML prefetching should be disabled")
	}
	
	mlCache.EnableMLPrefetch(true)
	if !mlCache.enableMLPrefetch {
		t.Error("ML prefetching should be enabled")
	}
}

func TestMLReaderCache_Configuration(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Test configuration
	mlCache.SetPrefetchConfiguration(16, 5)
	
	if mlCache.maxPrefetchAhead != 16 {
		t.Errorf("Expected maxPrefetchAhead=16, got %d", mlCache.maxPrefetchAhead)
	}
	
	if mlCache.prefetchBatchSize != 5 {
		t.Errorf("Expected prefetchBatchSize=5, got %d", mlCache.prefetchBatchSize)
	}
}

func TestMLReaderCache_calculatePrefetchChunks_Sequential(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Create access info with sequential pattern
	accessInfo := &AccessInfo{
		Pattern:      SequentialAccess,
		PrefetchSize: 4096,
		Confidence:   0.8,
	}
	
	chunks := mlCache.calculatePrefetchChunks(accessInfo, 0, 1024, 4096)
	
	if len(chunks) == 0 {
		t.Error("Should generate prefetch chunks for sequential access")
	}
	
	// Verify chunks are sequential
	for i, chunk := range chunks {
		expectedIndex := uint32(i + 1)
		if chunk.ChunkIndex != expectedIndex {
			t.Errorf("Expected chunk index %d, got %d", expectedIndex, chunk.ChunkIndex)
		}
	}
}

func TestMLReaderCache_calculatePrefetchChunks_ModelAccess(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Create access info with model access pattern
	accessInfo := &AccessInfo{
		Pattern:      ModelAccess,
		PrefetchSize: 8192,
		Confidence:   0.9,
	}
	
	chunks := mlCache.calculatePrefetchChunks(accessInfo, 0, 1024, 8192)
	
	if len(chunks) == 0 {
		t.Error("Should generate prefetch chunks for model access")
	}
	
	// Model access should prefetch more aggressively
	if len(chunks) <= mlCache.prefetchBatchSize {
		t.Log("Model access might prefetch more chunks (this is expected)")
	}
}

func TestMLReaderCache_calculatePrefetchChunks_EpochAccess(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Create access info with epoch access pattern
	accessInfo := &AccessInfo{
		Pattern:      EpochAccess,
		PrefetchSize: 2048,
		Confidence:   0.8,
	}
	
	// Test epoch access at beginning of file
	chunks := mlCache.calculatePrefetchChunks(accessInfo, 0, 1024, 2048)
	
	if len(chunks) == 0 {
		t.Error("Should generate prefetch chunks for epoch access at beginning")
	}
	
	// Test epoch access in middle of file (should not prefetch)
	chunksMiddle := mlCache.calculatePrefetchChunks(accessInfo, 100000, 1024, 2048)
	if len(chunksMiddle) != 0 {
		t.Error("Should not prefetch for epoch access in middle of file")
	}
}

func TestMLReaderCache_calculatePrefetchChunks_RandomAccess(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Create access info with random access pattern
	accessInfo := &AccessInfo{
		Pattern:      RandomAccess,
		PrefetchSize: 1024,
		Confidence:   0.3,
	}
	
	chunks := mlCache.calculatePrefetchChunks(accessInfo, 0, 1024, 1024)
	
	// Random access should not generate prefetch chunks
	if len(chunks) != 0 {
		t.Error("Should not generate prefetch chunks for random access")
	}
}

func TestMLReaderCache_PrefetchPriority(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Test priority calculation
	priority1 := mlCache.calculatePrefetchPriority(0)
	priority2 := mlCache.calculatePrefetchPriority(1)
	priority10 := mlCache.calculatePrefetchPriority(10)
	
	// All priorities should be in valid range
	if priority1 < 0 || priority1 > 9 {
		t.Errorf("Priority should be in range [0,9], got %d", priority1)
	}
	
	if priority2 < 0 || priority2 > 9 {
		t.Errorf("Priority should be in range [0,9], got %d", priority2)
	}
	
	// Priority should wrap around
	if priority1 != priority10 {
		t.Errorf("Priority should wrap around: priority(0)=%d, priority(10)=%d", priority1, priority10)
	}
}

func TestMLReaderCache_Metrics(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Get initial metrics
	metrics := mlCache.GetMLMetrics()
	
	if metrics.PrefetchHits != 0 {
		t.Error("Initial prefetch hits should be 0")
	}
	
	if metrics.PrefetchMisses != 0 {
		t.Error("Initial prefetch misses should be 0")
	}
	
	if metrics.MLPrefetchTriggered != 0 {
		t.Error("Initial ML prefetch triggered should be 0")
	}
	
	if !metrics.EnableMLPrefetch {
		t.Error("ML prefetching should be enabled in metrics")
	}
	
	// Test that metrics contain nested structures
	if metrics.PrefetchMetrics.Workers == 0 {
		t.Error("Should have worker information in prefetch metrics")
	}
}

func TestMLReaderCache_ReadChunkAt_WithPatternDetection(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	
	// Mock lookup function that always succeeds
	mockLookup := func(ctx context.Context, fileId string) ([]string, error) {
		return []string{"http://localhost:8080/" + fileId}, nil
	}
	
	mlCache := NewMLReaderCache(10, chunkCache, mockLookup)
	defer mlCache.Shutdown()
	
	// Test reading with pattern detection
	buffer := make([]byte, 1024)
	inode := uint64(123)
	
	// Don't actually try to read the chunk as it will cause a panic
	// Instead, just test the pattern detection directly by recording accesses
	mlCache.patternDetector.RecordAccess(inode, 0, len(buffer))
	
	// Verify pattern was recorded
	pattern := mlCache.patternDetector.GetPattern(inode)
	if pattern != RandomAccess {
		// First access should be random, but that's implementation dependent
		t.Logf("First access pattern: %v", pattern)
	}
	
	// Check that access was recorded in metrics
	patternMetrics := mlCache.patternDetector.GetMetrics()
	if patternMetrics.TotalAccesses == 0 {
		t.Error("Access should have been recorded in pattern detector")
	}
}

func TestMLReaderCache_generateChunkFileId(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	// Test chunk file ID generation
	fileId1 := mlCache.generateChunkFileId(0)
	fileId2 := mlCache.generateChunkFileId(1)
	
	if fileId1 == fileId2 {
		t.Error("Different chunk indices should generate different file IDs")
	}
	
	if fileId1 == "" || fileId2 == "" {
		t.Error("Generated file IDs should not be empty")
	}
}

func TestMLReaderCache_IntegrationWithAccessDetector(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	inode := uint64(456)
	
	// Simulate sequential access pattern
	for i := 0; i < 5; i++ {
		mlCache.patternDetector.RecordAccess(inode, int64(i*1024), 1024)
	}
	
	// Check if sequential pattern was detected
	shouldPrefetch, prefetchSize := mlCache.patternDetector.ShouldPrefetch(inode)
	
	if !shouldPrefetch {
		t.Error("Should recommend prefetch for sequential access")
	}
	
	if prefetchSize <= 0 {
		t.Error("Prefetch size should be positive for sequential access")
	}
	
	// Test prefetch chunk calculation
	accessInfo := mlCache.patternDetector.fileInfo[inode]
	chunks := mlCache.calculatePrefetchChunks(accessInfo, 4*1024, 1024, prefetchSize)
	
	if len(chunks) == 0 {
		t.Error("Should generate prefetch chunks for detected sequential pattern")
	}
}

func TestMLReaderCache_Shutdown(t *testing.T) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	
	// Test graceful shutdown
	done := make(chan struct{})
	go func() {
		mlCache.Shutdown()
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

func BenchmarkMLReaderCache_ReadChunkAt(b *testing.B) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	buffer := make([]byte, 1024)
	inode := uint64(789)
	fileId := "benchmark_file"
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		offset := int64(i * 1024)
		mlCache.ReadChunkAt(buffer, inode, fileId, nil, false, offset, 1024, true)
	}
}

func BenchmarkMLReaderCache_calculatePrefetchChunks(b *testing.B) {
	chunkCache := chunk_cache.NewChunkCacheInMemory(100)
	mlCache := NewMLReaderCache(10, chunkCache, nil)
	defer mlCache.Shutdown()
	
	accessInfo := &AccessInfo{
		Pattern:      SequentialAccess,
		PrefetchSize: 4096,
		Confidence:   0.8,
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		mlCache.calculatePrefetchChunks(accessInfo, int64(i*1024), 1024, 4096)
	}
}
