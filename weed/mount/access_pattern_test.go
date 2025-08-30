package mount

import (
	"testing"
	"time"
)

func TestAccessPatternDetector_Sequential(t *testing.T) {
	apd := NewAccessPatternDetector()

	inode := uint64(1)
	
	// Simulate sequential access pattern
	info1 := apd.RecordAccess(inode, 0, 1024)
	if info1.Pattern != RandomAccess {
		t.Error("First access should be detected as random")
	}

	info2 := apd.RecordAccess(inode, 1024, 1024)
	if info2.ConsecutiveSeq != 1 {
		t.Error("Second sequential access should increment counter")
	}

	info3 := apd.RecordAccess(inode, 2048, 1024)
	if info3.ConsecutiveSeq != 2 {
		t.Error("Third sequential access should increment counter")
	}

	info4 := apd.RecordAccess(inode, 3072, 1024)
	if info4.Pattern != SequentialAccess {
		t.Errorf("After %d sequential accesses, pattern should be Sequential, got: %v", 
			apd.sequentialThreshold+1, info4.Pattern)
	}
	
	if info4.PrefetchSize <= 0 {
		t.Error("Sequential access should set prefetch size")
	}
	
	shouldPrefetch, prefetchSize := apd.ShouldPrefetch(inode)
	if !shouldPrefetch {
		t.Error("Should recommend prefetch for sequential access")
	}
	
	if prefetchSize != info4.PrefetchSize {
		t.Errorf("Prefetch size mismatch: expected %d, got %d", info4.PrefetchSize, prefetchSize)
	}
}

func TestAccessPatternDetector_Random(t *testing.T) {
	apd := NewAccessPatternDetector()

	inode := uint64(2)
	
	// Simulate random access pattern
	offsets := []int64{0, 5000, 1000, 10000, 2000}
	
	for _, offset := range offsets {
		info := apd.RecordAccess(inode, offset, 1024)
		if info.ConsecutiveSeq > 0 && info != apd.fileInfo[inode] {
			// Reset should happen on non-sequential access
			t.Error("Sequential counter should reset on random access")
		}
	}
	
	finalInfo := apd.fileInfo[inode]
	if finalInfo.Pattern != RandomAccess {
		t.Errorf("Pattern should remain RandomAccess, got: %v", finalInfo.Pattern)
	}
	
	shouldPrefetch, _ := apd.ShouldPrefetch(inode)
	if shouldPrefetch {
		t.Error("Should not recommend prefetch for random access")
	}
}

func TestAccessPatternDetector_ModelAccess(t *testing.T) {
	apd := NewAccessPatternDetector()

	inode := uint64(3)
	
	// Simulate model file loading (large sequential reads)
	largeSize := 2 * 1024 * 1024 // 2MB
	
	apd.RecordAccess(inode, 0, largeSize)
	apd.RecordAccess(inode, int64(largeSize), largeSize)
	apd.RecordAccess(inode, int64(largeSize*2), largeSize)
	
	info := apd.RecordAccess(inode, int64(largeSize*3), largeSize)
	
	if info.Pattern != ModelAccess {
		t.Errorf("Large sequential reads should be detected as ModelAccess, got: %v", info.Pattern)
	}
	
	if info.Confidence < 0.9 {
		t.Errorf("Model access should have high confidence, got: %.2f", info.Confidence)
	}
	
	shouldPrefetch, prefetchSize := apd.ShouldPrefetch(inode)
	if !shouldPrefetch {
		t.Error("Should recommend prefetch for model access")
	}
	
	if prefetchSize < 4*1024*1024 { // Should be at least 4MB for models
		t.Errorf("Model access should have large prefetch size, got: %d", prefetchSize)
	}
}

func TestAccessPatternDetector_EpochAccess(t *testing.T) {
	apd := NewAccessPatternDetector()

	inode := uint64(4)
	
	// Simulate many accesses first
	for i := 0; i < 150; i++ {
		apd.RecordAccess(inode, int64(i*1024), 1024)
	}
	
	// Simulate gap (sleep not needed, just update last access time)
	info := apd.fileInfo[inode]
	info.LastAccessTime = time.Now().Add(-2 * time.Minute)
	
	// Access from beginning again (epoch restart)
	epochInfo := apd.RecordAccess(inode, 0, 1024)
	
	if epochInfo.Pattern != EpochAccess {
		t.Errorf("Restart from beginning should be detected as EpochAccess, got: %v", epochInfo.Pattern)
	}
	
	shouldPrefetch, prefetchSize := apd.ShouldPrefetch(inode)
	if !shouldPrefetch {
		t.Error("Should recommend prefetch for epoch access")
	}
	
	if prefetchSize < 256*1024 { // Should have reasonable prefetch size
		t.Errorf("Epoch access should have decent prefetch size, got: %d", prefetchSize)
	}
}

func TestAccessPatternDetector_StridedAccess(t *testing.T) {
	apd := NewAccessPatternDetector()

	inode := uint64(5)
	
	// Simulate strided access (e.g., reading every nth byte for image processing)
	stride := int64(4096)
	
	apd.RecordAccess(inode, 0, 1024)
	apd.RecordAccess(inode, 1024+stride, 1024) // Gap between reads
	apd.RecordAccess(inode, 2048+stride*2, 1024)
	info := apd.RecordAccess(inode, 3072+stride*3, 1024)
	
	// Note: Current simple implementation may not detect complex stride patterns
	// This test validates the structure is in place
	t.Logf("Strided access pattern: %v (confidence: %.2f)", info.Pattern, info.Confidence)
}

func TestAccessPatternDetector_PatternTransition(t *testing.T) {
	apd := NewAccessPatternDetector()

	inode := uint64(6)
	
	// Start with sequential
	apd.RecordAccess(inode, 0, 1024)
	apd.RecordAccess(inode, 1024, 1024)
	apd.RecordAccess(inode, 2048, 1024)
	info := apd.RecordAccess(inode, 3072, 1024)
	
	if info.Pattern != SequentialAccess {
		t.Error("Should detect sequential pattern")
	}
	
	// Break with random access
	randomInfo := apd.RecordAccess(inode, 10000, 1024)
	
	if randomInfo.Pattern != RandomAccess {
		t.Errorf("Pattern should transition to RandomAccess after break, got: %v", randomInfo.Pattern)
	}
	
	if randomInfo.PrefetchSize != 0 {
		t.Error("Prefetch size should be reset after pattern break")
	}
}

func TestAccessPatternDetector_MultipleFiles(t *testing.T) {
	apd := NewAccessPatternDetector()

	// Test tracking multiple files simultaneously
	file1 := uint64(10)
	file2 := uint64(20)
	
	// File 1: Sequential pattern
	apd.RecordAccess(file1, 0, 1024)
	apd.RecordAccess(file1, 1024, 1024)
	apd.RecordAccess(file1, 2048, 1024)
	seq_info := apd.RecordAccess(file1, 3072, 1024)
	
	// File 2: Random pattern
	apd.RecordAccess(file2, 5000, 1024)
	apd.RecordAccess(file2, 1000, 1024)
	random_info := apd.RecordAccess(file2, 8000, 1024)
	
	if seq_info.Pattern != SequentialAccess {
		t.Error("File 1 should maintain sequential pattern")
	}
	
	if random_info.Pattern != RandomAccess {
		t.Error("File 2 should maintain random pattern")
	}
	
	// Verify independent tracking
	pattern1 := apd.GetPattern(file1)
	pattern2 := apd.GetPattern(file2)
	
	if pattern1 != SequentialAccess || pattern2 != RandomAccess {
		t.Error("Files should maintain independent patterns")
	}
}

func TestAccessPatternDetector_Metrics(t *testing.T) {
	apd := NewAccessPatternDetector()

	// Generate some access patterns
	file1 := uint64(100)
	file2 := uint64(200)
	
	// Sequential accesses for file1
	for i := 0; i < 5; i++ {
		apd.RecordAccess(file1, int64(i*1024), 1024)
	}
	
	// Random accesses for file2
	offsets := []int64{0, 5000, 1000, 10000}
	for _, offset := range offsets {
		apd.RecordAccess(file2, offset, 1024)
	}
	
	metrics := apd.GetMetrics()
	
	if metrics.TotalAccesses != 9 {
		t.Errorf("Expected 9 total accesses, got: %d", metrics.TotalAccesses)
	}
	
	if metrics.TotalFiles != 2 {
		t.Errorf("Expected 2 files, got: %d", metrics.TotalFiles)
	}
	
	if metrics.PatternCounts[SequentialAccess] != 1 {
		t.Errorf("Expected 1 sequential file, got: %d", metrics.PatternCounts[SequentialAccess])
	}
	
	if metrics.PatternCounts[RandomAccess] != 1 {
		t.Errorf("Expected 1 random file, got: %d", metrics.PatternCounts[RandomAccess])
	}
}

func TestAccessPatternDetector_Cleanup(t *testing.T) {
	apd := NewAccessPatternDetector()

	inode := uint64(999)
	
	// Create an access record
	apd.RecordAccess(inode, 0, 1024)
	
	// Verify it exists
	if len(apd.fileInfo) != 1 {
		t.Error("Should have one file info entry")
	}
	
	// Set old timestamp
	info := apd.fileInfo[inode]
	info.LastAccessTime = time.Now().Add(-2 * time.Hour)
	
	// Cleanup old entries
	apd.CleanupOldEntries(1 * time.Hour)
	
	if len(apd.fileInfo) != 0 {
		t.Error("Old entry should have been cleaned up")
	}
}

func TestAccessPatternDetector_Confidence(t *testing.T) {
	apd := NewAccessPatternDetector()
	apd.confidenceThreshold = 0.8 // High threshold for testing

	inode := uint64(888)
	
	// Start sequential access but don't reach high confidence
	apd.RecordAccess(inode, 0, 1024)
	apd.RecordAccess(inode, 1024, 1024)
	apd.RecordAccess(inode, 2048, 1024)
	info := apd.RecordAccess(inode, 3072, 1024)
	
	// Should be sequential but low confidence
	if info.Pattern != SequentialAccess {
		t.Error("Should detect sequential pattern")
	}
	
	if info.Confidence >= 0.8 {
		t.Errorf("Early sequential detection should have low confidence, got: %.2f", info.Confidence)
	}
	
	// Should not recommend prefetch due to low confidence
	shouldPrefetch, _ := apd.ShouldPrefetch(inode)
	if shouldPrefetch {
		t.Error("Should not prefetch with low confidence")
	}
	
	// Continue sequential access to build confidence
	for i := 4; i < 8; i++ {
		apd.RecordAccess(inode, int64(i*1024), 1024)
	}
	
	// Now should have high confidence
	highConfInfo := apd.fileInfo[inode]
	if highConfInfo.Confidence < 0.8 {
		t.Errorf("Extended sequential access should have high confidence, got: %.2f", highConfInfo.Confidence)
	}
	
	shouldPrefetch, _ = apd.ShouldPrefetch(inode)
	if !shouldPrefetch {
		t.Error("Should prefetch with high confidence")
	}
}

// Benchmark tests

func BenchmarkAccessPatternDetector_RecordAccess(b *testing.B) {
	apd := NewAccessPatternDetector()

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		inode := uint64(i % 100) // Cycle through 100 different files
		offset := int64(i * 1024)
		apd.RecordAccess(inode, offset, 1024)
	}
}

func BenchmarkAccessPatternDetector_ShouldPrefetch(b *testing.B) {
	apd := NewAccessPatternDetector()

	// Setup some files with different patterns
	for i := 0; i < 100; i++ {
		inode := uint64(i)
		// Create sequential pattern
		for j := 0; j < 5; j++ {
			apd.RecordAccess(inode, int64(j*1024), 1024)
		}
	}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		inode := uint64(i % 100)
		apd.ShouldPrefetch(inode)
	}
}
