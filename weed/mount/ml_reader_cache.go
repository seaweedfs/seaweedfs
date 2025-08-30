package mount

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// MLReaderCache is an enhanced reader cache with ML-aware prefetching capabilities
type MLReaderCache struct {
	// Embed the existing reader cache
	*filer.ReaderCache
	
	// ML-specific components
	prefetchManager    *PrefetchManager
	patternDetector    *AccessPatternDetector
	
	// Configuration
	enableMLPrefetch   bool
	maxPrefetchAhead   int    // Maximum chunks to prefetch ahead
	prefetchBatchSize  int    // Number of chunks to prefetch in one batch
	
	// Metrics
	prefetchHits       int64
	prefetchMisses     int64
	mlPrefetchCount    int64
}

// NewMLReaderCache creates a new ML-aware reader cache
func NewMLReaderCache(limit int, chunkCache chunk_cache.ChunkCache, lookupFileIdFn wdclient.LookupFileIdFunctionType) *MLReaderCache {
	baseCache := filer.NewReaderCache(limit, chunkCache, lookupFileIdFn)
	
	mlCache := &MLReaderCache{
		ReaderCache:       baseCache,
		prefetchManager:   NewPrefetchManager(8, 100, 30*time.Second), // 8 workers for prefetch
		patternDetector:   NewAccessPatternDetector(),
		enableMLPrefetch:  true,
		maxPrefetchAhead:  8,  // Prefetch up to 8 chunks ahead
		prefetchBatchSize: 3,  // Prefetch 3 chunks at a time
	}
	
	// Start cleanup goroutine
	go mlCache.cleanupWorker()
	
	glog.V(1).Infof("MLReaderCache initialized with prefetching enabled")
	return mlCache
}

// ReadChunkAt reads a chunk and triggers ML-aware prefetching
func (mlc *MLReaderCache) ReadChunkAt(buffer []byte, inode uint64, fileId string, cipherKey []byte, isGzipped bool, offset int64, chunkSize int, shouldCache bool) (int, error) {
	// Record access for pattern detection
	accessInfo := mlc.patternDetector.RecordAccess(inode, offset, len(buffer))
	
	// Use the base reader cache for the actual read
	n, err := mlc.ReaderCache.ReadChunkAt(buffer, fileId, cipherKey, isGzipped, offset, chunkSize, shouldCache)
	
	// Trigger ML-aware prefetching if enabled
	if mlc.enableMLPrefetch && err == nil {
		mlc.triggerMLPrefetch(inode, fileId, cipherKey, isGzipped, offset, chunkSize, accessInfo)
	}
	
	return n, err
}

// triggerMLPrefetch triggers prefetching based on detected access patterns
func (mlc *MLReaderCache) triggerMLPrefetch(inode uint64, fileId string, cipherKey []byte, isGzipped bool, currentOffset int64, chunkSize int, accessInfo *AccessInfo) {
	shouldPrefetch, prefetchSize := mlc.patternDetector.ShouldPrefetch(inode)
	if !shouldPrefetch {
		return
	}
	
	// Calculate which chunks to prefetch based on access pattern
	chunksToPrefetech := mlc.calculatePrefetchChunks(accessInfo, currentOffset, chunkSize, prefetchSize)
	
	if len(chunksToPrefetech) == 0 {
		return
	}
	
	glog.V(4).Infof("Triggering ML prefetch for inode %d: pattern=%s, chunks=%d", 
		inode, accessInfo.Pattern, len(chunksToPrefetech))
	
	// Submit prefetch requests
	for _, chunkInfo := range chunksToPrefetech {
		mlc.prefetchChunk(chunkInfo.FileId, chunkInfo.ChunkIndex, chunkInfo.Offset, chunkInfo.Size, cipherKey, isGzipped)
	}
	
	mlc.mlPrefetchCount++
}

// PrefetchChunkInfo contains information about a chunk to prefetch
type PrefetchChunkInfo struct {
	FileId     string
	ChunkIndex uint32
	Offset     uint64
	Size       uint64
}

// calculatePrefetchChunks determines which chunks should be prefetched
func (mlc *MLReaderCache) calculatePrefetchChunks(accessInfo *AccessInfo, currentOffset int64, chunkSize int, prefetchSize int64) []PrefetchChunkInfo {
	var chunks []PrefetchChunkInfo
	
	currentChunkIndex := uint32(currentOffset / int64(chunkSize))
	chunksToFetch := minInt(mlc.maxPrefetchAhead, int(prefetchSize/int64(chunkSize))+1)
	
	switch accessInfo.Pattern {
	case SequentialAccess:
		// For sequential access, prefetch the next N chunks
		for i := 1; i <= chunksToFetch; i++ {
			chunkIndex := currentChunkIndex + uint32(i)
			chunks = append(chunks, PrefetchChunkInfo{
				FileId:     mlc.generateChunkFileId(chunkIndex), // This would need to be implemented
				ChunkIndex: chunkIndex,
				Offset:     uint64((int64(chunkIndex) * int64(chunkSize))),
				Size:       uint64(chunkSize),
			})
		}
		
	case ModelAccess:
		// For model access, prefetch more aggressively
		chunksToFetch = minInt(mlc.maxPrefetchAhead*2, int(prefetchSize/int64(chunkSize))+1)
		for i := 1; i <= chunksToFetch; i++ {
			chunkIndex := currentChunkIndex + uint32(i)
			chunks = append(chunks, PrefetchChunkInfo{
				FileId:     mlc.generateChunkFileId(chunkIndex),
				ChunkIndex: chunkIndex,
				Offset:     uint64(int64(chunkIndex) * int64(chunkSize)),
				Size:       uint64(chunkSize),
			})
		}
		
	case EpochAccess:
		// For epoch access, prefetch the beginning of the file
		if currentOffset < int64(chunkSize)*4 { // Only if we're near the beginning
			for i := 1; i <= minInt(chunksToFetch, 4); i++ {
				chunkIndex := uint32(i)
				chunks = append(chunks, PrefetchChunkInfo{
					FileId:     mlc.generateChunkFileId(chunkIndex),
					ChunkIndex: chunkIndex,
					Offset:     uint64(int64(chunkIndex) * int64(chunkSize)),
					Size:       uint64(chunkSize),
				})
			}
		}
		
	case StridedAccess:
		// For strided access, try to predict the next stride
		// This is a simplified implementation
		nextOffset := currentOffset + int64(accessInfo.PrefetchSize)
		nextChunkIndex := uint32(nextOffset / int64(chunkSize))
		if nextChunkIndex > currentChunkIndex {
			chunks = append(chunks, PrefetchChunkInfo{
				FileId:     mlc.generateChunkFileId(nextChunkIndex),
				ChunkIndex: nextChunkIndex,
				Offset:     uint64(nextOffset),
				Size:       uint64(chunkSize),
			})
		}
	}
	
	// Limit the total number of chunks to prefetch
	if len(chunks) > mlc.prefetchBatchSize {
		chunks = chunks[:mlc.prefetchBatchSize]
	}
	
	return chunks
}

// prefetchChunk submits a chunk for prefetching
func (mlc *MLReaderCache) prefetchChunk(fileId string, chunkIndex uint32, offset, size uint64, cipherKey []byte, isGzipped bool) {
	ctx := context.Background()
	
	// Create callback to handle prefetch completion
	callback := func(data []byte, err error) {
		if err != nil {
			glog.V(4).Infof("Prefetch failed for chunk %s[%d]: %v", fileId, chunkIndex, err)
			mlc.prefetchMisses++
		} else {
			glog.V(4).Infof("Prefetch completed for chunk %s[%d]: %d bytes", fileId, chunkIndex, len(data))
			mlc.prefetchHits++
			
			// TODO: Store the prefetched data in cache
			// This would integrate with the existing chunk cache
		}
	}
	
	// Submit to prefetch manager with priority based on access pattern
	priority := mlc.calculatePrefetchPriority(chunkIndex)
	success := mlc.prefetchManager.Prefetch(ctx, fileId, chunkIndex, offset, size, priority, callback)
	
	if !success {
		glog.V(4).Infof("Failed to queue prefetch for chunk %s[%d]", fileId, chunkIndex)
	}
}

// calculatePrefetchPriority calculates priority for prefetch requests
func (mlc *MLReaderCache) calculatePrefetchPriority(chunkIndex uint32) int {
	// Lower numbers = higher priority
	// Prioritize chunks that are closer to current read position
	return int(chunkIndex % 10) // Simple priority based on chunk index
}

// generateChunkFileId generates a file ID for a specific chunk
// TODO: This needs to be implemented based on SeaweedFS chunk naming scheme
func (mlc *MLReaderCache) generateChunkFileId(chunkIndex uint32) string {
	// This is a placeholder implementation
	// In real implementation, this would generate the actual chunk file ID
	// based on the file's chunk layout
	return "chunk_" + string(rune(chunkIndex))
}

// EnableMLPrefetch enables or disables ML-aware prefetching
func (mlc *MLReaderCache) EnableMLPrefetch(enabled bool) {
	mlc.enableMLPrefetch = enabled
	glog.V(2).Infof("ML prefetching %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// SetPrefetchConfiguration sets prefetch configuration parameters
func (mlc *MLReaderCache) SetPrefetchConfiguration(maxAhead, batchSize int) {
	mlc.maxPrefetchAhead = maxAhead
	mlc.prefetchBatchSize = batchSize
	glog.V(2).Infof("ML prefetch config: maxAhead=%d, batchSize=%d", maxAhead, batchSize)
}

// GetMLMetrics returns ML-specific caching metrics
func (mlc *MLReaderCache) GetMLMetrics() MLCacheMetrics {
	prefetchMetrics := mlc.prefetchManager.GetMetrics()
	patternMetrics := mlc.patternDetector.GetMetrics()
	
	return MLCacheMetrics{
		PrefetchHits:         mlc.prefetchHits,
		PrefetchMisses:       mlc.prefetchMisses,
		MLPrefetchTriggered:  mlc.mlPrefetchCount,
		PrefetchMetrics:      prefetchMetrics,
		PatternMetrics:       patternMetrics,
		EnableMLPrefetch:     mlc.enableMLPrefetch,
	}
}

// MLCacheMetrics holds comprehensive ML cache metrics
type MLCacheMetrics struct {
	PrefetchHits         int64
	PrefetchMisses       int64
	MLPrefetchTriggered  int64
	PrefetchMetrics      PrefetchMetrics
	PatternMetrics       AccessPatternMetrics
	EnableMLPrefetch     bool
}

// cleanupWorker periodically cleans up old access pattern entries
func (mlc *MLReaderCache) cleanupWorker() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// Clean up access patterns older than 1 hour
			mlc.patternDetector.CleanupOldEntries(1 * time.Hour)
		}
	}
}

// Shutdown gracefully shuts down the ML reader cache
func (mlc *MLReaderCache) Shutdown() {
	glog.V(1).Infof("Shutting down MLReaderCache...")
	
	if mlc.prefetchManager != nil {
		mlc.prefetchManager.Shutdown()
	}
	
	// Print final metrics
	metrics := mlc.GetMLMetrics()
	glog.V(1).Infof("MLReaderCache final metrics: hits=%d, misses=%d, ml_prefetch=%d", 
		metrics.PrefetchHits, metrics.PrefetchMisses, metrics.MLPrefetchTriggered)
}

// Helper function
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
