package ml

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// MLOptimization provides ML-aware optimizations for FUSE mounting
type MLOptimization struct {
	ReaderCache     *MLReaderCache
	PrefetchManager *PrefetchManager
	PatternDetector *AccessPatternDetector
	enabled         bool
}

// MLConfig holds configuration for ML optimizations
type MLConfig struct {
	// Prefetch configuration
	PrefetchWorkers   int           // Number of prefetch workers
	PrefetchQueueSize int           // Size of prefetch queue
	PrefetchTimeout   time.Duration // Timeout for prefetch operations
	
	// Pattern detection configuration
	EnableMLHeuristics    bool    // Enable ML-specific pattern detection
	SequentialThreshold   int     // Minimum consecutive reads for sequential detection
	ConfidenceThreshold   float64 // Minimum confidence to trigger prefetch
	
	// Cache configuration
	MaxPrefetchAhead  int // Maximum chunks to prefetch ahead
	PrefetchBatchSize int // Number of chunks to prefetch in one batch
}

// DefaultMLConfig returns default configuration optimized for ML workloads
func DefaultMLConfig() *MLConfig {
	return &MLConfig{
		// Prefetch settings
		PrefetchWorkers:   8,
		PrefetchQueueSize: 100,
		PrefetchTimeout:   30 * time.Second,
		
		// Pattern detection settings
		EnableMLHeuristics:  true,
		SequentialThreshold: 3,
		ConfidenceThreshold: 0.6,
		
		// Cache settings
		MaxPrefetchAhead:  8,
		PrefetchBatchSize: 3,
	}
}

// NewMLOptimization creates a new ML optimization instance
func NewMLOptimization(config *MLConfig, chunkCache chunk_cache.ChunkCache, lookupFn wdclient.LookupFileIdFunctionType) *MLOptimization {
	if config == nil {
		config = DefaultMLConfig()
	}
	
	// Create ML reader cache with embedded prefetch manager and pattern detector
	mlReaderCache := NewMLReaderCache(10, chunkCache, lookupFn)
	
	// Configure the ML reader cache with provided settings
	mlReaderCache.SetPrefetchConfiguration(config.MaxPrefetchAhead, config.PrefetchBatchSize)
	
	opt := &MLOptimization{
		ReaderCache:     mlReaderCache,
		PrefetchManager: mlReaderCache.prefetchManager,
		PatternDetector: mlReaderCache.patternDetector,
		enabled:         true,
	}
	
	glog.V(1).Infof("ML optimization enabled with config: workers=%d, queue=%d, confidence=%.2f", 
		config.PrefetchWorkers, config.PrefetchQueueSize, config.ConfidenceThreshold)
	
	return opt
}

// Enable enables or disables ML optimization
func (opt *MLOptimization) Enable(enabled bool) {
	opt.enabled = enabled
	if opt.ReaderCache != nil {
		opt.ReaderCache.EnableMLPrefetch(enabled)
	}
	glog.V(2).Infof("ML optimization %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// IsEnabled returns whether ML optimization is enabled
func (opt *MLOptimization) IsEnabled() bool {
	return opt.enabled
}

// GetMetrics returns comprehensive ML optimization metrics
func (opt *MLOptimization) GetMetrics() *MLOptimizationMetrics {
	if opt.ReaderCache == nil {
		return &MLOptimizationMetrics{}
	}
	
	mlMetrics := opt.ReaderCache.GetMLMetrics()
	
	return &MLOptimizationMetrics{
		Enabled:              opt.enabled,
		PrefetchHits:         mlMetrics.PrefetchHits,
		PrefetchMisses:       mlMetrics.PrefetchMisses,
		MLPrefetchTriggered:  mlMetrics.MLPrefetchTriggered,
		TotalAccesses:        mlMetrics.PatternMetrics.TotalAccesses,
		SequentialReads:      mlMetrics.PatternMetrics.SequentialReads,
		RandomReads:          mlMetrics.PatternMetrics.RandomReads,
		PatternCounts:        mlMetrics.PatternMetrics.PatternCounts,
		ActivePrefetchJobs:   mlMetrics.PrefetchMetrics.ActiveJobs,
		PrefetchWorkers:      mlMetrics.PrefetchMetrics.Workers,
	}
}

// MLOptimizationMetrics holds comprehensive metrics for ML optimization
type MLOptimizationMetrics struct {
	Enabled              bool                     `json:"enabled"`
	PrefetchHits         int64                    `json:"prefetch_hits"`
	PrefetchMisses       int64                    `json:"prefetch_misses"`
	MLPrefetchTriggered  int64                    `json:"ml_prefetch_triggered"`
	TotalAccesses        int64                    `json:"total_accesses"`
	SequentialReads      int64                    `json:"sequential_reads"`
	RandomReads          int64                    `json:"random_reads"`
	PatternCounts        map[AccessPattern]int    `json:"pattern_counts"`
	ActivePrefetchJobs   int64                    `json:"active_prefetch_jobs"`
	PrefetchWorkers      int64                    `json:"prefetch_workers"`
}

// Shutdown gracefully shuts down all ML optimization components
func (opt *MLOptimization) Shutdown() {
	if opt.ReaderCache != nil {
		opt.ReaderCache.Shutdown()
	}
	glog.V(1).Infof("ML optimization shutdown complete")
}

// RecordAccess records a file access for pattern detection (convenience method)
func (opt *MLOptimization) RecordAccess(inode uint64, offset int64, size int) *AccessInfo {
	if !opt.enabled || opt.PatternDetector == nil {
		return nil
	}
	return opt.PatternDetector.RecordAccess(inode, offset, size)
}

// ShouldPrefetch determines if prefetching should be triggered (convenience method)
func (opt *MLOptimization) ShouldPrefetch(inode uint64) (bool, int64) {
	if !opt.enabled || opt.PatternDetector == nil {
		return false, 0
	}
	return opt.PatternDetector.ShouldPrefetch(inode)
}
