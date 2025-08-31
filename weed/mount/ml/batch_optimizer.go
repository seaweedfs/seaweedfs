package ml

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// BatchAccessPattern represents different batch access patterns
type BatchAccessPattern int

const (
	BatchPatternUnknown      BatchAccessPattern = iota
	BatchPatternLinear                          // Linear batch processing
	BatchPatternStrided                         // Strided access with fixed gaps
	BatchPatternShuffled                        // Randomized batch order
	BatchPatternHierarchical                    // Hierarchical/nested batch access
	BatchPatternMultiGPU                        // Multi-GPU distributed batches
	BatchPatternPipelined                       // Pipelined batch processing
)

// BatchAccess represents a single file access that's part of batch processing
type BatchAccess struct {
	Offset     int64     // File offset
	Size       int       // Access size
	AccessTime time.Time // When accessed
	IsRead     bool      // Whether this was a read operation
	BatchHint  string    // Optional batch identifier hint
}

// BatchInfo holds information about a detected batch
type BatchInfo struct {
	sync.RWMutex

	// Batch identification
	BatchID     string // Unique batch identifier
	StartOffset int64  // Starting file offset
	EndOffset   int64  // Ending file offset
	Size        int64  // Total batch size in bytes
	ItemCount   int    // Number of items in batch
	ItemSize    int64  // Average item size

	// Access pattern
	AccessPattern  BatchAccessPattern // Detected access pattern
	AccessOrder    []int64            // Order of access within batch
	AccessTimes    []time.Time        // When each item was accessed
	ProcessingTime time.Duration      // Total time to process batch

	// Performance metrics
	LoadTime    time.Duration // Time to load batch from storage
	ProcessTime time.Duration // Time to process batch (compute)
	TotalTime   time.Duration // Total end-to-end time
	Throughput  float64       // Items per second

	// Optimization state
	IsPrefetched    bool    // Whether batch was prefetched
	CacheHitRate    float64 // Percentage of cache hits
	OptimalPrefetch int64   // Recommended prefetch size

	// Relationship to other batches
	PreviousBatch *BatchInfo   // Previous batch in sequence
	NextBatch     *BatchInfo   // Next batch in sequence
	ParentBatch   *BatchInfo   // Parent batch (for hierarchical)
	ChildBatches  []*BatchInfo // Child batches (for hierarchical)
}

// BatchOptimizer optimizes batch access patterns for ML workloads
type BatchOptimizer struct {
	sync.RWMutex

	// Configuration
	maxBatchesTracked    int   // Maximum number of batches to track
	batchDetectionWindow int   // Window size for batch detection
	minBatchSize         int64 // Minimum size to consider as batch
	maxBatchSize         int64 // Maximum size to consider as batch

	// Batch tracking
	activeBatches    map[string]*BatchInfo   // Currently active batches
	completedBatches map[string]*BatchInfo   // Recently completed batches
	inodeToBatches   map[uint64][]*BatchInfo // File to batches mapping

	// Pattern detection
	accessHistory  map[uint64][]BatchAccess  // Recent access history per file
	batchSequences map[uint64]*BatchSequence // Detected batch sequences

	// Optimization strategies
	prefetchStrategies map[BatchAccessPattern]*PrefetchConfig // Prefetch configs per pattern
	cacheStrategies    map[BatchAccessPattern]*CacheConfig    // Cache configs per pattern

	// Statistics
	totalBatchesDetected int64 // Total batches detected
	optimizationHits     int64 // Successful optimization applications
	optimizationMisses   int64 // Failed optimization attempts

	// Background processing
	cleanupTicker *time.Ticker  // Cleanup timer
	stopCleanup   chan struct{} // Cleanup stop signal
}

// BatchSequence represents a sequence of related batches
type BatchSequence struct {
	sync.RWMutex

	SequenceID  string             // Unique sequence identifier
	Batches     []*BatchInfo       // Batches in sequence
	Pattern     BatchAccessPattern // Overall sequence pattern
	StartTime   time.Time          // When sequence started
	LastAccess  time.Time          // Last access in sequence
	IsComplete  bool               // Whether sequence is complete
	RepeatCount int                // How many times sequence has repeated

	// Predictions
	NextBatchOffset int64   // Predicted next batch offset
	NextBatchSize   int64   // Predicted next batch size
	Confidence      float64 // Confidence in predictions (0-1)
}

// PrefetchConfig holds configuration for prefetching strategies
type PrefetchConfig struct {
	Strategy         PrefetchStrategy // Which prefetch strategy to use
	LookaheadCount   int              // How many items to prefetch ahead
	PrefetchSize     int64            // Size to prefetch per operation
	ConcurrencyLevel int              // How many concurrent prefetch operations
	AdaptiveScaling  bool             // Whether to scale based on performance
}

// CacheConfig holds configuration for caching strategies
type CacheConfig struct {
	Policy         CachePolicy   // Which cache policy to use
	RetentionTime  time.Duration // How long to keep items cached
	Priority       CachePriority // Cache priority level
	PreloadBatches int           // How many batches to preload
}

// NewBatchOptimizer creates a new batch optimizer
func NewBatchOptimizer() *BatchOptimizer {
	bo := &BatchOptimizer{
		maxBatchesTracked:    1000,              // Track up to 1000 batches
		batchDetectionWindow: 100,               // Look at last 100 accesses
		minBatchSize:         64 * 1024,         // Minimum 64KB batch
		maxBatchSize:         100 * 1024 * 1024, // Maximum 100MB batch

		activeBatches:    make(map[string]*BatchInfo),
		completedBatches: make(map[string]*BatchInfo),
		inodeToBatches:   make(map[uint64][]*BatchInfo),
		accessHistory:    make(map[uint64][]BatchAccess),
		batchSequences:   make(map[uint64]*BatchSequence),

		prefetchStrategies: make(map[BatchAccessPattern]*PrefetchConfig),
		cacheStrategies:    make(map[BatchAccessPattern]*CacheConfig),

		stopCleanup: make(chan struct{}),
	}

	// Initialize default strategies
	bo.initializeDefaultStrategies()

	// Start cleanup routine
	bo.cleanupTicker = time.NewTicker(5 * time.Minute)
	go bo.cleanupRoutine()

	glog.V(1).Infof("Batch optimizer initialized")
	return bo
}

// initializeDefaultStrategies sets up default optimization strategies for each pattern
func (bo *BatchOptimizer) initializeDefaultStrategies() {
	// Linear batch pattern - aggressive prefetching
	bo.prefetchStrategies[BatchPatternLinear] = &PrefetchConfig{
		Strategy:         PrefetchAggressive,
		LookaheadCount:   5,
		PrefetchSize:     2 * 1024 * 1024, // 2MB
		ConcurrencyLevel: 3,
		AdaptiveScaling:  true,
	}
	bo.cacheStrategies[BatchPatternLinear] = &CacheConfig{
		Policy:         CachePolicyTrainingAware,
		RetentionTime:  10 * time.Minute,
		Priority:       CachePriorityHigh,
		PreloadBatches: 2,
	}

	// Shuffled batch pattern - conservative prefetching
	bo.prefetchStrategies[BatchPatternShuffled] = &PrefetchConfig{
		Strategy:         PrefetchBalanced,
		LookaheadCount:   2,
		PrefetchSize:     512 * 1024, // 512KB
		ConcurrencyLevel: 2,
		AdaptiveScaling:  true,
	}
	bo.cacheStrategies[BatchPatternShuffled] = &CacheConfig{
		Policy:         CachePolicyLRU,
		RetentionTime:  5 * time.Minute,
		Priority:       CachePriorityNormal,
		PreloadBatches: 1,
	}

	// Multi-GPU pattern - high concurrency
	bo.prefetchStrategies[BatchPatternMultiGPU] = &PrefetchConfig{
		Strategy:         PrefetchAggressive,
		LookaheadCount:   8,
		PrefetchSize:     4 * 1024 * 1024, // 4MB
		ConcurrencyLevel: 6,
		AdaptiveScaling:  true,
	}
	bo.cacheStrategies[BatchPatternMultiGPU] = &CacheConfig{
		Policy:         CachePolicyML,
		RetentionTime:  15 * time.Minute,
		Priority:       CachePriorityUrgent,
		PreloadBatches: 4,
	}
}

// RecordBatchAccess records a file access that's part of batch processing
func (bo *BatchOptimizer) RecordBatchAccess(inode uint64, offset int64, size int, isRead bool, batchHint string) *BatchInfo {
	bo.Lock()
	defer bo.Unlock()

	access := BatchAccess{
		Offset:     offset,
		Size:       size,
		AccessTime: time.Now(),
		IsRead:     isRead,
		BatchHint:  batchHint,
	}

	// Add to access history
	history := bo.accessHistory[inode]
	history = append(history, access)
	if len(history) > bo.batchDetectionWindow {
		history = history[1:] // Keep only recent accesses
	}
	bo.accessHistory[inode] = history

	// Detect batch patterns
	batchInfo := bo.detectBatchPattern(inode, history)
	if batchInfo != nil {
		bo.totalBatchesDetected++

		// Add to tracking
		bo.activeBatches[batchInfo.BatchID] = batchInfo
		bo.inodeToBatches[inode] = append(bo.inodeToBatches[inode], batchInfo)

		// Update batch sequence
		bo.updateBatchSequence(inode, batchInfo)

		glog.V(3).Infof("Detected batch: inode=%d, pattern=%v, size=%d, items=%d",
			inode, batchInfo.AccessPattern, batchInfo.Size, batchInfo.ItemCount)
	}

	return batchInfo
}

// detectBatchPattern analyzes access history to detect batch patterns
func (bo *BatchOptimizer) detectBatchPattern(inode uint64, history []BatchAccess) *BatchInfo {
	if len(history) < 3 {
		return nil // Need minimum history
	}

	// Look for batch boundaries by analyzing access gaps and patterns
	startIdx := len(history) - 10
	if startIdx < 0 {
		startIdx = 0
	}
	recent := history[startIdx:] // Look at last 10 accesses (or all if fewer)
	if len(recent) < 3 {
		recent = history
	}

	// Check for batch characteristics
	batchInfo := bo.analyzePotentialBatch(recent, inode)
	if batchInfo == nil {
		return nil
	}

	// Determine access pattern
	batchInfo.AccessPattern = bo.classifyBatchPattern(batchInfo, recent)

	// Calculate performance metrics
	bo.calculateBatchMetrics(batchInfo, recent)

	return batchInfo
}

// analyzePotentialBatch analyzes a sequence of accesses to see if they form a batch
func (bo *BatchOptimizer) analyzePotentialBatch(accesses []BatchAccess, inode uint64) *BatchInfo {
	if len(accesses) < 2 {
		return nil
	}

	// Calculate basic statistics
	var totalSize int64
	var itemCount int
	minOffset := accesses[0].Offset
	maxOffset := accesses[0].Offset

	accessOrder := make([]int64, len(accesses))
	accessTimes := make([]time.Time, len(accesses))

	for i, access := range accesses {
		totalSize += int64(access.Size)
		itemCount++

		if access.Offset < minOffset {
			minOffset = access.Offset
		}
		if access.Offset > maxOffset {
			maxOffset = access.Offset
		}

		accessOrder[i] = access.Offset
		accessTimes[i] = access.AccessTime
	}

	batchSize := maxOffset - minOffset + int64(accesses[len(accesses)-1].Size)

	// Check if this qualifies as a batch
	if batchSize < bo.minBatchSize || batchSize > bo.maxBatchSize {
		return nil
	}

	// Check temporal locality (accesses should be close in time)
	timeSpan := accessTimes[len(accessTimes)-1].Sub(accessTimes[0])
	if timeSpan > 10*time.Minute { // Too spread out in time
		return nil
	}

	// Create batch info
	batchID := generateBatchID(inode, minOffset, time.Now())

	batchInfo := &BatchInfo{
		BatchID:     batchID,
		StartOffset: minOffset,
		EndOffset:   maxOffset,
		Size:        batchSize,
		ItemCount:   itemCount,
		ItemSize:    totalSize / int64(itemCount),
		AccessOrder: accessOrder,
		AccessTimes: accessTimes,
		TotalTime:   timeSpan,
		LoadTime:    timeSpan, // Initially assume all time is load time
	}

	return batchInfo
}

// classifyBatchPattern determines the access pattern of a batch
func (bo *BatchOptimizer) classifyBatchPattern(batch *BatchInfo, accesses []BatchAccess) BatchAccessPattern {
	if len(batch.AccessOrder) < 2 {
		return BatchPatternUnknown
	}

	// Check for linear pattern (sequential offsets)
	isLinear := true
	for i := 1; i < len(batch.AccessOrder); i++ {
		if batch.AccessOrder[i] <= batch.AccessOrder[i-1] {
			isLinear = false
			break
		}
	}

	if isLinear {
		return BatchPatternLinear
	}

	// Check for strided pattern (regular gaps)
	if bo.isStridedPattern(batch.AccessOrder) {
		return BatchPatternStrided
	}

	// Check for shuffled pattern (randomized order)
	if bo.isShuffledPattern(batch.AccessOrder) {
		return BatchPatternShuffled
	}

	// Check for multi-GPU pattern (parallel access indicators)
	if bo.isMultiGPUPattern(accesses) {
		return BatchPatternMultiGPU
	}

	// Check for pipelined pattern (overlapping accesses)
	if bo.isPipelinedPattern(batch.AccessTimes) {
		return BatchPatternPipelined
	}

	return BatchPatternUnknown
}

// isStridedPattern checks if accesses follow a strided pattern
func (bo *BatchOptimizer) isStridedPattern(offsets []int64) bool {
	if len(offsets) < 3 {
		return false
	}

	// Calculate stride
	stride := offsets[1] - offsets[0]
	if stride <= 0 {
		return false
	}

	// Check if all accesses follow the same stride
	consistentStrides := 0
	for i := 2; i < len(offsets); i++ {
		currentStride := offsets[i] - offsets[i-1]
		if currentStride == stride {
			consistentStrides++
		}
	}

	// At least 80% of strides should be consistent
	return float64(consistentStrides)/float64(len(offsets)-2) >= 0.8
}

// isShuffledPattern checks if accesses are in randomized order
func (bo *BatchOptimizer) isShuffledPattern(offsets []int64) bool {
	if len(offsets) < 5 {
		return false
	}

	// Count inversions (out-of-order pairs)
	inversions := 0
	for i := 0; i < len(offsets); i++ {
		for j := i + 1; j < len(offsets); j++ {
			if offsets[i] > offsets[j] {
				inversions++
			}
		}
	}

	totalPairs := len(offsets) * (len(offsets) - 1) / 2
	inversionRate := float64(inversions) / float64(totalPairs)

	// High inversion rate suggests shuffling
	return inversionRate > 0.3
}

// isMultiGPUPattern checks for multi-GPU access patterns
func (bo *BatchOptimizer) isMultiGPUPattern(accesses []BatchAccess) bool {
	// Look for multiple concurrent access streams
	// This is a simplified heuristic - in practice, this would need more
	// sophisticated detection based on process info, etc.

	if len(accesses) < 4 {
		return false
	}

	// Check for concurrent accesses (multiple accesses in very short time)
	concurrentWindows := 0
	windowSize := 100 * time.Millisecond

	for i := 0; i < len(accesses)-1; i++ {
		timeDiff := accesses[i+1].AccessTime.Sub(accesses[i].AccessTime)
		if timeDiff < windowSize {
			concurrentWindows++
		}
	}

	// If many accesses are concurrent, might be multi-GPU
	return float64(concurrentWindows)/float64(len(accesses)) > 0.5
}

// isPipelinedPattern checks for pipelined access patterns
func (bo *BatchOptimizer) isPipelinedPattern(accessTimes []time.Time) bool {
	if len(accessTimes) < 3 {
		return false
	}

	// Look for regular, overlapping timing patterns
	intervals := make([]time.Duration, len(accessTimes)-1)
	for i := 1; i < len(accessTimes); i++ {
		intervals[i-1] = accessTimes[i].Sub(accessTimes[i-1])
	}

	// Calculate coefficient of variation for intervals
	var sum, sumSq time.Duration
	for _, interval := range intervals {
		sum += interval
		sumSq += interval * interval
	}

	n := time.Duration(len(intervals))
	mean := sum / n
	if mean == 0 {
		return false
	}

	// Calculate variance and CV
	variance := (sumSq / n) - (mean * mean)
	cv := float64(variance) / float64(mean*mean)

	// Low coefficient of variation suggests regular pipelining
	return cv < 0.2
}

// calculateBatchMetrics calculates performance metrics for a batch
func (bo *BatchOptimizer) calculateBatchMetrics(batch *BatchInfo, accesses []BatchAccess) {
	if len(batch.AccessTimes) < 2 {
		return
	}

	// Calculate throughput
	timeSpan := batch.AccessTimes[len(batch.AccessTimes)-1].Sub(batch.AccessTimes[0])
	if timeSpan > 0 {
		batch.Throughput = float64(batch.ItemCount) / timeSpan.Seconds()
	}

	// Estimate processing vs load time (heuristic)
	// In practice, this would need more sophisticated measurement
	avgItemTime := timeSpan / time.Duration(batch.ItemCount)
	batch.ProcessTime = avgItemTime / 2 // Assume 50% processing time
	batch.LoadTime = avgItemTime / 2    // Assume 50% load time
}

// updateBatchSequence updates the batch sequence for an inode
func (bo *BatchOptimizer) updateBatchSequence(inode uint64, newBatch *BatchInfo) {
	sequence := bo.batchSequences[inode]
	if sequence == nil {
		sequence = &BatchSequence{
			SequenceID: generateSequenceID(inode, time.Now()),
			Batches:    make([]*BatchInfo, 0, 10),
			StartTime:  time.Now(),
			Pattern:    newBatch.AccessPattern,
		}
		bo.batchSequences[inode] = sequence
	}

	sequence.Lock()
	defer sequence.Unlock()

	// Link batches
	if len(sequence.Batches) > 0 {
		lastBatch := sequence.Batches[len(sequence.Batches)-1]
		lastBatch.NextBatch = newBatch
		newBatch.PreviousBatch = lastBatch
	}

	sequence.Batches = append(sequence.Batches, newBatch)
	sequence.LastAccess = time.Now()

	// Update sequence pattern based on majority of batches
	bo.updateSequencePattern(sequence)

	// Make predictions for next batch
	bo.updateSequencePredictions(sequence)

	// Keep sequence size manageable
	if len(sequence.Batches) > 100 {
		sequence.Batches = sequence.Batches[len(sequence.Batches)-50:] // Keep last 50 batches
	}
}

// updateSequencePattern updates the overall pattern of a batch sequence
func (bo *BatchOptimizer) updateSequencePattern(sequence *BatchSequence) {
	if len(sequence.Batches) < 3 {
		return
	}

	// Count patterns
	patternCounts := make(map[BatchAccessPattern]int)
	for _, batch := range sequence.Batches {
		patternCounts[batch.AccessPattern]++
	}

	// Find most common pattern
	maxCount := 0
	var dominantPattern BatchAccessPattern
	for pattern, count := range patternCounts {
		if count > maxCount {
			maxCount = count
			dominantPattern = pattern
		}
	}

	sequence.Pattern = dominantPattern
}

// updateSequencePredictions updates predictions for the next batch
func (bo *BatchOptimizer) updateSequencePredictions(sequence *BatchSequence) {
	if len(sequence.Batches) < 2 {
		return
	}

	recent := sequence.Batches[len(sequence.Batches)-3:] // Last 3 batches
	if len(recent) < 2 {
		recent = sequence.Batches
	}

	// Predict next batch offset based on pattern
	switch sequence.Pattern {
	case BatchPatternLinear:
		// Linear progression
		lastBatch := recent[len(recent)-1]
		if len(recent) >= 2 {
			prevBatch := recent[len(recent)-2]
			gap := lastBatch.StartOffset - prevBatch.EndOffset
			sequence.NextBatchOffset = lastBatch.EndOffset + gap
			sequence.NextBatchSize = lastBatch.Size
			sequence.Confidence = 0.8
		}

	case BatchPatternStrided:
		// Regular stride
		if len(recent) >= 3 {
			stride := recent[len(recent)-1].StartOffset - recent[len(recent)-2].StartOffset
			sequence.NextBatchOffset = recent[len(recent)-1].StartOffset + stride
			sequence.NextBatchSize = recent[len(recent)-1].Size
			sequence.Confidence = 0.7
		}

	default:
		// Lower confidence for unpredictable patterns
		sequence.Confidence = 0.3
	}
}

// GetBatchRecommendations returns optimization recommendations for batch access
func (bo *BatchOptimizer) GetBatchRecommendations(inode uint64) *BatchOptimizationRecommendations {
	bo.RLock()
	defer bo.RUnlock()

	sequence := bo.batchSequences[inode]
	if sequence == nil {
		return &BatchOptimizationRecommendations{
			ShouldOptimize: false,
		}
	}

	sequence.RLock()
	defer sequence.RUnlock()

	prefetchConfig := bo.prefetchStrategies[sequence.Pattern]
	cacheConfig := bo.cacheStrategies[sequence.Pattern]

	if prefetchConfig == nil {
		prefetchConfig = bo.prefetchStrategies[BatchPatternUnknown]
	}
	if cacheConfig == nil {
		cacheConfig = bo.cacheStrategies[BatchPatternUnknown]
	}

	recommendations := &BatchOptimizationRecommendations{
		ShouldOptimize:  true,
		Pattern:         sequence.Pattern,
		PrefetchSize:    prefetchConfig.PrefetchSize,
		PrefetchCount:   prefetchConfig.LookaheadCount,
		CachePriority:   cacheConfig.Priority,
		CacheRetention:  cacheConfig.RetentionTime,
		NextBatchOffset: sequence.NextBatchOffset,
		NextBatchSize:   sequence.NextBatchSize,
		Confidence:      sequence.Confidence,
	}

	return recommendations
}

// BatchOptimizationRecommendations holds batch optimization recommendations
type BatchOptimizationRecommendations struct {
	ShouldOptimize  bool               `json:"should_optimize"`
	Pattern         BatchAccessPattern `json:"pattern"`
	PrefetchSize    int64              `json:"prefetch_size"`
	PrefetchCount   int                `json:"prefetch_count"`
	CachePriority   CachePriority      `json:"cache_priority"`
	CacheRetention  time.Duration      `json:"cache_retention"`
	NextBatchOffset int64              `json:"next_batch_offset"`
	NextBatchSize   int64              `json:"next_batch_size"`
	Confidence      float64            `json:"confidence"`
}

// GetBatchMetrics returns comprehensive batch optimization metrics
func (bo *BatchOptimizer) GetBatchMetrics() BatchOptimizerMetrics {
	bo.RLock()
	defer bo.RUnlock()

	metrics := BatchOptimizerMetrics{
		TotalBatchesDetected: bo.totalBatchesDetected,
		ActiveBatches:        int64(len(bo.activeBatches)),
		CompletedBatches:     int64(len(bo.completedBatches)),
		OptimizationHits:     bo.optimizationHits,
		OptimizationMisses:   bo.optimizationMisses,
		PatternCounts:        make(map[BatchAccessPattern]int64),
	}

	// Count patterns
	for _, batch := range bo.activeBatches {
		batch.RLock()
		metrics.PatternCounts[batch.AccessPattern]++
		batch.RUnlock()
	}

	// Calculate hit rate
	totalAttempts := bo.optimizationHits + bo.optimizationMisses
	if totalAttempts > 0 {
		metrics.OptimizationHitRate = float64(bo.optimizationHits) / float64(totalAttempts)
	}

	return metrics
}

// BatchOptimizerMetrics holds metrics for batch optimization
type BatchOptimizerMetrics struct {
	TotalBatchesDetected int64                        `json:"total_batches_detected"`
	ActiveBatches        int64                        `json:"active_batches"`
	CompletedBatches     int64                        `json:"completed_batches"`
	OptimizationHits     int64                        `json:"optimization_hits"`
	OptimizationMisses   int64                        `json:"optimization_misses"`
	OptimizationHitRate  float64                      `json:"optimization_hit_rate"`
	PatternCounts        map[BatchAccessPattern]int64 `json:"pattern_counts"`
}

// cleanupRoutine performs periodic cleanup of old batch information
func (bo *BatchOptimizer) cleanupRoutine() {
	for {
		select {
		case <-bo.cleanupTicker.C:
			bo.performCleanup()
		case <-bo.stopCleanup:
			return
		}
	}
}

// performCleanup removes old batch information
func (bo *BatchOptimizer) performCleanup() {
	bo.Lock()
	defer bo.Unlock()

	now := time.Now()
	cutoff := now.Add(-30 * time.Minute) // Remove batches older than 30 minutes

	// Clean up completed batches
	for id, batch := range bo.completedBatches {
		batch.RLock()
		shouldRemove := len(batch.AccessTimes) > 0 && batch.AccessTimes[0].Before(cutoff)
		batch.RUnlock()

		if shouldRemove {
			delete(bo.completedBatches, id)
		}
	}

	// Clean up access history
	for inode, history := range bo.accessHistory {
		filtered := make([]BatchAccess, 0, len(history))
		for _, access := range history {
			if access.AccessTime.After(cutoff) {
				filtered = append(filtered, access)
			}
		}

		if len(filtered) == 0 {
			delete(bo.accessHistory, inode)
		} else {
			bo.accessHistory[inode] = filtered
		}
	}

	// Clean up batch sequences
	for inode, sequence := range bo.batchSequences {
		sequence.Lock()
		if sequence.LastAccess.Before(cutoff) {
			delete(bo.batchSequences, inode)
			sequence.Unlock()
			continue
		}
		sequence.Unlock()
	}

	glog.V(4).Infof("Batch optimizer cleanup completed")
}

// Shutdown gracefully shuts down the batch optimizer
func (bo *BatchOptimizer) Shutdown() {
	if bo.cleanupTicker != nil {
		bo.cleanupTicker.Stop()
	}

	close(bo.stopCleanup)

	glog.V(1).Infof("Batch optimizer shutdown complete")
}

// Helper functions

func generateBatchID(inode uint64, offset int64, timestamp time.Time) string {
	return fmt.Sprintf("batch_%d_%d_%d", inode, offset, timestamp.Unix())
}

func generateSequenceID(inode uint64, timestamp time.Time) string {
	return fmt.Sprintf("seq_%d_%d", inode, timestamp.Unix())
}

// String methods for enums

func (bap BatchAccessPattern) String() string {
	switch bap {
	case BatchPatternLinear:
		return "Linear"
	case BatchPatternStrided:
		return "Strided"
	case BatchPatternShuffled:
		return "Shuffled"
	case BatchPatternHierarchical:
		return "Hierarchical"
	case BatchPatternMultiGPU:
		return "MultiGPU"
	case BatchPatternPipelined:
		return "Pipelined"
	default:
		return "Unknown"
	}
}
