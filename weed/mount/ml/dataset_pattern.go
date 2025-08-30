package ml

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// DatasetAccessPattern represents different dataset access patterns in ML training
type DatasetAccessPattern int

const (
	DatasetUnknown DatasetAccessPattern = iota
	DatasetSequential                   // Linear traversal through dataset
	DatasetShuffle                     // Randomized access within epochs
	DatasetBatch                       // Batch-based access patterns
	DatasetMultiEpoch                  // Cross-epoch pattern detection
	DatasetDistributed                 // Multi-GPU/distributed training patterns
	DatasetValidation                  // Validation/test set access patterns
)

// DatasetTraversalInfo holds information about dataset traversal patterns
type DatasetTraversalInfo struct {
	sync.RWMutex
	
	// Dataset characteristics
	DatasetSize       int64             // Estimated total dataset size
	ItemSize          int64             // Average item size
	ItemCount         int64             // Number of items in dataset
	BatchSize         int               // Detected batch size
	EpochCount        int               // Number of completed epochs
	
	// Access patterns
	Pattern           DatasetAccessPattern // Current detected pattern
	LastEpochStart    time.Time            // When current epoch started
	EpochDuration     time.Duration        // Average epoch duration
	ItemsPerSecond    float64              // Processing throughput
	
	// Traversal tracking
	AccessOrder       []int64              // Recent access order for pattern detection
	EpochBoundaries   []int64              // File offsets where epochs start
	ShufflePattern    []int                // Detected shuffle pattern if any
	
	// Batch detection
	BatchStartOffsets []int64              // Starting offsets of detected batches
	BatchAccessTimes  []time.Time          // When batches were accessed
	
	// Statistics
	TotalAccesses     int64                // Total number of accesses
	EpochAccesses     int64                // Accesses in current epoch
	ValidationAccess  bool                 // Whether this looks like validation data
	
	// Prediction and optimization
	PredictedNextAccess int64              // Predicted next access offset
	OptimalPrefetchSize int64              // Recommended prefetch size
	ShouldCache         bool               // Whether to aggressively cache this dataset
}

// DatasetPatternDetector detects and analyzes ML dataset access patterns
type DatasetPatternDetector struct {
	sync.RWMutex
	
	// Configuration
	maxDatasets          int                        // Maximum datasets to track
	epochDetectionWindow int                        // Number of accesses to analyze for epoch detection
	batchDetectionWindow int                        // Number of accesses to analyze for batch detection
	shuffleWindowSize    int                        // Size of window to detect shuffling
	
	// Active datasets
	datasets             map[uint64]*DatasetTraversalInfo // inode -> dataset info
	
	// Pattern detection parameters
	sequentialThreshold  float64                          // Threshold for sequential detection
	shuffleThreshold     float64                          // Threshold for shuffle detection
	batchSizeVariance    float64                          // Allowed variance in batch size detection
	
	// Statistics
	totalDatasets        int64                            // Total datasets seen
	patternsDetected     map[DatasetAccessPattern]int64   // Count of each pattern detected
	
	// Cleanup
	lastCleanup          time.Time                        // When we last cleaned up
	cleanupInterval      time.Duration                    // How often to cleanup
}

// NewDatasetPatternDetector creates a new dataset pattern detector
func NewDatasetPatternDetector() *DatasetPatternDetector {
	return &DatasetPatternDetector{
		maxDatasets:          100,  // Track up to 100 datasets
		epochDetectionWindow: 1000, // Look at last 1000 accesses for epoch detection
		batchDetectionWindow: 50,   // Look at last 50 accesses for batch detection
		shuffleWindowSize:    100,  // Look at 100-item windows for shuffle detection
		
		datasets:             make(map[uint64]*DatasetTraversalInfo),
		patternsDetected:     make(map[DatasetAccessPattern]int64),
		
		sequentialThreshold:  0.8,  // 80% sequential for sequential pattern
		shuffleThreshold:     0.6,  // 60% randomness for shuffle pattern
		batchSizeVariance:    0.15, // 15% variance allowed in batch sizes
		
		cleanupInterval:      10 * time.Minute,
	}
}

// RecordDatasetAccess records an access to a dataset file and updates pattern detection
func (dpd *DatasetPatternDetector) RecordDatasetAccess(inode uint64, offset int64, size int, fileSize int64, isNewEpoch bool) *DatasetTraversalInfo {
	dpd.Lock()
	defer dpd.Unlock()
	
	// Get or create dataset info
	datasetInfo := dpd.datasets[inode]
	if datasetInfo == nil {
		datasetInfo = &DatasetTraversalInfo{
			DatasetSize:         fileSize,
			ItemSize:            int64(size), // Initial estimate
			LastEpochStart:      time.Now(),
			AccessOrder:         make([]int64, 0, dpd.epochDetectionWindow),
			EpochBoundaries:     make([]int64, 0, 10),
			BatchStartOffsets:   make([]int64, 0, dpd.batchDetectionWindow),
			BatchAccessTimes:    make([]time.Time, 0, dpd.batchDetectionWindow),
			Pattern:             DatasetUnknown,
		}
		dpd.datasets[inode] = datasetInfo
		dpd.totalDatasets++
		
		glog.V(3).Infof("New dataset registered: inode=%d, size=%d", inode, fileSize)
	}
	
	datasetInfo.Lock()
	defer datasetInfo.Unlock()
	
	now := time.Now()
	
	// Update basic statistics
	datasetInfo.TotalAccesses++
	datasetInfo.EpochAccesses++
	
	// Handle epoch boundary detection
	if isNewEpoch || dpd.detectEpochBoundary(datasetInfo, offset) {
		dpd.handleEpochBoundary(datasetInfo, offset, now)
	}
	
	// Update access tracking
	datasetInfo.AccessOrder = append(datasetInfo.AccessOrder, offset)
	if len(datasetInfo.AccessOrder) > dpd.epochDetectionWindow {
		datasetInfo.AccessOrder = datasetInfo.AccessOrder[1:]
	}
	
	// Update batch tracking
	datasetInfo.BatchStartOffsets = append(datasetInfo.BatchStartOffsets, offset)
	datasetInfo.BatchAccessTimes = append(datasetInfo.BatchAccessTimes, now)
	if len(datasetInfo.BatchStartOffsets) > dpd.batchDetectionWindow {
		datasetInfo.BatchStartOffsets = datasetInfo.BatchStartOffsets[1:]
		datasetInfo.BatchAccessTimes = datasetInfo.BatchAccessTimes[1:]
	}
	
	// Detect patterns
	oldPattern := datasetInfo.Pattern
	dpd.detectDatasetPattern(datasetInfo)
	
	// Update predictions and recommendations
	dpd.updatePredictions(datasetInfo)
	
	// Log pattern changes
	if oldPattern != datasetInfo.Pattern {
		dpd.patternsDetected[datasetInfo.Pattern]++
		glog.V(2).Infof("Dataset pattern changed: inode=%d, %v -> %v, batch_size=%d", 
			inode, oldPattern, datasetInfo.Pattern, datasetInfo.BatchSize)
	}
	
	return datasetInfo
}

// detectEpochBoundary detects if we've started a new epoch
func (dpd *DatasetPatternDetector) detectEpochBoundary(info *DatasetTraversalInfo, offset int64) bool {
	// Simple heuristic: if we're accessing near the beginning of the file after accessing later parts
	if len(info.AccessOrder) < 2 {
		return false
	}
	
	// If current access is near beginning (first 10%) and previous was near end (last 50%)
	fileStart := info.DatasetSize / 10
	fileMiddle := info.DatasetSize / 2
	
	previousOffset := info.AccessOrder[len(info.AccessOrder)-1]
	
	return offset < fileStart && previousOffset > fileMiddle
}

// handleEpochBoundary handles the start of a new epoch
func (dpd *DatasetPatternDetector) handleEpochBoundary(info *DatasetTraversalInfo, offset int64, now time.Time) {
	if !info.LastEpochStart.IsZero() {
		// Calculate epoch duration
		epochDuration := now.Sub(info.LastEpochStart)
		if info.EpochDuration == 0 {
			info.EpochDuration = epochDuration
		} else {
			// Running average
			info.EpochDuration = (info.EpochDuration + epochDuration) / 2
		}
		
		// Calculate throughput
		if epochDuration > 0 && info.EpochAccesses > 0 {
			info.ItemsPerSecond = float64(info.EpochAccesses) / epochDuration.Seconds()
		}
	}
	
	info.EpochCount++
	info.LastEpochStart = now
	info.EpochAccesses = 0
	info.EpochBoundaries = append(info.EpochBoundaries, offset)
	
	// Keep only recent epoch boundaries
	if len(info.EpochBoundaries) > 10 {
		info.EpochBoundaries = info.EpochBoundaries[len(info.EpochBoundaries)-10:]
	}
	
	glog.V(3).Infof("Epoch boundary detected: inode=%d, epoch=%d, duration=%v, throughput=%.1f items/sec", 
		info.DatasetSize, info.EpochCount, info.EpochDuration, info.ItemsPerSecond)
}

// detectDatasetPattern analyzes recent accesses to determine the dataset access pattern
func (dpd *DatasetPatternDetector) detectDatasetPattern(info *DatasetTraversalInfo) {
	if len(info.AccessOrder) < 10 {
		return // Need more data
	}
	
	// Analyze last N accesses
	windowSize := min(len(info.AccessOrder), 50)
	recentAccesses := info.AccessOrder[len(info.AccessOrder)-windowSize:]
	
	// Calculate various pattern indicators
	sequentialScore := dpd.calculateSequentialScore(recentAccesses)
	shuffleScore := dpd.calculateShuffleScore(recentAccesses)
	batchScore := dpd.calculateBatchScore(info)
	
	// Determine pattern based on scores
	newPattern := DatasetUnknown
	
	if sequentialScore > dpd.sequentialThreshold {
		newPattern = DatasetSequential
	} else if shuffleScore > dpd.shuffleThreshold {
		newPattern = DatasetShuffle
	} else if batchScore > 0.7 {
		newPattern = DatasetBatch
	} else if info.EpochCount > 1 {
		newPattern = DatasetMultiEpoch
	}
	
	// Special case: validation pattern (less frequent, different timing)
	if dpd.detectValidationPattern(info) {
		newPattern = DatasetValidation
	}
	
	info.Pattern = newPattern
	
	glog.V(4).Infof("Pattern scores: inode=%d, seq=%.2f, shuffle=%.2f, batch=%.2f -> %v", 
		info.DatasetSize, sequentialScore, shuffleScore, batchScore, newPattern)
}

// calculateSequentialScore determines how sequential the access pattern is
func (dpd *DatasetPatternDetector) calculateSequentialScore(accesses []int64) float64 {
	if len(accesses) < 2 {
		return 0.0
	}
	
	sequentialCount := 0
	for i := 1; i < len(accesses); i++ {
		if accesses[i] > accesses[i-1] {
			sequentialCount++
		}
	}
	
	return float64(sequentialCount) / float64(len(accesses)-1)
}

// calculateShuffleScore determines how shuffled/randomized the access pattern is
func (dpd *DatasetPatternDetector) calculateShuffleScore(accesses []int64) float64 {
	if len(accesses) < dpd.shuffleWindowSize {
		return 0.0
	}
	
	// Look for randomness in access order
	// A shuffled pattern will have accesses distributed across the file
	
	// Calculate variance in access positions
	var sum, sumSq float64
	n := float64(len(accesses))
	
	for _, offset := range accesses {
		sum += float64(offset)
		sumSq += float64(offset) * float64(offset)
	}
	
	mean := sum / n
	variance := (sumSq / n) - (mean * mean)
	
	// Higher variance suggests more randomness/shuffling
	// Normalize by dataset size
	if len(accesses) > 0 {
		maxOffset := float64(accesses[0])
		for _, offset := range accesses {
			if float64(offset) > maxOffset {
				maxOffset = float64(offset)
			}
		}
		if maxOffset > 0 {
			normalizedVariance := variance / (maxOffset * maxOffset)
			return minFloat64(normalizedVariance*10, 1.0) // Scale to 0-1 range
		}
	}
	
	return 0.0
}

// calculateBatchScore determines if accesses follow a clear batch pattern
func (dpd *DatasetPatternDetector) calculateBatchScore(info *DatasetTraversalInfo) float64 {
	if len(info.BatchStartOffsets) < 5 {
		return 0.0
	}
	
	// Look for regular intervals between batch starts
	intervals := make([]int64, 0, len(info.BatchStartOffsets)-1)
	for i := 1; i < len(info.BatchStartOffsets); i++ {
		interval := info.BatchStartOffsets[i] - info.BatchStartOffsets[i-1]
		if interval > 0 {
			intervals = append(intervals, interval)
		}
	}
	
	if len(intervals) < 3 {
		return 0.0
	}
	
	// Calculate coefficient of variation for intervals
	var sum, sumSq float64
	for _, interval := range intervals {
		sum += float64(interval)
		sumSq += float64(interval) * float64(interval)
	}
	
	n := float64(len(intervals))
	mean := sum / n
	variance := (sumSq / n) - (mean * mean)
	
	if mean > 0 {
		cv := variance / (mean * mean) // Coefficient of variation
		
		// Lower CV (more regular intervals) = higher batch score
		batchScore := maxFloat64(0.0, 1.0-cv)
		
		// Update detected batch size
		if batchScore > 0.5 && mean > 0 {
			estimatedBatchSize := int(mean / float64(info.ItemSize))
			if estimatedBatchSize > 0 {
				info.BatchSize = estimatedBatchSize
			}
		}
		
		return batchScore
	}
	
	return 0.0
}

// detectValidationPattern determines if this looks like validation dataset access
func (dpd *DatasetPatternDetector) detectValidationPattern(info *DatasetTraversalInfo) bool {
	// Validation datasets typically:
	// 1. Are accessed less frequently than training data
	// 2. Have more regular/sequential access patterns
	// 3. Are accessed after training phases
	
	if info.TotalAccesses < 100 {
		return false
	}
	
	// Check access frequency (validation typically accessed less often)
	avgTimeBetweenAccesses := time.Duration(0)
	if len(info.BatchAccessTimes) > 1 {
		totalDuration := info.BatchAccessTimes[len(info.BatchAccessTimes)-1].Sub(info.BatchAccessTimes[0])
		avgTimeBetweenAccesses = totalDuration / time.Duration(len(info.BatchAccessTimes)-1)
	}
	
	// If average time between accesses is > 1 minute, might be validation
	if avgTimeBetweenAccesses > time.Minute {
		info.ValidationAccess = true
		return true
	}
	
	return false
}

// updatePredictions updates predictions and optimization recommendations
func (dpd *DatasetPatternDetector) updatePredictions(info *DatasetTraversalInfo) {
	if len(info.AccessOrder) < 2 {
		return
	}
	
	switch info.Pattern {
	case DatasetSequential:
		// Predict next sequential access
		lastAccess := info.AccessOrder[len(info.AccessOrder)-1]
		info.PredictedNextAccess = lastAccess + info.ItemSize
		info.OptimalPrefetchSize = info.ItemSize * int64(info.BatchSize) * 2 // Prefetch 2 batches ahead
		info.ShouldCache = true
		
	case DatasetShuffle:
		// For shuffled access, prefetch is less predictable but still valuable
		info.OptimalPrefetchSize = info.ItemSize * int64(info.BatchSize) // Prefetch current batch
		info.ShouldCache = true
		
	case DatasetBatch:
		// Predict batch-aligned access
		if info.BatchSize > 0 {
			info.OptimalPrefetchSize = info.ItemSize * int64(info.BatchSize) * 3 // Prefetch 3 batches
			info.ShouldCache = true
		}
		
	case DatasetValidation:
		// Validation data can be more aggressively cached
		info.OptimalPrefetchSize = minInt64(info.DatasetSize/10, 1024*1024*50) // Up to 50MB or 10% of dataset
		info.ShouldCache = true
		
	default:
		info.OptimalPrefetchSize = info.ItemSize * 8 // Default prefetch
		info.ShouldCache = false
	}
	
	// Ensure prefetch size is reasonable
	info.OptimalPrefetchSize = maxInt64(info.OptimalPrefetchSize, 64*1024)        // At least 64KB
	info.OptimalPrefetchSize = minInt64(info.OptimalPrefetchSize, 100*1024*1024) // At most 100MB
}

// GetDatasetInfo returns information about a dataset
func (dpd *DatasetPatternDetector) GetDatasetInfo(inode uint64) *DatasetTraversalInfo {
	dpd.RLock()
	defer dpd.RUnlock()
	
	return dpd.datasets[inode]
}

// GetDatasetMetrics returns comprehensive metrics about dataset patterns
func (dpd *DatasetPatternDetector) GetDatasetMetrics() DatasetPatternMetrics {
	dpd.RLock()
	defer dpd.RUnlock()
	
	metrics := DatasetPatternMetrics{
		TotalDatasets:    dpd.totalDatasets,
		ActiveDatasets:   int64(len(dpd.datasets)),
		PatternsDetected: make(map[DatasetAccessPattern]int64),
	}
	
	// Copy pattern counts
	for pattern, count := range dpd.patternsDetected {
		metrics.PatternsDetected[pattern] = count
	}
	
	// Calculate aggregate statistics
	var totalEpochs, totalBatches int64
	var avgThroughput float64
	activeCount := 0
	
	for _, info := range dpd.datasets {
		info.RLock()
		totalEpochs += int64(info.EpochCount)
		if info.BatchSize > 0 {
			totalBatches += int64(info.TotalAccesses / int64(info.BatchSize))
		}
		if info.ItemsPerSecond > 0 {
			avgThroughput += info.ItemsPerSecond
			activeCount++
		}
		info.RUnlock()
	}
	
	metrics.TotalEpochs = totalEpochs
	metrics.TotalBatches = totalBatches
	if activeCount > 0 {
		metrics.AverageThroughput = avgThroughput / float64(activeCount)
	}
	
	return metrics
}

// DatasetPatternMetrics holds metrics for dataset pattern detection
type DatasetPatternMetrics struct {
	TotalDatasets       int64                              `json:"total_datasets"`
	ActiveDatasets      int64                              `json:"active_datasets"`
	TotalEpochs         int64                              `json:"total_epochs"`
	TotalBatches        int64                              `json:"total_batches"`
	AverageThroughput   float64                            `json:"average_throughput"`
	PatternsDetected    map[DatasetAccessPattern]int64     `json:"patterns_detected"`
}

// Cleanup removes old dataset information
func (dpd *DatasetPatternDetector) Cleanup() {
	dpd.Lock()
	defer dpd.Unlock()
	
	now := time.Now()
	if now.Sub(dpd.lastCleanup) < dpd.cleanupInterval {
		return
	}
	
	// Remove datasets that haven't been accessed recently
	toRemove := make([]uint64, 0)
	for inode, info := range dpd.datasets {
		info.RLock()
		lastAccess := time.Time{}
		if len(info.BatchAccessTimes) > 0 {
			lastAccess = info.BatchAccessTimes[len(info.BatchAccessTimes)-1]
		}
		shouldRemove := now.Sub(lastAccess) > 30*time.Minute
		info.RUnlock()
		
		if shouldRemove {
			toRemove = append(toRemove, inode)
		}
	}
	
	for _, inode := range toRemove {
		delete(dpd.datasets, inode)
	}
	
	if len(toRemove) > 0 {
		glog.V(3).Infof("Cleaned up %d old dataset entries", len(toRemove))
	}
	
	dpd.lastCleanup = now
}

// Helper functions

func minFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// String methods for enums

func (dap DatasetAccessPattern) String() string {
	switch dap {
	case DatasetSequential:
		return "Sequential"
	case DatasetShuffle:
		return "Shuffle"
	case DatasetBatch:
		return "Batch"
	case DatasetMultiEpoch:
		return "MultiEpoch"
	case DatasetDistributed:
		return "Distributed"
	case DatasetValidation:
		return "Validation"
	default:
		return "Unknown"
	}
}
