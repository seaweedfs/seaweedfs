package ml

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// AccessPattern represents different file access patterns
type AccessPattern int

const (
	RandomAccess AccessPattern = iota
	SequentialAccess
	StridedAccess    // Common in image datasets - fixed stride between accesses
	BatchAccess      // Multiple files accessed together
	EpochAccess      // Dataset restart patterns (ML training)
	ModelAccess      // Large model checkpoint loading
)

func (ap AccessPattern) String() string {
	switch ap {
	case RandomAccess:
		return "Random"
	case SequentialAccess:
		return "Sequential"
	case StridedAccess:
		return "Strided"
	case BatchAccess:
		return "Batch"
	case EpochAccess:
		return "Epoch"
	case ModelAccess:
		return "Model"
	default:
		return "Unknown"
	}
}

// AccessEvent represents a single file access event
type AccessEvent struct {
	Timestamp time.Time
	Inode     uint64
	Offset    int64
	Size      int
	ReadType  string // "sequential", "random", etc.
}

// AccessInfo contains access pattern information for a file
type AccessInfo struct {
	Inode           uint64
	LastOffset      int64
	LastAccessTime  time.Time
	LastSize        int
	ConsecutiveSeq  int  // Count of consecutive sequential reads
	TotalAccesses   int
	BytesRead       int64
	Pattern         AccessPattern
	Confidence      float64 // Confidence in pattern detection (0.0-1.0)
	PrefetchSize    int64   // Recommended prefetch size
}

// AccessPatternDetector detects and analyzes file access patterns for ML workloads
type AccessPatternDetector struct {
	sync.RWMutex
	
	// Configuration
	maxHistory          int
	sequentialThreshold int     // Minimum consecutive reads to consider sequential
	maxGapSize          int64   // Maximum gap to still consider sequential
	stridedMinRepeats   int     // Minimum repeats to detect strided access
	confidenceThreshold float64 // Minimum confidence to act on pattern
	
	// Per-file tracking
	fileInfo map[uint64]*AccessInfo
	
	// Global access history for cross-file pattern detection
	recentAccesses []AccessEvent
	
	// ML-specific heuristics
	enableMLHeuristics bool
	imageFileExtensions map[string]bool
	modelFileExtensions map[string]bool
	
	// Metrics
	totalAccesses     int64
	sequentialReads   int64
	randomReads       int64
	prefetchTriggered int64
}

// NewAccessPatternDetector creates a new access pattern detector optimized for ML workloads
func NewAccessPatternDetector() *AccessPatternDetector {
	return &AccessPatternDetector{
		maxHistory:          1000,
		sequentialThreshold: 3,
		maxGapSize:          64 * 1024, // 64KB
		stridedMinRepeats:   3,
		confidenceThreshold: 0.6,
		fileInfo:            make(map[uint64]*AccessInfo),
		recentAccesses:      make([]AccessEvent, 0, 1000),
		enableMLHeuristics:  true,
		imageFileExtensions: map[string]bool{
			"jpg": true, "jpeg": true, "png": true, "bmp": true,
			"tiff": true, "webp": true, "raw": true,
		},
		modelFileExtensions: map[string]bool{
			"pt": true, "pth": true, "pkl": true, "h5": true,
			"pb": true, "onnx": true, "tflite": true, "caffemodel": true,
		},
	}
}

// RecordAccess records a file access and updates pattern detection
func (apd *AccessPatternDetector) RecordAccess(inode uint64, offset int64, size int) *AccessInfo {
	apd.Lock()
	defer apd.Unlock()
	
	now := time.Now()
	apd.totalAccesses++
	
	// Get or create file info
	info := apd.fileInfo[inode]
	if info == nil {
		info = &AccessInfo{
			Inode:          inode,
			LastOffset:     -1,
			Pattern:        RandomAccess,
			PrefetchSize:   0,
		}
		apd.fileInfo[inode] = info
	}
	
	// Update basic stats
	info.TotalAccesses++
	info.BytesRead += int64(size)
	
	// Detect access pattern
	apd.detectPattern(info, offset, size, now)
	
	// Record in global history for cross-file analysis
	event := AccessEvent{
		Timestamp: now,
		Inode:     inode,
		Offset:    offset,
		Size:      size,
	}
	apd.addToHistory(event)
	
	// Update timing
	info.LastAccessTime = now
	info.LastOffset = offset
	info.LastSize = size
	
	glog.V(4).Infof("Access pattern for inode %d: %s (confidence: %.2f, prefetch: %d)", 
		inode, info.Pattern, info.Confidence, info.PrefetchSize)
	
	return info
}

// detectPattern analyzes access patterns and updates confidence scores
func (apd *AccessPatternDetector) detectPattern(info *AccessInfo, offset int64, size int, now time.Time) {
	if info.LastOffset == -1 {
		// First access
		info.Pattern = RandomAccess
		info.Confidence = 0.5
		return
	}
	
	gap := offset - (info.LastOffset + int64(info.LastSize))
	
	// Sequential access detection
	if gap >= 0 && gap <= apd.maxGapSize {
		info.ConsecutiveSeq++
		if info.ConsecutiveSeq >= apd.sequentialThreshold {
			oldPattern := info.Pattern
			info.Pattern = SequentialAccess
			info.Confidence = minFloat(1.0, 0.1 + float64(info.ConsecutiveSeq) * 0.1)
			
			// Calculate prefetch size for sequential access
			if info.Pattern == SequentialAccess && oldPattern != SequentialAccess {
				apd.sequentialReads++
				// Start with 4x the current read size, capped at 1MB
				info.PrefetchSize = minInt64(4 * int64(size), 1024*1024)
				glog.V(3).Infof("Sequential pattern detected for inode %d, prefetch size: %d", 
					info.Inode, info.PrefetchSize)
			}
		}
	} else {
		// Reset sequential counter on non-sequential access
		if info.ConsecutiveSeq > 0 {
			info.ConsecutiveSeq = 0
			if info.Pattern == SequentialAccess {
				info.Pattern = RandomAccess
				info.Confidence = 0.5
				info.PrefetchSize = 0
				glog.V(4).Infof("Sequential pattern broken for inode %d", info.Inode)
				return // Don't check for other patterns after breaking sequential
			}
		}
		apd.randomReads++
	}
	
	// ML-specific pattern detection
	if apd.enableMLHeuristics {
		apd.detectMLPatterns(info, offset, size, now)
	}
	
	// Adapt prefetch size based on access frequency
	if info.Pattern == SequentialAccess && info.TotalAccesses > 10 {
		timeSinceLastAccess := now.Sub(info.LastAccessTime)
		if timeSinceLastAccess < 100*time.Millisecond {
			// High frequency access, increase prefetch
			info.PrefetchSize = minInt64(info.PrefetchSize * 2, 2*1024*1024) // Cap at 2MB
		} else if timeSinceLastAccess > 5*time.Second {
			// Low frequency access, decrease prefetch
			info.PrefetchSize = maxInt64(info.PrefetchSize / 2, 64*1024) // Minimum 64KB
		}
	}
}

// detectMLPatterns detects ML-specific access patterns
func (apd *AccessPatternDetector) detectMLPatterns(info *AccessInfo, offset int64, size int, now time.Time) {
	// Large file sequential reads often indicate model loading
	if size > 1024*1024 && info.Pattern == SequentialAccess { // > 1MB reads
		info.Pattern = ModelAccess
		info.Confidence = 0.9
		info.PrefetchSize = minInt64(8*1024*1024, info.PrefetchSize*4) // Aggressive prefetch for models
		glog.V(3).Infof("Model access pattern detected for inode %d", info.Inode)
		return
	}
	
	// Detect epoch restarts - same file accessed after a gap
	if info.TotalAccesses > 100 && offset == 0 {
		timeSinceLastAccess := now.Sub(info.LastAccessTime)
		if timeSinceLastAccess > 1*time.Minute {
			info.Pattern = EpochAccess
			info.Confidence = 0.8
			// For epoch access, prefetch aggressively at the beginning
			info.PrefetchSize = minInt64(2*1024*1024, maxInt64(info.PrefetchSize, 256*1024))
			glog.V(3).Infof("Epoch restart detected for inode %d", info.Inode)
			return
		}
	}
	
	// Detect strided access patterns (common with image datasets)
	// Only detect strided access if we have enough accesses and it's not already sequential
	if info.TotalAccesses > 3 && info.Pattern != SequentialAccess && apd.isStridedAccess(info, offset) {
		info.Pattern = StridedAccess
		info.Confidence = 0.7
		// For strided access, prefetch based on stride size
		info.PrefetchSize = minInt64(1024*1024, maxInt64(info.PrefetchSize, 128*1024))
		glog.V(4).Infof("Strided access pattern detected for inode %d", info.Inode)
	}
}

// isStridedAccess detects regular stride patterns in file access
func (apd *AccessPatternDetector) isStridedAccess(info *AccessInfo, offset int64) bool {
	// This is a simplified implementation
	// In a real implementation, we'd track multiple previous offsets to detect patterns
	if info.TotalAccesses < 5 { // Require more accesses for stride detection
		return false
	}
	
	// For now, just detect if there's a consistent gap size
	// This would be expanded to track multiple stride patterns
	expectedOffset := info.LastOffset + int64(info.LastSize)
	if offset > expectedOffset {
		gap := offset - expectedOffset
		// If the gap is consistent and reasonable for image data
		// Be more restrictive: gap should be in a reasonable range for strided access
		if gap > 1024 && gap < 64*1024 { // Between 1KB and 64KB gap
			return true
		}
	}
	
	return false
}

// ShouldPrefetch determines if prefetching should be triggered for a file
func (apd *AccessPatternDetector) ShouldPrefetch(inode uint64) (bool, int64) {
	apd.RLock()
	defer apd.RUnlock()
	
	info := apd.fileInfo[inode]
	if info == nil {
		return false, 0
	}
	
	// Only prefetch if we have high confidence in the pattern
	if info.Confidence < apd.confidenceThreshold {
		return false, 0
	}
	
	// Always prefetch for sequential and ML-specific patterns
	switch info.Pattern {
	case SequentialAccess, ModelAccess, EpochAccess:
		return true, info.PrefetchSize
	case StridedAccess:
		// Be more conservative with strided access
		return info.Confidence > 0.8, info.PrefetchSize
	default:
		return false, 0
	}
}

// GetPattern returns the detected access pattern for a file
func (apd *AccessPatternDetector) GetPattern(inode uint64) AccessPattern {
	apd.RLock()
	defer apd.RUnlock()
	
	info := apd.fileInfo[inode]
	if info == nil {
		return RandomAccess
	}
	
	return info.Pattern
}

// GetMetrics returns access pattern detection metrics
func (apd *AccessPatternDetector) GetMetrics() AccessPatternMetrics {
	apd.RLock()
	defer apd.RUnlock()
	
	patterns := make(map[AccessPattern]int)
	totalFiles := len(apd.fileInfo)
	
	for _, info := range apd.fileInfo {
		patterns[info.Pattern]++
	}
	
	return AccessPatternMetrics{
		TotalAccesses:     apd.totalAccesses,
		SequentialReads:   apd.sequentialReads,
		RandomReads:       apd.randomReads,
		PrefetchTriggered: apd.prefetchTriggered,
		TotalFiles:        int64(totalFiles),
		PatternCounts:     patterns,
	}
}

// AccessPatternMetrics holds metrics for access pattern detection
type AccessPatternMetrics struct {
	TotalAccesses     int64
	SequentialReads   int64
	RandomReads       int64
	PrefetchTriggered int64
	TotalFiles        int64
	PatternCounts     map[AccessPattern]int
}

// addToHistory adds an access event to the global history
func (apd *AccessPatternDetector) addToHistory(event AccessEvent) {
	if len(apd.recentAccesses) >= apd.maxHistory {
		// Remove oldest entry (simple circular buffer)
		copy(apd.recentAccesses, apd.recentAccesses[1:])
		apd.recentAccesses = apd.recentAccesses[:len(apd.recentAccesses)-1]
	}
	
	apd.recentAccesses = append(apd.recentAccesses, event)
}

// CleanupOldEntries removes stale file access information
func (apd *AccessPatternDetector) CleanupOldEntries(maxAge time.Duration) {
	apd.Lock()
	defer apd.Unlock()
	
	now := time.Now()
	toDelete := make([]uint64, 0)
	
	for inode, info := range apd.fileInfo {
		if now.Sub(info.LastAccessTime) > maxAge {
			toDelete = append(toDelete, inode)
		}
	}
	
	for _, inode := range toDelete {
		delete(apd.fileInfo, inode)
	}
	
	if len(toDelete) > 0 {
		glog.V(3).Infof("Cleaned up %d old access pattern entries", len(toDelete))
	}
}

// Helper functions

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

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
