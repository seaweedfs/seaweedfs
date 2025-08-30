package ml

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// ChunkMetadata contains metadata about a cached chunk
type ChunkMetadata struct {
	FileId     string            // Chunk file ID
	Offset     uint64            // Offset within the file
	Size       uint64            // Size of the chunk
	CacheLevel int               // 0=memory, 1=disk, 2=not cached
	LastAccess time.Time         // Last access time
	AccessCount int64             // Number of times accessed
	IsHot      bool              // Whether this chunk is frequently accessed
	Pattern    AccessPattern     // Access pattern for this chunk
}

// OpenFileInfo contains comprehensive information about an open file
type OpenFileInfo struct {
	sync.RWMutex
	
	// Basic file information
	Inode      uint64                      // File inode
	Entry      *filer_pb.Entry            // File entry from filer
	OpenCount  int                        // Number of open handles
	OpenTime   time.Time                  // When file was first opened
	LastAccess time.Time                  // Last access time
	
	// Chunk-level caching
	ChunkCache    map[uint32]*ChunkMetadata // chunk index -> metadata
	ChunkCount    uint32                    // Total number of chunks in file
	ChunkSize     int64                     // Size of each chunk
	
	// Access pattern tracking
	AccessInfo    *AccessInfo               // Access pattern information
	ReadPattern   AccessPattern             // Overall file access pattern
	PrefetchState PrefetchState             // Current prefetch state
	
	// ML-specific optimizations
	IsMLFile      bool                      // Whether this is likely an ML-related file
	FileType      MLFileType                // Type of ML file (dataset, model, etc.)
	BatchSize     int                       // Detected batch size for training data
	EpochCount    int                       // Number of epochs detected
	
	// Performance tracking
	TotalBytesRead   int64                  // Total bytes read from this file
	CacheHitCount    int64                  // Number of cache hits
	CacheMissCount   int64                  // Number of cache misses
	PrefetchHitCount int64                  // Number of prefetch hits
}

// PrefetchState represents the current prefetch state for a file
type PrefetchState int

const (
	PrefetchIdle PrefetchState = iota
	PrefetchActive
	PrefetchComplete
	PrefetchSuspended
)

// MLFileType represents the type of ML-related file
type MLFileType int

const (
	MLFileUnknown MLFileType = iota
	MLFileDataset          // Training/validation dataset
	MLFileModel            // Model checkpoint/weights
	MLFileConfig           // Configuration files
	MLFileTensor           // Individual tensor files
	MLFileLog              // Training logs
)

// OpenFileCache manages open file information with ML-aware optimizations
type OpenFileCache struct {
	sync.RWMutex
	
	// Configuration
	maxFiles        int           // Maximum number of files to track
	ttl             time.Duration // TTL for inactive files
	cleanupInterval time.Duration // Cleanup interval
	
	// File tracking
	files           map[uint64]*OpenFileInfo  // inode -> file info
	accessOrder     []uint64                  // LRU order for eviction
	
	// ML-specific configuration
	enableMLOptimization bool
	mlFileDetector      *MLFileDetector
	
	// Metrics
	totalFiles      int64
	evictedFiles    int64
	cacheHits       int64
	cacheMisses     int64
	
	// Background cleanup
	shutdown chan struct{}
	done     chan struct{}
}

// MLFileDetector detects ML-related files based on patterns and metadata
type MLFileDetector struct {
	// File extension patterns
	datasetExtensions map[string]bool
	modelExtensions   map[string]bool
	configExtensions  map[string]bool
	
	// Path patterns
	datasetPaths []string
	modelPaths   []string
	
	// Size heuristics
	modelMinSize    int64 // Minimum size for model files
	datasetMaxItems int   // Maximum items in dataset directory
}

// NewOpenFileCache creates a new open file cache optimized for ML workloads
func NewOpenFileCache(maxFiles int, ttl time.Duration) *OpenFileCache {
	if maxFiles <= 0 {
		maxFiles = 1000 // Default suitable for ML workloads
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute // Default TTL
	}
	
	ofc := &OpenFileCache{
		maxFiles:        maxFiles,
		ttl:             ttl,
		cleanupInterval: 5 * time.Minute,
		files:           make(map[uint64]*OpenFileInfo),
		accessOrder:     make([]uint64, 0, maxFiles),
		enableMLOptimization: true,
		mlFileDetector:  newMLFileDetector(),
		shutdown:        make(chan struct{}),
		done:            make(chan struct{}),
	}
	
	// Start background cleanup
	go ofc.cleanupWorker()
	
	glog.V(1).Infof("OpenFileCache initialized: maxFiles=%d, ttl=%v", maxFiles, ttl)
	return ofc
}

// newMLFileDetector creates a new ML file detector with common patterns
func newMLFileDetector() *MLFileDetector {
	return &MLFileDetector{
		datasetExtensions: map[string]bool{
			"jpg": true, "jpeg": true, "png": true, "bmp": true, "tiff": true,
			"wav": true, "mp3": true, "flac": true,
			"txt": true, "csv": true, "json": true, "jsonl": true,
			"parquet": true, "arrow": true, "h5": true, "hdf5": true,
			"tfrecord": true, "tfrecords": true,
		},
		modelExtensions: map[string]bool{
			"pt": true, "pth": true, "pkl": true, "pickle": true,
			"h5": true, "hdf5": true, "pb": true, "pbtxt": true,
			"onnx": true, "tflite": true, "caffemodel": true,
			"bin": true, "safetensors": true,
		},
		configExtensions: map[string]bool{
			"yaml": true, "yml": true, "json": true, "toml": true,
			"cfg": true, "config": true, "conf": true,
		},
		datasetPaths: []string{
			"/datasets", "/data", "/train", "/test", "/val", "/validation",
			"/images", "/audio", "/text", "/corpus",
		},
		modelPaths: []string{
			"/models", "/checkpoints", "/weights", "/pretrained",
			"/saved_models", "/exports",
		},
		modelMinSize:    1024 * 1024, // 1MB minimum for model files
		datasetMaxItems: 1000000,     // 1M max items in dataset directory
	}
}

// OpenFile registers a file as opened and initializes tracking
func (ofc *OpenFileCache) OpenFile(inode uint64, entry *filer_pb.Entry, fullPath string) *OpenFileInfo {
	ofc.Lock()
	defer ofc.Unlock()
	
	// Get or create file info
	fileInfo := ofc.files[inode]
	if fileInfo == nil {
		fileInfo = &OpenFileInfo{
			Inode:         inode,
			Entry:         entry,
			OpenTime:      time.Now(),
			ChunkCache:    make(map[uint32]*ChunkMetadata),
			AccessInfo:    &AccessInfo{Inode: inode},
			ReadPattern:   RandomAccess,
			PrefetchState: PrefetchIdle,
		}
		
		// Detect ML file type
		if ofc.enableMLOptimization {
			fileInfo.IsMLFile, fileInfo.FileType = ofc.mlFileDetector.DetectMLFile(entry, fullPath)
			if fileInfo.IsMLFile {
				glog.V(3).Infof("ML file detected: inode=%d, type=%v, path=%s", 
					inode, fileInfo.FileType, fullPath)
			}
		}
		
		ofc.files[inode] = fileInfo
		ofc.totalFiles++
		
		// Update access order for LRU
		ofc.updateAccessOrder(inode)
		
		// Evict if necessary
		if len(ofc.files) > ofc.maxFiles {
			ofc.evictLRU()
		}
	}
	
	fileInfo.OpenCount++
	fileInfo.LastAccess = time.Now()
	ofc.updateAccessOrder(inode)
	
	glog.V(4).Infof("File opened: inode=%d, openCount=%d, isML=%v", 
		inode, fileInfo.OpenCount, fileInfo.IsMLFile)
	
	return fileInfo
}

// CloseFile decrements the open count and potentially cleans up
func (ofc *OpenFileCache) CloseFile(inode uint64) bool {
	ofc.Lock()
	defer ofc.Unlock()
	
	fileInfo := ofc.files[inode]
	if fileInfo == nil {
		return true // Already cleaned up
	}
	
	fileInfo.OpenCount--
	glog.V(4).Infof("File closed: inode=%d, openCount=%d", inode, fileInfo.OpenCount)
	
	// Return true if file can be evicted (no more open handles)
	return fileInfo.OpenCount <= 0
}

// GetFileInfo retrieves file information if cached
func (ofc *OpenFileCache) GetFileInfo(inode uint64) *OpenFileInfo {
	ofc.RLock()
	defer ofc.RUnlock()
	
	fileInfo := ofc.files[inode]
	if fileInfo != nil {
		fileInfo.LastAccess = time.Now()
		ofc.cacheHits++
		return fileInfo
	}
	
	ofc.cacheMisses++
	return nil
}

// UpdateChunkCache updates chunk metadata for a file
func (ofc *OpenFileCache) UpdateChunkCache(inode uint64, chunkIndex uint32, metadata *ChunkMetadata) {
	ofc.RLock()
	fileInfo := ofc.files[inode]
	ofc.RUnlock()
	
	if fileInfo == nil {
		return
	}
	
	fileInfo.Lock()
	defer fileInfo.Unlock()
	
	fileInfo.ChunkCache[chunkIndex] = metadata
	metadata.LastAccess = time.Now()
	metadata.AccessCount++
	
	glog.V(4).Infof("Updated chunk cache: inode=%d, chunk=%d, level=%d", 
		inode, chunkIndex, metadata.CacheLevel)
}

// GetChunkMetadata retrieves chunk metadata if available
func (ofc *OpenFileCache) GetChunkMetadata(inode uint64, chunkIndex uint32) (*ChunkMetadata, bool) {
	ofc.RLock()
	fileInfo := ofc.files[inode]
	ofc.RUnlock()
	
	if fileInfo == nil {
		return nil, false
	}
	
	fileInfo.RLock()
	defer fileInfo.RUnlock()
	
	metadata, exists := fileInfo.ChunkCache[chunkIndex]
	if exists {
		metadata.LastAccess = time.Now()
		metadata.AccessCount++
	}
	
	return metadata, exists
}

// updateAccessOrder updates the LRU access order
func (ofc *OpenFileCache) updateAccessOrder(inode uint64) {
	// Remove from current position
	for i, ino := range ofc.accessOrder {
		if ino == inode {
			ofc.accessOrder = append(ofc.accessOrder[:i], ofc.accessOrder[i+1:]...)
			break
		}
	}
	
	// Add to front (most recently used)
	ofc.accessOrder = append([]uint64{inode}, ofc.accessOrder...)
}

// evictLRU evicts the least recently used file
func (ofc *OpenFileCache) evictLRU() {
	if len(ofc.accessOrder) == 0 {
		return
	}
	
	// Find LRU file that can be evicted (not currently open)
	for i := len(ofc.accessOrder) - 1; i >= 0; i-- {
		inode := ofc.accessOrder[i]
		fileInfo := ofc.files[inode]
		
		if fileInfo != nil && fileInfo.OpenCount <= 0 {
			// Evict this file
			delete(ofc.files, inode)
			ofc.accessOrder = append(ofc.accessOrder[:i], ofc.accessOrder[i+1:]...)
			ofc.evictedFiles++
			
			glog.V(3).Infof("Evicted file from cache: inode=%d, chunks=%d", 
				inode, len(fileInfo.ChunkCache))
			return
		}
	}
	
	// If no files can be evicted, just log a warning
	glog.V(2).Infof("Warning: Could not evict any files from cache (all files are open)")
}

// cleanupWorker periodically cleans up expired entries
func (ofc *OpenFileCache) cleanupWorker() {
	ticker := time.NewTicker(ofc.cleanupInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			ofc.cleanup()
		case <-ofc.shutdown:
			close(ofc.done)
			return
		}
	}
}

// cleanup removes expired file entries
func (ofc *OpenFileCache) cleanup() {
	ofc.Lock()
	defer ofc.Unlock()
	
	now := time.Now()
	toRemove := make([]uint64, 0)
	
	for inode, fileInfo := range ofc.files {
		// Only cleanup files that are not open and have expired
		if fileInfo.OpenCount <= 0 && now.Sub(fileInfo.LastAccess) > ofc.ttl {
			toRemove = append(toRemove, inode)
		}
	}
	
	// Remove expired files
	for _, inode := range toRemove {
		delete(ofc.files, inode)
		// Remove from access order
		for i, ino := range ofc.accessOrder {
			if ino == inode {
				ofc.accessOrder = append(ofc.accessOrder[:i], ofc.accessOrder[i+1:]...)
				break
			}
		}
	}
	
	if len(toRemove) > 0 {
		glog.V(3).Infof("Cleaned up %d expired file cache entries", len(toRemove))
	}
}

// GetMetrics returns cache metrics
func (ofc *OpenFileCache) GetMetrics() OpenFileCacheMetrics {
	ofc.RLock()
	defer ofc.RUnlock()
	
	var totalChunks int64
	var mlFiles int64
	fileTypes := make(map[MLFileType]int)
	patterns := make(map[AccessPattern]int)
	
	for _, fileInfo := range ofc.files {
		totalChunks += int64(len(fileInfo.ChunkCache))
		if fileInfo.IsMLFile {
			mlFiles++
			fileTypes[fileInfo.FileType]++
		}
		patterns[fileInfo.ReadPattern]++
	}
	
	return OpenFileCacheMetrics{
		TotalFiles:    int64(len(ofc.files)),
		MLFiles:       mlFiles,
		TotalChunks:   totalChunks,
		CacheHits:     ofc.cacheHits,
		CacheMisses:   ofc.cacheMisses,
		EvictedFiles:  ofc.evictedFiles,
		FileTypes:     fileTypes,
		AccessPatterns: patterns,
	}
}

// OpenFileCacheMetrics holds metrics for the open file cache
type OpenFileCacheMetrics struct {
	TotalFiles     int64                    `json:"total_files"`
	MLFiles        int64                    `json:"ml_files"`
	TotalChunks    int64                    `json:"total_chunks"`
	CacheHits      int64                    `json:"cache_hits"`
	CacheMisses    int64                    `json:"cache_misses"`
	EvictedFiles   int64                    `json:"evicted_files"`
	FileTypes      map[MLFileType]int       `json:"file_types"`
	AccessPatterns map[AccessPattern]int    `json:"access_patterns"`
}

// Shutdown gracefully shuts down the open file cache
func (ofc *OpenFileCache) Shutdown() {
	glog.V(1).Infof("Shutting down OpenFileCache...")
	
	close(ofc.shutdown)
	
	// Wait for cleanup worker to finish
	<-ofc.done
	
	// Print final metrics
	metrics := ofc.GetMetrics()
	glog.V(1).Infof("OpenFileCache final metrics: files=%d, chunks=%d, hits=%d, misses=%d", 
		metrics.TotalFiles, metrics.TotalChunks, metrics.CacheHits, metrics.CacheMisses)
}

// MLFileDetector methods

// DetectMLFile determines if a file is ML-related and its type
func (detector *MLFileDetector) DetectMLFile(entry *filer_pb.Entry, fullPath string) (bool, MLFileType) {
	if entry == nil {
		return false, MLFileUnknown
	}
	
	name := entry.Name
	size := int64(entry.Attributes.FileSize)
	
	// Check file extension
	if ext := getFileExtension(name); ext != "" {
		if detector.datasetExtensions[ext] {
			return true, MLFileDataset
		}
		if detector.modelExtensions[ext] {
			return true, MLFileModel
		}
		if detector.configExtensions[ext] {
			return true, MLFileConfig
		}
	}
	
	// Check path patterns
	for _, path := range detector.datasetPaths {
		if contains(fullPath, path) {
			return true, MLFileDataset
		}
	}
	
	for _, path := range detector.modelPaths {
		if contains(fullPath, path) {
			return true, MLFileModel
		}
	}
	
	// Check size heuristics
	if size > detector.modelMinSize {
		// Large files in certain contexts might be models
		if contains(fullPath, "model") || contains(fullPath, "checkpoint") || contains(fullPath, "weight") {
			return true, MLFileModel
		}
	}
	
	// Check for tensor files
	if contains(name, "tensor") || contains(name, ".pt") || contains(name, ".npy") {
		return true, MLFileTensor
	}
	
	// Check for log files
	if contains(name, "log") || contains(name, "tensorboard") || contains(fullPath, "logs") {
		return true, MLFileLog
	}
	
	return false, MLFileUnknown
}

// Helper functions

func getFileExtension(filename string) string {
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] == '.' {
			return filename[i+1:]
		}
	}
	return ""
}

func contains(str, substr string) bool {
	return len(str) >= len(substr) && findSubstring(str, substr)
}

func findSubstring(str, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(str) < len(substr) {
		return false
	}
	
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// String methods for enums

func (ps PrefetchState) String() string {
	switch ps {
	case PrefetchIdle:
		return "Idle"
	case PrefetchActive:
		return "Active"
	case PrefetchComplete:
		return "Complete"
	case PrefetchSuspended:
		return "Suspended"
	default:
		return "Unknown"
	}
}

func (ft MLFileType) String() string {
	switch ft {
	case MLFileDataset:
		return "Dataset"
	case MLFileModel:
		return "Model"
	case MLFileConfig:
		return "Config"
	case MLFileTensor:
		return "Tensor"
	case MLFileLog:
		return "Log"
	default:
		return "Unknown"
	}
}
