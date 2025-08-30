package ml

import (
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// FUSEMLIntegration provides ML optimization integration for SeaweedFS FUSE mount
type FUSEMLIntegration struct {
	// Core ML components
	openFileCache  *OpenFileCache
	cachePolicy    *MLCachePolicy
	mlOptimization *MLOptimization

	// FUSE-specific configuration
	enableKeepCache   bool          // Enable FOPEN_KEEP_CACHE for ML files
	enableWriteback   bool          // Enable writeback caching
	attrCacheTimeout  time.Duration // Attribute cache timeout for ML files
	entryCacheTimeout time.Duration // Entry cache timeout for ML files

	// ML-specific FUSE optimizations
	mlAttrTimeout      time.Duration // Extended attribute timeout for ML files
	datasetAttrTimeout time.Duration // Even longer timeout for dataset files
	modelAttrTimeout   time.Duration // Longest timeout for model files

	// Statistics
	keepCacheEnabled int64 // Number of times keep cache was enabled
	writebackEnabled int64 // Number of times writeback was enabled
	mlAttrCacheHits  int64 // ML-specific attribute cache hits
}

// NewFUSEMLIntegration creates a new FUSE ML integration
func NewFUSEMLIntegration(mlOpt *MLOptimization) *FUSEMLIntegration {
	return &FUSEMLIntegration{
		openFileCache:     NewOpenFileCache(1000, 30*time.Minute),
		cachePolicy:       NewMLCachePolicy(),
		mlOptimization:    mlOpt,
		enableKeepCache:   true,
		enableWriteback:   true,
		attrCacheTimeout:  5 * time.Second,
		entryCacheTimeout: 10 * time.Second,

		// ML-specific timeouts (longer for more stable caching)
		mlAttrTimeout:      30 * time.Second,
		datasetAttrTimeout: 60 * time.Second,
		modelAttrTimeout:   120 * time.Second, // Longest for model files
	}
}

// OnFileOpen handles file open events for ML optimization
func (fmi *FUSEMLIntegration) OnFileOpen(inode uint64, entry *filer_pb.Entry, fullPath string, flags uint32, out *fuse.OpenOut) {
	// Register file in cache
	fileInfo := fmi.openFileCache.OpenFile(inode, entry, fullPath)

	// Apply ML-specific FUSE optimizations
	if fileInfo.IsMLFile && fmi.enableKeepCache {
		// Enable keep cache for ML files to reduce redundant reads
		out.OpenFlags |= fuse.FOPEN_KEEP_CACHE
		fmi.keepCacheEnabled++

		glog.V(3).Infof("Enabled FOPEN_KEEP_CACHE for ML file: inode=%d, type=%v",
			inode, fileInfo.FileType)
	}

	// For large model files, also enable direct I/O to bypass page cache for very large reads
	if fileInfo.FileType == MLFileModel && entry.Attributes.FileSize > 100*1024*1024 { // > 100MB
		// Note: Direct I/O can be beneficial for very large sequential reads
		// but may hurt performance for small random reads
		if fileInfo.ReadPattern == SequentialAccess || fileInfo.ReadPattern == ModelAccess {
			out.OpenFlags |= fuse.FOPEN_DIRECT_IO
			glog.V(3).Infof("Enabled FOPEN_DIRECT_IO for large model file: inode=%d", inode)
		}
	}
}

// OnFileClose handles file close events
func (fmi *FUSEMLIntegration) OnFileClose(inode uint64) {
	canEvict := fmi.openFileCache.CloseFile(inode)

	if canEvict {
		glog.V(4).Infof("File closed and available for eviction: inode=%d", inode)
	}
}

// OnFileRead handles file read events for ML pattern detection
func (fmi *FUSEMLIntegration) OnFileRead(inode uint64, offset int64, size int) {
	// Update access pattern
	if fmi.mlOptimization != nil && fmi.mlOptimization.IsEnabled() {
		accessInfo := fmi.mlOptimization.RecordAccess(inode, offset, size)

		// Update file info with detected pattern
		if fileInfo := fmi.openFileCache.GetFileInfo(inode); fileInfo != nil {
			fileInfo.Lock()
			if accessInfo != nil {
				fileInfo.ReadPattern = accessInfo.Pattern
				fileInfo.AccessInfo = accessInfo
			}
			fileInfo.TotalBytesRead += int64(size)
			fileInfo.Unlock()

			// Trigger prefetching if pattern detected
			if shouldPrefetch, _ := fmi.mlOptimization.ShouldPrefetch(inode); shouldPrefetch {
				glog.V(4).Infof("Prefetch triggered for ML file: inode=%d, pattern=%v",
					inode, fileInfo.ReadPattern)
			}
		}
	}
}

// OptimizeAttributes applies ML-specific attribute caching optimizations
func (fmi *FUSEMLIntegration) OptimizeAttributes(inode uint64, out *fuse.AttrOut) {
	fileInfo := fmi.openFileCache.GetFileInfo(inode)
	if fileInfo == nil {
		// Use default timeout
		out.AttrValid = uint64(fmi.attrCacheTimeout.Seconds())
		return
	}

	// Apply ML-specific timeouts
	var timeout time.Duration

	switch fileInfo.FileType {
	case MLFileModel:
		// Model files rarely change, cache attributes longer
		timeout = fmi.modelAttrTimeout
	case MLFileDataset:
		// Dataset files are read-only during training, cache longer
		timeout = fmi.datasetAttrTimeout
	case MLFileTensor, MLFileConfig:
		// Moderate timeout for tensor and config files
		timeout = fmi.mlAttrTimeout
	default:
		// Use default timeout for non-ML files
		timeout = fmi.attrCacheTimeout
	}

	out.AttrValid = uint64(timeout.Seconds())
	fmi.mlAttrCacheHits++

	glog.V(4).Infof("ML attribute cache timeout: inode=%d, type=%v, timeout=%v",
		inode, fileInfo.FileType, timeout)
}

// OptimizeEntryCache applies ML-specific entry caching optimizations
func (fmi *FUSEMLIntegration) OptimizeEntryCache(inode uint64, entry *filer_pb.Entry, out *fuse.EntryOut) {
	fileInfo := fmi.openFileCache.GetFileInfo(inode)
	if fileInfo == nil {
		// Use default timeout
		out.SetEntryTimeout(fmi.entryCacheTimeout)
		return
	}

	// ML files can have longer entry cache timeouts since they change infrequently
	var timeout time.Duration

	switch fileInfo.FileType {
	case MLFileModel, MLFileDataset:
		// Models and datasets rarely change during training
		timeout = fmi.datasetAttrTimeout
	case MLFileConfig:
		// Config files change even less frequently
		timeout = fmi.modelAttrTimeout
	default:
		timeout = fmi.entryCacheTimeout
	}

	out.SetEntryTimeout(timeout)

	glog.V(4).Infof("ML entry cache timeout: inode=%d, type=%v, timeout=%v",
		inode, fileInfo.FileType, timeout)
}

// ShouldEnableWriteback determines if writeback caching should be enabled for a file
func (fmi *FUSEMLIntegration) ShouldEnableWriteback(inode uint64, entry *filer_pb.Entry) bool {
	if !fmi.enableWriteback {
		return false
	}

	fileInfo := fmi.openFileCache.GetFileInfo(inode)
	if fileInfo == nil {
		return false
	}

	// Enable writeback for ML files that are frequently written
	switch fileInfo.FileType {
	case MLFileLog:
		// Training logs benefit from writeback caching
		return true
	case MLFileModel:
		// Model checkpoints during training benefit from writeback
		if fileInfo.AccessInfo != nil && fileInfo.AccessInfo.Pattern == SequentialAccess {
			return true
		}
	case MLFileConfig:
		// Config files rarely change, so writeback not as beneficial
		return false
	case MLFileDataset:
		// Datasets are typically read-only during training
		return false
	default:
		// Default behavior for non-ML files
		return false
	}

	return false
}

// OnChunkAccess updates chunk-level metadata when chunks are accessed
func (fmi *FUSEMLIntegration) OnChunkAccess(inode uint64, chunkIndex uint32, fileId string, cacheLevel int, isHit bool) {
	metadata := &ChunkMetadata{
		FileId:      fileId,
		Offset:      uint64(chunkIndex) * 1024, // Assuming 1KB chunks for now
		Size:        1024,
		LastAccess:  time.Now(),
		CacheLevel:  cacheLevel,
		AccessCount: 1, // Will be incremented in UpdateChunkCache
	}

	// Update chunk cache
	fmi.openFileCache.UpdateChunkCache(inode, chunkIndex, metadata)

	// Update file-level statistics
	if fileInfo := fmi.openFileCache.GetFileInfo(inode); fileInfo != nil {
		fileInfo.Lock()
		if isHit {
			fileInfo.CacheHitCount++
		} else {
			fileInfo.CacheMissCount++
		}
		fileInfo.Unlock()
	}
}

// GetOptimizationMetrics returns comprehensive optimization metrics
func (fmi *FUSEMLIntegration) GetOptimizationMetrics() FUSEMLMetrics {
	var mlMetrics *MLOptimizationMetrics
	if fmi.mlOptimization != nil {
		mlMetrics = fmi.mlOptimization.GetMetrics()
	}

	return FUSEMLMetrics{
		MLOptimizationMetrics: mlMetrics,
		OpenFileCacheMetrics:  fmi.openFileCache.GetMetrics(),
		CachePolicyMetrics:    fmi.cachePolicy.GetEvictionMetrics(),
		KeepCacheEnabled:      fmi.keepCacheEnabled,
		WritebackEnabled:      fmi.writebackEnabled,
		MLAttrCacheHits:       fmi.mlAttrCacheHits,
		EnableKeepCache:       fmi.enableKeepCache,
		EnableWriteback:       fmi.enableWriteback,
	}
}

// FUSEMLMetrics holds comprehensive FUSE ML optimization metrics
type FUSEMLMetrics struct {
	MLOptimizationMetrics *MLOptimizationMetrics `json:"ml_optimization,omitempty"`
	OpenFileCacheMetrics  OpenFileCacheMetrics   `json:"open_file_cache"`
	CachePolicyMetrics    MLCachePolicyMetrics   `json:"cache_policy"`

	// FUSE-specific metrics
	KeepCacheEnabled int64 `json:"keep_cache_enabled"`
	WritebackEnabled int64 `json:"writeback_enabled"`
	MLAttrCacheHits  int64 `json:"ml_attr_cache_hits"`

	// Configuration
	EnableKeepCache bool `json:"enable_keep_cache"`
	EnableWriteback bool `json:"enable_writeback"`
}

// Shutdown gracefully shuts down the FUSE ML integration
func (fmi *FUSEMLIntegration) Shutdown() {
	glog.V(1).Infof("Shutting down FUSE ML integration...")

	if fmi.openFileCache != nil {
		fmi.openFileCache.Shutdown()
	}

	if fmi.mlOptimization != nil {
		fmi.mlOptimization.Shutdown()
	}

	// Print final metrics
	metrics := fmi.GetOptimizationMetrics()
	glog.V(1).Infof("FUSE ML integration final metrics: keep_cache=%d, writeback=%d, attr_hits=%d",
		metrics.KeepCacheEnabled, metrics.WritebackEnabled, metrics.MLAttrCacheHits)
}

// EnableMLOptimizations enables or disables ML optimizations
func (fmi *FUSEMLIntegration) EnableMLOptimizations(enabled bool) {
	fmi.enableKeepCache = enabled
	fmi.enableWriteback = enabled

	if fmi.mlOptimization != nil {
		fmi.mlOptimization.Enable(enabled)
	}

	glog.V(1).Infof("ML FUSE optimizations %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// SetCacheTimeouts configures cache timeouts for different file types
func (fmi *FUSEMLIntegration) SetCacheTimeouts(attr, entry, mlAttr, dataset, model time.Duration) {
	fmi.attrCacheTimeout = attr
	fmi.entryCacheTimeout = entry
	fmi.mlAttrTimeout = mlAttr
	fmi.datasetAttrTimeout = dataset
	fmi.modelAttrTimeout = model

	glog.V(2).Infof("Updated cache timeouts: attr=%v, entry=%v, ml=%v, dataset=%v, model=%v",
		attr, entry, mlAttr, dataset, model)
}
