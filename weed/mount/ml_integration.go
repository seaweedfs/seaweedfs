package mount

import (
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/ml"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// MLIntegrationManager manages ML optimization integration for the main WFS
type MLIntegrationManager struct {
	mlOptimization  *ml.MLOptimization
	fuseIntegration *ml.FUSEMLIntegration
	enabled         bool
}

// NewMLIntegrationManager creates a new ML integration manager
func NewMLIntegrationManager(chunkCache chunk_cache.ChunkCache, lookupFn wdclient.LookupFileIdFunctionType) *MLIntegrationManager {
	// Create ML optimization with default config
	config := ml.DefaultMLConfig()
	mlOpt := ml.NewMLOptimization(config, chunkCache, lookupFn)

	// Create FUSE integration
	fuseInt := ml.NewFUSEMLIntegration(mlOpt)

	manager := &MLIntegrationManager{
		mlOptimization:  mlOpt,
		fuseIntegration: fuseInt,
		enabled:         true,
	}

	glog.V(1).Infof("ML integration manager initialized")
	return manager
}

// EnableMLOptimization enables or disables ML optimization
func (mgr *MLIntegrationManager) EnableMLOptimization(enabled bool) {
	mgr.enabled = enabled

	if mgr.mlOptimization != nil {
		mgr.mlOptimization.Enable(enabled)
	}

	if mgr.fuseIntegration != nil {
		mgr.fuseIntegration.EnableMLOptimizations(enabled)
	}

	glog.V(1).Infof("ML optimization %s", map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// OnFileOpen should be called when a file is opened
func (mgr *MLIntegrationManager) OnFileOpen(inode uint64, entry *filer_pb.Entry, fullPath string, flags uint32, out *fuse.OpenOut) {
	if !mgr.enabled || mgr.fuseIntegration == nil {
		return
	}

	mgr.fuseIntegration.OnFileOpen(inode, entry, fullPath, flags, out)
}

// OnFileClose should be called when a file is closed
func (mgr *MLIntegrationManager) OnFileClose(inode uint64) {
	if !mgr.enabled || mgr.fuseIntegration == nil {
		return
	}

	mgr.fuseIntegration.OnFileClose(inode)
}

// OnFileRead should be called when a file is read
func (mgr *MLIntegrationManager) OnFileRead(inode uint64, offset int64, size int) {
	if !mgr.enabled || mgr.fuseIntegration == nil {
		return
	}

	mgr.fuseIntegration.OnFileRead(inode, offset, size)
}

// OnChunkAccess should be called when a chunk is accessed
func (mgr *MLIntegrationManager) OnChunkAccess(inode uint64, chunkIndex uint32, fileId string, cacheLevel int, isHit bool) {
	if !mgr.enabled || mgr.fuseIntegration == nil {
		return
	}

	mgr.fuseIntegration.OnChunkAccess(inode, chunkIndex, fileId, cacheLevel, isHit)
}

// OptimizeAttributes applies ML-specific attribute caching
func (mgr *MLIntegrationManager) OptimizeAttributes(inode uint64, out *fuse.AttrOut) {
	if !mgr.enabled || mgr.fuseIntegration == nil {
		return
	}

	mgr.fuseIntegration.OptimizeAttributes(inode, out)
}

// OptimizeEntryCache applies ML-specific entry caching
func (mgr *MLIntegrationManager) OptimizeEntryCache(inode uint64, entry *filer_pb.Entry, out *fuse.EntryOut) {
	if !mgr.enabled || mgr.fuseIntegration == nil {
		return
	}

	mgr.fuseIntegration.OptimizeEntryCache(inode, entry, out)
}

// ShouldEnableWriteback determines if writeback should be enabled for a file
func (mgr *MLIntegrationManager) ShouldEnableWriteback(inode uint64, entry *filer_pb.Entry) bool {
	if !mgr.enabled || mgr.fuseIntegration == nil {
		return false
	}

	return mgr.fuseIntegration.ShouldEnableWriteback(inode, entry)
}

// GetComprehensiveMetrics returns all ML optimization metrics
func (mgr *MLIntegrationManager) GetComprehensiveMetrics() *ml.FUSEMLMetrics {
	if !mgr.enabled || mgr.fuseIntegration == nil {
		return &ml.FUSEMLMetrics{}
	}

	metrics := mgr.fuseIntegration.GetOptimizationMetrics()
	return &metrics
}

// IsEnabled returns whether ML optimization is enabled
func (mgr *MLIntegrationManager) IsEnabled() bool {
	return mgr.enabled
}

// Shutdown gracefully shuts down the ML integration
func (mgr *MLIntegrationManager) Shutdown() {
	glog.V(1).Infof("Shutting down ML integration manager...")

	if mgr.fuseIntegration != nil {
		mgr.fuseIntegration.Shutdown()
	}

	glog.V(1).Infof("ML integration manager shutdown complete")
}
