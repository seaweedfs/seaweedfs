package empty_folder_cleanup

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DefaultMaxCountCheck  = 1000
	DefaultCacheExpiry    = 5 * time.Minute
	DefaultQueueMaxSize   = 1000
	DefaultQueueMaxAge    = 10 * time.Minute
	DefaultProcessorSleep = 10 * time.Second // How often to check queue
)

// FilerOperations defines the filer operations needed by EmptyFolderCleaner
type FilerOperations interface {
	CountDirectoryEntries(ctx context.Context, dirPath util.FullPath, limit int) (count int, err error)
	DeleteEntryMetaAndData(ctx context.Context, p util.FullPath, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32, ifNotModifiedAfter int64) error
	GetEntryAttributes(ctx context.Context, p util.FullPath) (attributes map[string][]byte, err error)
}

// folderState tracks the state of a folder for empty folder cleanup
type folderState struct {
	roughCount  int       // Cached rough count (up to maxCountCheck)
	isImplicit  *bool     // Tri-state boolean: nil (unknown), true (implicit), false (explicit)
	lastAddTime time.Time // Last time an item was added
	lastDelTime time.Time // Last time an item was deleted
	lastCheck   time.Time // Last time we checked the actual count
}

// EmptyFolderCleaner handles asynchronous cleanup of empty folders
// Each filer owns specific folders via consistent hashing based on the peer filer list
type EmptyFolderCleaner struct {
	filer    FilerOperations
	lockRing *lock_manager.LockRing
	host     pb.ServerAddress

	// Folder state tracking
	mu           sync.RWMutex
	folderCounts map[string]*folderState // Rough count cache

	// Cleanup queue (thread-safe, has its own lock)
	cleanupQueue *CleanupQueue

	// Configuration
	maxCountCheck  int           // Max items to count (1000)
	cacheExpiry    time.Duration // How long to keep cache entries
	processorSleep time.Duration // How often processor checks queue
	bucketPath     string        // e.g., "/buckets"

	// Control
	enabled bool
	stopCh  chan struct{}
}

// NewEmptyFolderCleaner creates a new EmptyFolderCleaner
func NewEmptyFolderCleaner(filer FilerOperations, lockRing *lock_manager.LockRing, host pb.ServerAddress, bucketPath string) *EmptyFolderCleaner {
	efc := &EmptyFolderCleaner{
		filer:          filer,
		lockRing:       lockRing,
		host:           host,
		folderCounts:   make(map[string]*folderState),
		cleanupQueue:   NewCleanupQueue(DefaultQueueMaxSize, DefaultQueueMaxAge),
		maxCountCheck:  DefaultMaxCountCheck,
		cacheExpiry:    DefaultCacheExpiry,
		processorSleep: DefaultProcessorSleep,
		bucketPath:     bucketPath,
		enabled:        true,
		stopCh:         make(chan struct{}),
	}
	go efc.cacheEvictionLoop()
	go efc.cleanupProcessor()
	return efc
}

// SetEnabled enables or disables the cleaner
func (efc *EmptyFolderCleaner) SetEnabled(enabled bool) {
	efc.mu.Lock()
	defer efc.mu.Unlock()
	efc.enabled = enabled
}

// IsEnabled returns whether the cleaner is enabled
func (efc *EmptyFolderCleaner) IsEnabled() bool {
	efc.mu.RLock()
	defer efc.mu.RUnlock()
	return efc.enabled
}

// ownsFolder checks if this filer owns the folder via consistent hashing
func (efc *EmptyFolderCleaner) ownsFolder(folder string) bool {
	servers := efc.lockRing.GetSnapshot()
	if len(servers) <= 1 {
		return true // Single filer case
	}
	return efc.hashKeyToServer(folder, servers) == efc.host
}

// hashKeyToServer uses consistent hashing to map a folder to a server
func (efc *EmptyFolderCleaner) hashKeyToServer(key string, servers []pb.ServerAddress) pb.ServerAddress {
	if len(servers) == 0 {
		return ""
	}
	x := util.HashStringToLong(key)
	if x < 0 {
		x = -x
	}
	x = x % int64(len(servers))
	return servers[x]
}

// OnDeleteEvent is called when a file or directory is deleted
// Both file and directory deletions count towards making the parent folder empty
// eventTime is the time when the delete event occurred (for proper ordering)
func (efc *EmptyFolderCleaner) OnDeleteEvent(directory string, entryName string, isDirectory bool, eventTime time.Time) {
	// Skip if not under bucket path (must be at least /buckets/<bucket>/...)
	if efc.bucketPath != "" && !isUnderBucketPath(directory, efc.bucketPath) {
		return
	}

	// Check if we own this folder
	if !efc.ownsFolder(directory) {
		glog.V(4).Infof("EmptyFolderCleaner: not owner of %s, skipping", directory)
		return
	}

	efc.mu.Lock()
	defer efc.mu.Unlock()

	// Check enabled inside lock to avoid race with Stop()
	if !efc.enabled {
		return
	}

	glog.V(3).Infof("EmptyFolderCleaner: delete event in %s/%s (isDir=%v)", directory, entryName, isDirectory)

	// Update cached count (create entry if needed)
	state, exists := efc.folderCounts[directory]
	if !exists {
		state = &folderState{}
		efc.folderCounts[directory] = state
	}
	if state.roughCount > 0 {
		state.roughCount--
	}
	state.lastDelTime = eventTime

	// Only add to cleanup queue if roughCount suggests folder might be empty
	if state.roughCount > 0 {
		glog.V(3).Infof("EmptyFolderCleaner: skipping queue for %s, roughCount=%d", directory, state.roughCount)
		return
	}

	// Add to cleanup queue with event time (handles out-of-order events)
	if efc.cleanupQueue.Add(directory, eventTime) {
		glog.V(3).Infof("EmptyFolderCleaner: queued %s for cleanup", directory)
	}
}

// OnCreateEvent is called when a file or directory is created
// Both file and directory creations cancel pending cleanup for the parent folder
func (efc *EmptyFolderCleaner) OnCreateEvent(directory string, entryName string, isDirectory bool) {
	// Skip if not under bucket path (must be at least /buckets/<bucket>/...)
	if efc.bucketPath != "" && !isUnderBucketPath(directory, efc.bucketPath) {
		return
	}

	efc.mu.Lock()
	defer efc.mu.Unlock()

	// Check enabled inside lock to avoid race with Stop()
	if !efc.enabled {
		return
	}

	// Update cached count only if already tracked (no need to track new folders)
	if state, exists := efc.folderCounts[directory]; exists {
		state.roughCount++
		state.lastAddTime = time.Now()
	}

	// Remove from cleanup queue (cancel pending cleanup)
	if efc.cleanupQueue.Remove(directory) {
		glog.V(3).Infof("EmptyFolderCleaner: cancelled cleanup for %s due to new entry", directory)
	}
}

// cleanupProcessor runs in background and processes the cleanup queue
func (efc *EmptyFolderCleaner) cleanupProcessor() {
	ticker := time.NewTicker(efc.processorSleep)
	defer ticker.Stop()

	for {
		select {
		case <-efc.stopCh:
			return
		case <-ticker.C:
			efc.processCleanupQueue()
		}
	}
}

// processCleanupQueue processes items from the cleanup queue
func (efc *EmptyFolderCleaner) processCleanupQueue() {
	// Check if we should process
	if !efc.cleanupQueue.ShouldProcess() {
		if efc.cleanupQueue.Len() > 0 {
			glog.Infof("EmptyFolderCleaner: pending queue not processed yet (len=%d, oldest_age=%v, max_size=%d, max_age=%v)",
				efc.cleanupQueue.Len(), efc.cleanupQueue.OldestAge(), efc.cleanupQueue.maxSize, efc.cleanupQueue.maxAge)
		}
		return
	}

	glog.V(3).Infof("EmptyFolderCleaner: processing cleanup queue (len=%d, age=%v)",
		efc.cleanupQueue.Len(), efc.cleanupQueue.OldestAge())

	// Process all items that are ready
	for efc.cleanupQueue.Len() > 0 {
		// Check if still enabled
		if !efc.IsEnabled() {
			return
		}

		// Pop the oldest item
		folder, ok := efc.cleanupQueue.Pop()
		if !ok {
			break
		}

		// Execute cleanup for this folder
		efc.executeCleanup(folder)

		// If queue is no longer full and oldest item is not old enough, stop processing
		if !efc.cleanupQueue.ShouldProcess() {
			break
		}
	}
}

// executeCleanup performs the actual cleanup of an empty folder
func (efc *EmptyFolderCleaner) executeCleanup(folder string) {
	efc.mu.Lock()

	// Quick check: if we have cached count and it's > 0, skip
	if state, exists := efc.folderCounts[folder]; exists {
		if state.roughCount > 0 {
			glog.V(3).Infof("EmptyFolderCleaner: skipping %s, cached count=%d", folder, state.roughCount)
			efc.mu.Unlock()
			return
		}
		// If there was an add after our delete, skip
		if !state.lastAddTime.IsZero() && state.lastAddTime.After(state.lastDelTime) {
			glog.V(3).Infof("EmptyFolderCleaner: skipping %s, add happened after delete", folder)
			efc.mu.Unlock()
			return
		}
	}
	efc.mu.Unlock()

	// Re-check ownership (topology might have changed)
	if !efc.ownsFolder(folder) {
		glog.V(3).Infof("EmptyFolderCleaner: no longer owner of %s, skipping", folder)
		return
	}

	// Check for explicit implicit_dir attribute
	// First check cache
	ctx := context.Background()
	efc.mu.RLock()
	var cachedImplicit *bool
	if state, exists := efc.folderCounts[folder]; exists {
		cachedImplicit = state.isImplicit
	}
	efc.mu.RUnlock()

	var isImplicit bool
	implicitSource := "cache"
	implicitAttr := "<cached>"
	if cachedImplicit != nil {
		isImplicit = *cachedImplicit
		if isImplicit {
			implicitAttr = "true"
		} else {
			implicitAttr = "false"
		}
	} else {
		implicitSource = "filer"
		// Not cached, check filer
		attrs, err := efc.filer.GetEntryAttributes(ctx, util.FullPath(folder))
		if err != nil {
			if err == filer_pb.ErrNotFound {
				return
			}
			glog.V(2).Infof("EmptyFolderCleaner: error getting attributes for %s: %v", folder, err)
			return
		}

		isImplicit = attrs != nil && string(attrs[s3_constants.ExtS3ImplicitDir]) == "true"
		if attrs == nil {
			implicitAttr = "<no_attrs>"
		} else if value, found := attrs[s3_constants.ExtS3ImplicitDir]; found {
			implicitAttr = string(value)
		} else {
			implicitAttr = "<missing>"
		}

		// Update cache
		efc.mu.Lock()
		if _, exists := efc.folderCounts[folder]; !exists {
			efc.folderCounts[folder] = &folderState{}
		}
		efc.folderCounts[folder].isImplicit = &isImplicit
		efc.mu.Unlock()
	}

	if !isImplicit {
		glog.Infof("EmptyFolderCleaner: folder %s is not marked as implicit (source=%s attr=%s), skipping", folder, implicitSource, implicitAttr)
		return
	}

	// Check if folder is actually empty (count up to maxCountCheck)
	count, err := efc.countItems(ctx, folder)
	if err != nil {
		glog.V(2).Infof("EmptyFolderCleaner: error counting items in %s: %v", folder, err)
		return
	}

	efc.mu.Lock()
	// Update cache
	if _, exists := efc.folderCounts[folder]; !exists {
		efc.folderCounts[folder] = &folderState{}
	}
	efc.folderCounts[folder].roughCount = count
	efc.folderCounts[folder].lastCheck = time.Now()
	efc.mu.Unlock()

	if count > 0 {
		glog.V(3).Infof("EmptyFolderCleaner: folder %s has %d items, not empty", folder, count)
		return
	}

	// Delete the empty folder
	glog.V(2).Infof("EmptyFolderCleaner: deleting empty folder %s", folder)
	if err := efc.deleteFolder(ctx, folder); err != nil {
		glog.V(2).Infof("EmptyFolderCleaner: failed to delete empty folder %s: %v", folder, err)
		return
	}

	// Clean up cache entry
	efc.mu.Lock()
	delete(efc.folderCounts, folder)
	efc.mu.Unlock()

	// Note: No need to recursively check parent folder here.
	// The deletion of this folder will generate a metadata event,
	// which will trigger OnDeleteEvent for the parent folder.
}

// countItems counts items in a folder (up to maxCountCheck)
func (efc *EmptyFolderCleaner) countItems(ctx context.Context, folder string) (int, error) {
	return efc.filer.CountDirectoryEntries(ctx, util.FullPath(folder), efc.maxCountCheck)
}

// deleteFolder deletes an empty folder
func (efc *EmptyFolderCleaner) deleteFolder(ctx context.Context, folder string) error {
	return efc.filer.DeleteEntryMetaAndData(ctx, util.FullPath(folder), false, false, false, false, nil, 0)
}

// isUnderPath checks if child is under parent path
func isUnderPath(child, parent string) bool {
	if parent == "" || parent == "/" {
		return true
	}
	// Ensure parent ends without slash for proper prefix matching
	if len(parent) > 0 && parent[len(parent)-1] == '/' {
		parent = parent[:len(parent)-1]
	}
	// Child must start with parent and then have a / or be exactly parent
	if len(child) < len(parent) {
		return false
	}
	if child[:len(parent)] != parent {
		return false
	}
	if len(child) == len(parent) {
		return true
	}
	return child[len(parent)] == '/'
}

// isUnderBucketPath checks if directory is inside a bucket (under /buckets/<bucket>/...)
// This ensures we only clean up folders inside buckets, not the buckets themselves
func isUnderBucketPath(directory, bucketPath string) bool {
	if bucketPath == "" {
		return true
	}
	// Ensure bucketPath ends without slash
	if len(bucketPath) > 0 && bucketPath[len(bucketPath)-1] == '/' {
		bucketPath = bucketPath[:len(bucketPath)-1]
	}
	// Directory must be under bucketPath
	if !isUnderPath(directory, bucketPath) {
		return false
	}
	// Directory must be at least /buckets/<bucket>/<something>
	// i.e., depth must be at least bucketPath depth + 2
	// For /buckets (depth 1), we need at least /buckets/mybucket/folder (depth 3)
	bucketPathDepth := strings.Count(bucketPath, "/")
	directoryDepth := strings.Count(directory, "/")
	return directoryDepth >= bucketPathDepth+2
}

// cacheEvictionLoop periodically removes stale entries from folderCounts
func (efc *EmptyFolderCleaner) cacheEvictionLoop() {
	ticker := time.NewTicker(efc.cacheExpiry)
	defer ticker.Stop()

	for {
		select {
		case <-efc.stopCh:
			return
		case <-ticker.C:
			efc.evictStaleCacheEntries()
		}
	}
}

// evictStaleCacheEntries removes cache entries that haven't been accessed recently
func (efc *EmptyFolderCleaner) evictStaleCacheEntries() {
	efc.mu.Lock()
	defer efc.mu.Unlock()

	now := time.Now()
	expiredCount := 0
	for folder, state := range efc.folderCounts {
		// Skip if folder is in cleanup queue
		if efc.cleanupQueue.Contains(folder) {
			continue
		}

		// Find the most recent activity time for this folder
		lastActivity := state.lastCheck
		if state.lastAddTime.After(lastActivity) {
			lastActivity = state.lastAddTime
		}
		if state.lastDelTime.After(lastActivity) {
			lastActivity = state.lastDelTime
		}

		// Evict if no activity within cache expiry period
		if now.Sub(lastActivity) > efc.cacheExpiry {
			delete(efc.folderCounts, folder)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		glog.V(3).Infof("EmptyFolderCleaner: evicted %d stale cache entries", expiredCount)
	}
}

// Stop stops the cleaner and cancels all pending tasks
func (efc *EmptyFolderCleaner) Stop() {
	close(efc.stopCh)

	efc.mu.Lock()
	defer efc.mu.Unlock()

	efc.enabled = false
	efc.cleanupQueue.Clear()
	efc.folderCounts = make(map[string]*folderState) // Clear cache on stop
}

// GetPendingCleanupCount returns the number of pending cleanup tasks (for testing)
func (efc *EmptyFolderCleaner) GetPendingCleanupCount() int {
	return efc.cleanupQueue.Len()
}

// GetCachedFolderCount returns the cached count for a folder (for testing)
func (efc *EmptyFolderCleaner) GetCachedFolderCount(folder string) (int, bool) {
	efc.mu.RLock()
	defer efc.mu.RUnlock()
	if state, exists := efc.folderCounts[folder]; exists {
		return state.roughCount, true
	}
	return 0, false
}
