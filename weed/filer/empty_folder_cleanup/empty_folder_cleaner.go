package empty_folder_cleanup

import (
	"context"
	"os"
	"sort"
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
	DefaultQueueMaxAge    = 2 * time.Minute
	DefaultProcessorSleep = 30 * time.Second // How often to check queue
	DefaultMaxDeletedKept = 10000            // Deleted folders remembered for the restore check
	// How long a deleted folder is kept so that a create event arriving for it can
	// still put it back. It bounds how far behind the event stream may run, not how
	// long the race window is.
	DefaultObservationWindow = 2 * time.Minute
)

// DirectoryAttributes is what a restored directory needs to come back as it was.
type DirectoryAttributes struct {
	Mode       os.FileMode
	Uid        uint32
	Gid        uint32
	UserName   string
	GroupNames []string
}

// deletedFolder is a folder under observation for entries that landed while it was
// being deleted. writtenTo is set by the create event for such an entry.
type deletedFolder struct {
	path      string
	attrs     DirectoryAttributes
	deletedAt time.Time
	writtenTo bool
}

// FilerOperations defines the filer operations needed by EmptyFolderCleaner
type FilerOperations interface {
	CountDirectoryEntries(ctx context.Context, dirPath util.FullPath, limit int) (count int, err error)
	DeleteEntryMetaAndData(ctx context.Context, p util.FullPath, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32, ifNotModifiedAfter int64) error
	GetEntryAttributes(ctx context.Context, p util.FullPath) (attributes map[string][]byte, err error)
	IsDirectoryKeyObject(ctx context.Context, p util.FullPath) (bool, error)
	DirectoryAttributes(ctx context.Context, p util.FullPath) (DirectoryAttributes, error)
	EnsureDirectoryEntry(ctx context.Context, p util.FullPath, attrs DirectoryAttributes) error
}

// folderState tracks the state of a folder for empty folder cleanup
type folderState struct {
	roughCount  int       // Cached rough count (up to maxCountCheck)
	lastAddTime time.Time // Last time an item was added
	lastDelTime time.Time // Last time an item was deleted
	lastCheck   time.Time // Last time we checked the actual count
}

type bucketCleanupPolicyState struct {
	autoRemove bool
	attrValue  string
	lastCheck  time.Time
}

// EmptyFolderCleaner handles asynchronous cleanup of empty folders
// Each filer owns specific folders via consistent hashing based on the peer filer list
type EmptyFolderCleaner struct {
	filer    FilerOperations
	lockRing *lock_manager.LockRing
	host     pb.ServerAddress

	// Folder state tracking
	mu                    sync.RWMutex
	folderCounts          map[string]*folderState              // Rough count cache
	bucketCleanupPolicies map[string]*bucketCleanupPolicyState // bucket path -> cleanup policy cache

	// Folders deleted recently, kept so that a create event arriving for one of them
	// can put it back
	deleted        map[string]*deletedFolder
	deletedDropped int

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

// NewEmptyFolderCleaner creates a new EmptyFolderCleaner.
// cleanupDelay controls how long an empty folder must remain in the queue before deletion.
// If zero, DefaultQueueMaxAge is used.
func NewEmptyFolderCleaner(filer FilerOperations, lockRing *lock_manager.LockRing, host pb.ServerAddress, bucketPath string, cleanupDelay time.Duration) *EmptyFolderCleaner {
	if cleanupDelay <= 0 {
		cleanupDelay = DefaultQueueMaxAge
	}
	efc := &EmptyFolderCleaner{
		filer:                 filer,
		lockRing:              lockRing,
		host:                  host,
		folderCounts:          make(map[string]*folderState),
		bucketCleanupPolicies: make(map[string]*bucketCleanupPolicyState),
		deleted:               make(map[string]*deletedFolder),
		cleanupQueue:          NewCleanupQueue(DefaultQueueMaxSize, cleanupDelay),
		maxCountCheck:         DefaultMaxCountCheck,
		cacheExpiry:           DefaultCacheExpiry,
		processorSleep:        DefaultProcessorSleep,
		bucketPath:            bucketPath,
		enabled:               true,
		stopCh:                make(chan struct{}),
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
	primary := efc.lockRing.GetPrimary(folder)
	if primary == "" {
		return true // Single filer case or no servers
	}
	return primary == efc.host
}

// OnDeleteEvent is called when a file or directory is deleted
// Both file and directory deletions count towards making the parent folder empty
// eventTime is the time when the delete event occurred (for proper ordering)
func (efc *EmptyFolderCleaner) OnDeleteEvent(directory string, entryName string, isDirectory bool, eventTime time.Time) {
	// Skip if not under bucket path (must be at least /buckets/<bucket>/...)
	if efc.bucketPath != "" && !isUnderBucketPath(directory, efc.bucketPath) {
		return
	}

	// Never queue the S3 multipart staging area; the upload lifecycle owns it.
	if isMultipartUploadsPath(efc.bucketPath, directory) {
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
	if efc.cleanupQueue.Add(directory, entryName, eventTime) {
		glog.V(3).Infof("EmptyFolderCleaner: queued %s for cleanup (triggered by %s)", directory, entryName)
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

	// An entry landing in a folder we just deleted is the race this cleaner cannot
	// exclude: the folder was empty when checked and is gone now, so the entry has
	// nothing holding it. The event says so outright, which beats going back to look.
	if folder, found := efc.deleted[directory]; found {
		folder.writtenTo = true
		glog.V(2).Infof("EmptyFolderCleaner: %s was written to while being deleted, restoring it", directory)
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
	efc.restoreFoldersWrittenDuringDelete()

	if efc.cleanupQueue.Len() == 0 {
		return
	}

	glog.V(3).Infof("EmptyFolderCleaner: processing cleanup queue (len=%d, oldest_age=%v)",
		efc.cleanupQueue.Len(), efc.cleanupQueue.OldestAge())

	// Only process items that have been queued longer than maxAge
	for {
		// Check if still enabled
		if !efc.IsEnabled() {
			return
		}

		// Only pop items old enough — newer items stay in the queue
		folder, triggeredBy, ok := efc.cleanupQueue.PopOlderThan(efc.cleanupQueue.maxAge)
		if !ok {
			break
		}

		// Execute cleanup for this folder
		efc.executeCleanup(folder, triggeredBy)
	}
}

// restoreFoldersWrittenDuringDelete puts back folders that received an entry between
// the emptiness check and the delete, which leaves that entry with no directory
// holding it: reachable by its own path, but absent from any listing.
//
// A folder stays under observation for DefaultRestoreCheckWindow rather than being
// checked once. A writer looks up the parent before inserting the child, so a check
// can land in that gap and see nothing; ticks also coalesce when a pass runs long, so
// "next pass" is not a delay at all. Re-checking for a bounded wall-clock window
// covers both. This narrows the exposure rather than closing it - only making the
// emptiness check and the delete atomic would do that.
func (efc *EmptyFolderCleaner) restoreFoldersWrittenDuringDelete() {
	efc.mu.Lock()
	dropped := efc.deletedDropped
	efc.deletedDropped = 0
	var restore []*deletedFolder
	for path, folder := range efc.deleted {
		// The window applies whatever the folder's state is. Checking writtenTo first
		// would keep a folder whose restore keeps failing forever, re-counting it on
		// every pass.
		if time.Since(folder.deletedAt) >= DefaultObservationWindow {
			delete(efc.deleted, path)
			continue
		}
		if folder.writtenTo {
			restore = append(restore, folder)
			delete(efc.deleted, path)
		}
	}
	// A cascade takes ancestors along with the folder. Rebuild those from what they
	// were too: leaving them to the descendant's restore would mint them from the
	// descendant's own attributes, handing back access the ancestor did not grant.
	for i := 0; i < len(restore); i++ {
		ancestor, _ := util.FullPath(restore[i].path).DirAndName()
		for ancestor != "" && ancestor != "/" {
			if folder, found := efc.deleted[ancestor]; found {
				restore = append(restore, folder)
				delete(efc.deleted, ancestor)
			}
			ancestor, _ = util.FullPath(ancestor).DirAndName()
		}
	}
	efc.mu.Unlock()

	if dropped > 0 {
		glog.V(1).Infof("EmptyFolderCleaner: %d deleted folders left unobserved, past the %d kept", dropped, DefaultMaxDeletedKept)
	}
	if len(restore) == 0 {
		return
	}

	// Restore shallowest first, so a folder taken by the parent cascade is rebuilt
	// with its own attributes before anything below it needs it as a parent.
	sort.Slice(restore, func(i, j int) bool {
		return strings.Count(restore[i].path, "/") < strings.Count(restore[j].path, "/")
	})

	ctx := context.Background()
	var retry []*deletedFolder
	for i, folder := range restore {
		if !efc.IsEnabled() {
			retry = append(retry, restore[i:]...)
			break
		}
		// An event named this folder, but the entry may have been removed again since,
		// in which case there is nothing to hold and it can stay gone. Ancestors pulled
		// in above carry no event and are rebuilt regardless, since the folder below
		// them needs them.
		if folder.writtenTo {
			count, err := efc.countItems(ctx, folder.path)
			if err != nil {
				glog.V(2).Infof("EmptyFolderCleaner: cannot count %s before restoring it: %v", folder.path, err)
				retry = append(retry, folder)
				continue
			}
			if count == 0 {
				continue
			}
		}
		glog.V(1).Infof("EmptyFolderCleaner: restoring %s, written to while it was being deleted", folder.path)
		if err := efc.filer.EnsureDirectoryEntry(ctx, util.FullPath(folder.path), folder.attrs); err != nil {
			glog.V(2).Infof("EmptyFolderCleaner: failed to restore %s: %v", folder.path, err)
			retry = append(retry, folder)
		}
	}

	if len(retry) == 0 {
		return
	}
	efc.mu.Lock()
	for _, folder := range retry {
		if _, found := efc.deleted[folder.path]; !found {
			efc.makeRoomForDeletedLocked()
		}
		efc.deleted[folder.path] = folder
	}
	efc.mu.Unlock()
}

// makeRoomForDeletedLocked drops the oldest of a small sample when the set is full.
// The newest folders are the ones whose race is still live, so they must not be the
// ones given up; sampling keeps this cheap under heavy deletion rates.
func (efc *EmptyFolderCleaner) makeRoomForDeletedLocked() {
	if len(efc.deleted) < DefaultMaxDeletedKept {
		return
	}
	const sampleSize = 32
	oldestPath, seen := "", 0
	for path, folder := range efc.deleted {
		if oldestPath == "" || folder.deletedAt.Before(efc.deleted[oldestPath].deletedAt) {
			oldestPath = path
		}
		if seen++; seen >= sampleSize {
			break
		}
	}
	if oldestPath != "" {
		delete(efc.deleted, oldestPath)
		efc.deletedDropped++
	}
}

// executeCleanup performs the actual cleanup of an empty folder
func (efc *EmptyFolderCleaner) executeCleanup(folder string, triggeredBy string) {
	// The bucket-shared .uploads staging tree holds in-progress multipart uploads.
	// Deleting <bucket>/.uploads (reached here directly or via the parent cascade
	// below) races a concurrent CreateMultipartUpload: the new upload's marker row
	// is inserted between the emptiness check and the folder delete, then wiped,
	// so the upload silently vanishes. Leave this tree to the upload lifecycle.
	if isMultipartUploadsPath(efc.bucketPath, folder) {
		return
	}

	efc.mu.Lock()

	// Quick check: if we have cached count and it's > 0, skip
	if state, exists := efc.folderCounts[folder]; exists {
		if state.roughCount > 0 {
			glog.V(3).Infof("EmptyFolderCleaner: skipping %s (triggered by %s), cached count=%d", folder, triggeredBy, state.roughCount)
			efc.mu.Unlock()
			return
		}
		// If there was an add after our delete, skip
		if !state.lastAddTime.IsZero() && state.lastAddTime.After(state.lastDelTime) {
			glog.V(3).Infof("EmptyFolderCleaner: skipping %s (triggered by %s), add happened after delete", folder, triggeredBy)
			efc.mu.Unlock()
			return
		}
	}
	efc.mu.Unlock()

	// Re-check ownership (topology might have changed)
	if !efc.ownsFolder(folder) {
		glog.V(3).Infof("EmptyFolderCleaner: no longer owner of %s (triggered by %s), skipping", folder, triggeredBy)
		return
	}

	ctx := context.Background()
	bucketPath, autoRemove, source, attrValue, err := efc.getBucketCleanupPolicy(ctx, folder)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return
		}
		glog.V(2).Infof("EmptyFolderCleaner: failed to load bucket cleanup policy for folder %s (triggered by %s): %v", folder, triggeredBy, err)
		return
	}

	if !autoRemove {
		glog.V(3).Infof("EmptyFolderCleaner: skipping folder %s (triggered by %s), bucket %s auto-remove-empty-folders disabled (source=%s attr=%s)",
			folder, triggeredBy, bucketPath, source, attrValue)
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
		glog.V(4).Infof("EmptyFolderCleaner: folder %s (triggered by %s) has %d items, not empty", folder, triggeredBy, count)
		return
	}

	// Skip explicitly created directory markers (e.g., PUT /bucket/folder/)
	// These have a MIME type set and should be preserved even when empty
	if isKeyObj, err := efc.filer.IsDirectoryKeyObject(ctx, util.FullPath(folder)); err != nil {
		glog.V(2).Infof("EmptyFolderCleaner: error checking directory key object %s: %v", folder, err)
		return
	} else if isKeyObj {
		glog.V(3).Infof("EmptyFolderCleaner: skipping %s (triggered by %s), explicit directory marker", folder, triggeredBy)
		return
	}

	// Read what it would take to put this folder back before removing it; without
	// that a restore would have to invent attributes for it.
	attrs, err := efc.filer.DirectoryAttributes(ctx, util.FullPath(folder))
	if err != nil {
		glog.V(2).Infof("EmptyFolderCleaner: cannot read %s before deleting it: %v", folder, err)
		return
	}

	// Observe it before the delete rather than after. A delete can fail partway and
	// still leave the folder gone - the redis stores remove the folder before their
	// parent-list member - so a failure return is not proof that it is still there.
	efc.mu.Lock()
	if efc.deleted == nil {
		efc.deleted = make(map[string]*deletedFolder)
	}
	efc.makeRoomForDeletedLocked()
	efc.deleted[folder] = &deletedFolder{path: folder, attrs: attrs, deletedAt: time.Now()}
	efc.mu.Unlock()

	glog.Infof("EmptyFolderCleaner: deleting empty folder %s (triggered by %s)", folder, triggeredBy)
	if err := efc.deleteFolder(ctx, folder); err != nil {
		glog.V(2).Infof("EmptyFolderCleaner: failed to delete empty folder %s (triggered by %s): %v", folder, triggeredBy, err)
		return
	}

	// Clean up cache entry
	efc.mu.Lock()
	delete(efc.folderCounts, folder)
	efc.mu.Unlock()

	// After deleting this folder, immediately try to clean the parent.
	// Relying solely on cascading metadata events would re-enter the full
	// delay queue for each ancestor level, causing multi-minute cascading
	// waits (e.g. 3 levels × 2m = 6m+).  Instead, walk up eagerly.
	parentDir, _ := util.FullPath(folder).DirAndName()
	if parentDir != "" && parentDir != folder &&
		efc.bucketPath != "" && isUnderBucketPath(parentDir, efc.bucketPath) {
		// Remove any pending queue entry for the parent so we don't
		// double-process it later from a stale event.
		efc.cleanupQueue.Remove(parentDir)
		efc.executeCleanup(parentDir, triggeredBy)
	}
}

// countItems counts items in a folder (up to maxCountCheck)
func (efc *EmptyFolderCleaner) countItems(ctx context.Context, folder string) (int, error) {
	return efc.filer.CountDirectoryEntries(ctx, util.FullPath(folder), efc.maxCountCheck)
}

// deleteFolder deletes an empty folder
func (efc *EmptyFolderCleaner) deleteFolder(ctx context.Context, folder string) error {
	return efc.filer.DeleteEntryMetaAndData(ctx, util.FullPath(folder), false, false, false, false, nil, 0)
}

func (efc *EmptyFolderCleaner) getBucketCleanupPolicy(ctx context.Context, folder string) (bucketPath string, autoRemove bool, source string, attrValue string, err error) {
	bucketPath, ok := util.ExtractBucketPath(efc.bucketPath, folder, true)
	if !ok {
		return "", true, "default", "<not_bucket_path>", nil
	}

	now := time.Now()

	efc.mu.RLock()
	if state, found := efc.bucketCleanupPolicies[bucketPath]; found && now.Sub(state.lastCheck) <= efc.cacheExpiry {
		efc.mu.RUnlock()
		return bucketPath, state.autoRemove, "cache", state.attrValue, nil
	}
	efc.mu.RUnlock()

	attrs, err := efc.filer.GetEntryAttributes(ctx, util.FullPath(bucketPath))
	if err != nil {
		return "", true, "", "", err
	}

	autoRemove, attrValue = autoRemoveEmptyFoldersEnabled(attrs)

	efc.mu.Lock()
	if efc.bucketCleanupPolicies == nil {
		efc.bucketCleanupPolicies = make(map[string]*bucketCleanupPolicyState)
	}
	efc.bucketCleanupPolicies[bucketPath] = &bucketCleanupPolicyState{
		autoRemove: autoRemove,
		attrValue:  attrValue,
		lastCheck:  now,
	}
	efc.mu.Unlock()

	return bucketPath, autoRemove, "filer", attrValue, nil
}

func autoRemoveEmptyFoldersEnabled(attrs map[string][]byte) (bool, string) {
	if attrs == nil {
		return true, "<no_attrs>"
	}

	value, found := attrs[s3_constants.ExtAllowEmptyFolders]
	if !found {
		return true, "<missing>"
	}

	text := strings.TrimSpace(string(value))
	if text == "" {
		return true, "<empty>"
	}

	return !strings.EqualFold(text, "true"), text
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

// isMultipartUploadsPath reports whether directory is the S3 multipart staging
// root <bucket>/.uploads or anything beneath it.
func isMultipartUploadsPath(bucketPath, directory string) bool {
	if bucketPath == "" {
		return false
	}
	dir, ok := util.ExtractBucketPath(bucketPath, directory, true)
	if !ok {
		return false
	}
	// requireChild guarantees directory starts with dir + "/", so slice past it.
	first, _, _ := strings.Cut(directory[len(dir)+1:], "/")
	return first == s3_constants.MultipartUploadsFolder
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

	for bucketPath, state := range efc.bucketCleanupPolicies {
		if now.Sub(state.lastCheck) > efc.cacheExpiry {
			delete(efc.bucketCleanupPolicies, bucketPath)
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
	efc.bucketCleanupPolicies = make(map[string]*bucketCleanupPolicyState)
	efc.deleted, efc.deletedDropped = make(map[string]*deletedFolder), 0
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
