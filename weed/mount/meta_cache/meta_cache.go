package meta_cache

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/leveldb"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// need to have logic similar to FilerStoreWrapper
// e.g. fill fileId field for chunks

type MetaCache struct {
	root         util.FullPath
	localStore   filer.VirtualFilerStore
	leveldbStore *leveldb.LevelDBStore // direct reference for batch operations
	sync.RWMutex
	uidGidMapper         *UidGidMapper
	markCachedFn         func(fullpath util.FullPath)
	isCachedFn           func(fullpath util.FullPath) bool
	invalidateFunc       func(EntryInvalidation)
	onDirectoryUpdate    func(dir util.FullPath)
	pinnedChildFn        func(*filer.Entry) bool // a child a rebuild must not drop (local-only, not yet on the filer); nil disables
	visitGroup           singleflight.Group      // deduplicates concurrent EnsureVisited calls for the same path
	applyCh              chan metadataApplyRequest
	applyDone            chan struct{}
	applyStateMu         sync.Mutex
	applyClosed          bool
	buildingDirs         map[util.FullPath]*directoryBuildState
	dedupRing            dedupRingBuffer
	includeSystemEntries bool

	// dirVersionFloors is each cached directory's listing snapshot: the
	// version of every child the listing covered, present or absent, unless
	// a later event gave that child its own record. One map write per build
	// instead of a record per child.
	dirVersionFloors map[util.FullPath]int64

	// Entry invalidations run on a worker, not inline on the apply loop:
	// invalidateFunc takes the fh lock, which a flush can hold while waiting on
	// the apply loop (flushMetadataToFiler -> applyLocalMetadataEvent), so inline
	// invalidation deadlocks the mount.
	invalidateWorker *util.AsyncBatchWorker[EntryInvalidation]
}

var errMetaCacheClosed = errors.New("metadata cache is shut down")

type MetadataResponseApplyOptions struct {
	NotifyDirectories bool
	InvalidateEntries bool
}

var (
	LocalMetadataResponseApplyOptions = MetadataResponseApplyOptions{
		NotifyDirectories: true,
	}
	SubscriberMetadataResponseApplyOptions = MetadataResponseApplyOptions{
		NotifyDirectories: true,
		InvalidateEntries: true,
	}
)

type directoryBuildState struct {
	bufferedEvents []*filer_pb.SubscribeMetadataResponse
}

const recentEventDedupWindow = 4096

type metadataApplyRequestKind int

const (
	metadataApplyEvent metadataApplyRequestKind = iota
	metadataBeginBuild
	metadataCompleteBuild
	metadataAbortBuild
	metadataPurgeDir
	metadataShutdown
)

type metadataApplyRequest struct {
	ctx          context.Context
	kind         metadataApplyRequestKind
	resp         *filer_pb.SubscribeMetadataResponse
	options      MetadataResponseApplyOptions
	buildPath    util.FullPath
	snapshotTsNs int64
	resetFn      func()
	done         chan error
}

func NewMetaCache(dbFolder string, uidGidMapper *UidGidMapper, root util.FullPath, includeSystemEntries bool,
	markCachedFn func(path util.FullPath), isCachedFn func(path util.FullPath) bool, invalidateFunc func(EntryInvalidation), onDirectoryUpdate func(dir util.FullPath)) *MetaCache {
	leveldbStore, virtualStore := openMetaStore(dbFolder)
	mc := &MetaCache{
		root:                 root,
		localStore:           virtualStore,
		leveldbStore:         leveldbStore,
		markCachedFn:         markCachedFn,
		isCachedFn:           isCachedFn,
		uidGidMapper:         uidGidMapper,
		onDirectoryUpdate:    onDirectoryUpdate,
		includeSystemEntries: includeSystemEntries,
		invalidateFunc:       invalidateFunc,
		applyCh:              make(chan metadataApplyRequest, 128),
		applyDone:            make(chan struct{}),
		buildingDirs:         make(map[util.FullPath]*directoryBuildState),
		dedupRing:            newDedupRingBuffer(),
		dirVersionFloors:     make(map[util.FullPath]int64),
	}
	mc.invalidateWorker = util.NewAsyncBatchWorker(func(batch []EntryInvalidation) {
		for _, invalidation := range batch {
			mc.invalidateFunc(invalidation)
		}
	})
	go mc.runApplyLoop()
	return mc
}

func openMetaStore(dbFolder string) (*leveldb.LevelDBStore, filer.VirtualFilerStore) {

	os.RemoveAll(dbFolder)
	os.MkdirAll(dbFolder, 0755)

	store := &leveldb.LevelDBStore{}
	config := &cacheConfig{
		dir: dbFolder,
	}

	if err := store.Initialize(config, ""); err != nil {
		glog.Fatalf("Failed to initialize metadata cache store for %s: %+v", store.GetName(), err)
	}

	return store, filer.NewFilerStoreWrapper(store)

}

func (mc *MetaCache) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	return mc.doInsertEntry(ctx, entry)
}

func (mc *MetaCache) doInsertEntry(ctx context.Context, entry *filer.Entry) error {
	if err := mc.localStore.InsertEntry(ctx, entry); err != nil {
		return err
	}
	mc.clearEntryVersionLocked(ctx, entry.FullPath)
	return nil
}

// doBatchInsertEntries inserts multiple entries using LevelDB's batch write.
// This is more efficient than inserting entries one by one.
func (mc *MetaCache) doBatchInsertEntries(ctx context.Context, entries []*filer.Entry) error {
	return mc.leveldbStore.BatchInsertEntries(ctx, entries)
}

func (mc *MetaCache) AtomicUpdateEntryFromFiler(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	return mc.atomicUpdateEntryFromFilerLocked(ctx, oldPath, newEntry, false, 0)
}

func (mc *MetaCache) atomicUpdateEntryFromFilerLocked(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry, allowUncachedInsert bool, versionTsNs int64) error {
	entry, err := mc.localStore.FindEntry(ctx, oldPath)
	if err != nil && err != filer_pb.ErrNotFound {
		glog.Errorf("Metacache: find entry error: %v", err)
		return err
	}
	vacatingOldPath := oldPath != "" && !(newEntry != nil && oldPath == newEntry.FullPath)
	if entry != nil && vacatingOldPath {
		ctx = context.WithValue(ctx, "OP", "MV")
		glog.V(3).Infof("DeleteEntry %s", oldPath)
		if err := mc.localStore.DeleteEntry(ctx, oldPath); err != nil {
			return err
		}
	}
	if vacatingOldPath {
		// A deletion is a fact about the path with no entry left to carry it:
		// without a tombstone a delayed older event resurrects it, and the
		// deletion's own redelivery is dedup-suppressed. Only for cached
		// parents — an uncached one gates its own inserts, so a tombstone
		// there would just accumulate.
		oldDir, _ := oldPath.DirAndName()
		if versionTsNs != 0 && (allowUncachedInsert || mc.isCachedFn(util.FullPath(oldDir))) {
			mc.setEntryTombstoneLocked(ctx, oldPath, versionTsNs)
		} else {
			mc.clearEntryVersionLocked(ctx, oldPath)
		}
	}

	if newEntry != nil {
		newDir, _ := newEntry.DirAndName()
		if allowUncachedInsert || mc.isCachedFn(util.FullPath(newDir)) {
			glog.V(3).Infof("InsertEntry %s/%s", newDir, newEntry.Name())
			if err := mc.localStore.InsertEntry(ctx, newEntry); err != nil {
				return err
			}
			mc.setEntryVersionLocked(ctx, newEntry.FullPath, versionTsNs)
		}
	}
	return nil
}

func (mc *MetaCache) shouldHideEntry(fullpath util.FullPath) bool {
	if mc.includeSystemEntries {
		return false
	}
	dir, name := fullpath.DirAndName()
	return IsHiddenSystemEntry(dir, name)
}

func (mc *MetaCache) purgeEntryLocked(ctx context.Context, fullpath util.FullPath, isDirectory bool) error {
	if fullpath == "" {
		return nil
	}
	if err := mc.localStore.DeleteEntry(ctx, fullpath); err != nil {
		return err
	}
	if isDirectory {
		if err := mc.localStore.DeleteFolderChildren(ctx, fullpath); err != nil {
			return err
		}
	}
	return nil
}

func (mc *MetaCache) ApplyMetadataResponse(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions) error {
	if resp == nil || resp.EventNotification == nil {
		return nil
	}
	clonedResp := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
	return mc.applyMetadataResponseEnqueue(ctx, clonedResp, options)
}

// ApplyMetadataResponseOwned is like ApplyMetadataResponse but takes ownership
// of resp without cloning. The caller must not use resp after this call.
func (mc *MetaCache) ApplyMetadataResponseOwned(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions) error {
	if resp == nil || resp.EventNotification == nil {
		return nil
	}
	return mc.applyMetadataResponseEnqueue(ctx, resp, options)
}

// ApplyMetadataResponseOwnedAsync enqueues resp without waiting, for callers holding a
// lock the apply loop's invalidateFunc also needs. Best-effort: the subscription re-delivers.
func (mc *MetaCache) ApplyMetadataResponseOwnedAsync(resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions) {
	if resp == nil || resp.EventNotification == nil {
		return
	}
	req := metadataApplyRequest{
		ctx:     context.Background(),
		kind:    metadataApplyEvent,
		resp:    resp,
		options: options,
		done:    make(chan error, 1),
	}
	select {
	case mc.applyCh <- req:
	default:
	}
}

func (mc *MetaCache) applyMetadataResponseEnqueue(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	req := metadataApplyRequest{
		// Use a non-cancellable context for the queued mutation so a
		// cancelled caller doesn't abort the apply loop mid-write.
		ctx:     context.Background(),
		kind:    metadataApplyEvent,
		resp:    resp,
		options: options,
		done:    make(chan error, 1),
	}

	if err := mc.enqueueApplyRequest(req); err != nil {
		return err
	}

	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (mc *MetaCache) BeginDirectoryBuild(ctx context.Context, dirPath util.FullPath) error {
	return mc.enqueueAndWait(ctx, metadataApplyRequest{
		kind:      metadataBeginBuild,
		buildPath: dirPath,
	})
}

func (mc *MetaCache) CompleteDirectoryBuild(ctx context.Context, dirPath util.FullPath, snapshotTsNs int64) error {
	return mc.enqueueAndWait(ctx, metadataApplyRequest{
		kind:         metadataCompleteBuild,
		buildPath:    dirPath,
		snapshotTsNs: snapshotTsNs,
	})
}

func (mc *MetaCache) AbortDirectoryBuild(ctx context.Context, dirPath util.FullPath) error {
	return mc.enqueueAndWait(ctx, metadataApplyRequest{
		kind:      metadataAbortBuild,
		buildPath: dirPath,
	})
}

// PurgeDirectoryChildren asynchronously clears a directory's cached children and
// resets its cached flag (resetFn) via the apply loop. Asynchronous so callers
// like kernel Forget don't block; see purgeDirectoryChildrenNow for why off-loop
// callers must route through here rather than wiping the store directly.
func (mc *MetaCache) PurgeDirectoryChildren(dirPath util.FullPath, resetFn func()) {
	_ = mc.enqueueApplyRequest(metadataApplyRequest{
		ctx:       context.Background(),
		kind:      metadataPurgeDir,
		buildPath: dirPath,
		resetFn:   resetFn,
		done:      make(chan error, 1),
	})
}

func (mc *MetaCache) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	if err := mc.localStore.UpdateEntry(ctx, entry); err != nil {
		return err
	}
	mc.clearEntryVersionLocked(ctx, entry.FullPath)
	return nil
}

// TouchDirMtimeCtime updates the mtime and ctime of a directory entry
// directly in the local metadata cache store. This avoids a filer RPC
// round-trip and the associated metadata event that would invalidate
// recently cached child entries.
func (mc *MetaCache) TouchDirMtimeCtime(ctx context.Context, dirPath util.FullPath, now time.Time) error {
	mc.Lock()
	defer mc.Unlock()
	entry, err := mc.localStore.FindEntry(ctx, dirPath)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil
	}
	entry.Attr.Mtime = now
	entry.Attr.Ctime = now
	if err := mc.localStore.UpdateEntry(ctx, entry); err != nil {
		return err
	}
	mc.clearEntryVersionLocked(ctx, dirPath)
	return nil
}

// FindEntry is the hot lookup/getattr path and deliberately skips the version
// record, sparing every cache hit a second store read. Callers needing the
// version use FindEntryWithVersion.
func (mc *MetaCache) FindEntry(ctx context.Context, fp util.FullPath) (entry *filer.Entry, err error) {
	mc.RLock()
	defer mc.RUnlock()
	entry, err = mc.localStore.FindEntry(ctx, fp)
	if err != nil {
		return nil, err
	}
	if isTtlExpired(entry) {
		return nil, filer_pb.ErrNotFound
	}
	mc.mapIdFromFilerToLocal(entry)
	return
}

// FindEntryWithVersion pairs the entry with the log position of the write
// that produced it, zero for an unversioned local write. Read under the lock
// the apply path writes both under, so the pair is consistent.
func (mc *MetaCache) FindEntryWithVersion(ctx context.Context, fp util.FullPath) (entry *filer.Entry, versionTsNs int64, err error) {
	mc.RLock()
	defer mc.RUnlock()
	entry, err = mc.localStore.FindEntry(ctx, fp)
	if err != nil {
		return nil, 0, err
	}
	if isTtlExpired(entry) {
		return nil, 0, filer_pb.ErrNotFound
	}
	mc.mapIdFromFilerToLocal(entry)
	versionTsNs = mc.entryVersionFloorLocked(fp, mc.getEntryVersionLocked(ctx, fp))
	return
}

// entryVersionKeyPrefix namespaces per-entry version records apart from entry
// keys. Keys encode parent-NUL-name, so a directory's direct children form one
// contiguous range: pruning scans exactly them, never a subtree.
const entryVersionKeyPrefix = "\x00mount.entry.ver\x00"

func entryVersionKey(fp util.FullPath) []byte {
	dir, name := fp.DirAndName()
	return []byte(entryVersionKeyPrefix + dir + "\x00" + name)
}

func entryVersionChildPrefix(dirPath util.FullPath) []byte {
	return []byte(entryVersionKeyPrefix + string(dirPath) + "\x00")
}

// setEntryVersionLocked records the log position an entry write reflects.
// Zero (an unversioned local write) clears the claim: the new content is not
// proven at the old position.
func (mc *MetaCache) setEntryVersionLocked(ctx context.Context, fp util.FullPath, tsNs int64) {
	if tsNs == 0 {
		mc.clearEntryVersionLocked(ctx, fp)
		return
	}
	value := make([]byte, 8)
	util.Uint64toBytes(value, uint64(tsNs))
	if err := mc.localStore.KvPut(ctx, entryVersionKey(fp), value); err != nil {
		glog.V(1).Infof("set entry version %s: %v", fp, err)
	}
}

// setEntryTombstoneLocked records a versioned deletion. The ninth byte marks
// a tombstone, which fences even with no entry present.
//
// Lifetime: a recreate at the same name overwrites the key, so repeating names
// are self-limiting; distinct names (unique temp files, rotated logs) each keep
// a record until the directory is rebuilt or evicted, which prunes everything
// at or below the new snapshot. The ceiling is therefore the number of
// distinct names deleted in a cached directory since its last listing.
func (mc *MetaCache) setEntryTombstoneLocked(ctx context.Context, fp util.FullPath, tsNs int64) {
	value := make([]byte, 9)
	util.Uint64toBytes(value, uint64(tsNs))
	value[8] = 1
	if err := mc.localStore.KvPut(ctx, entryVersionKey(fp), value); err != nil {
		glog.V(1).Infof("set entry tombstone %s: %v", fp, err)
	}
}

func (mc *MetaCache) clearEntryVersionLocked(ctx context.Context, fp util.FullPath) {
	if err := mc.localStore.KvDelete(ctx, entryVersionKey(fp)); err != nil {
		glog.V(4).Infof("clear entry version %s: %v", fp, err)
	}
}

func (mc *MetaCache) getEntryVersionLocked(ctx context.Context, fp util.FullPath) int64 {
	tsNs, _ := mc.getEntryVersionRecordLocked(ctx, fp)
	return tsNs
}

func (mc *MetaCache) getEntryVersionRecordLocked(ctx context.Context, fp util.FullPath) (tsNs int64, tombstone bool) {
	value, err := mc.localStore.KvGet(ctx, entryVersionKey(fp))
	if err != nil || len(value) < 8 {
		return 0, false
	}
	return int64(util.BytesToUint64(value[:8])), len(value) == 9 && value[8] == 1
}

// entryVersionBlocksLocked reports whether a write at tsNs is already
// reflected at fp. A tombstone fences with no entry present. Otherwise the
// path's version is its own record or, lacking one, its directory's listing
// floor — which covers children the listing saw present and absent alike.
// A plain record only counts while its entry exists: records linger after a
// bulk folder wipe and must not fence a recreate.
func (mc *MetaCache) entryVersionBlocksLocked(ctx context.Context, fp util.FullPath, tsNs int64) bool {
	recordTsNs, tombstone := mc.getEntryVersionRecordLocked(ctx, fp)
	if tombstone {
		return recordTsNs >= tsNs
	}
	if !mc.entryExistsLocked(ctx, fp) {
		recordTsNs = 0
	}
	return mc.entryVersionFloorLocked(fp, recordTsNs) >= tsNs
}

// entryExistsLocked reports whether fp has a live entry, applying the same TTL
// expiry the read path does so both agree on what "exists" means.
func (mc *MetaCache) entryExistsLocked(ctx context.Context, fp util.FullPath) bool {
	entry, err := mc.localStore.FindEntry(ctx, fp)
	if err != nil || entry == nil {
		return false
	}
	return !isTtlExpired(entry)
}

// entryVersionFloorLocked raises a path's own version record to its
// directory's listing floor: the listing covered every child at its snapshot,
// so a child without a later record of its own is versioned at the snapshot.
func (mc *MetaCache) entryVersionFloorLocked(fp util.FullPath, recordTsNs int64) int64 {
	dir, _ := fp.DirAndName()
	if floor := mc.dirVersionFloors[util.FullPath(dir)]; floor > recordTsNs {
		return floor
	}
	return recordTsNs
}

func isTtlExpired(entry *filer.Entry) bool {
	return entry.TtlSec > 0 && entry.Crtime.Add(time.Duration(entry.TtlSec)*time.Second).Before(time.Now())
}

// pruneSupersededTombstonesLocked drops direct-child tombstones at or below
// the listing snapshot: the absence floor now fences what they fenced.
// Tombstones above it (deletions the listing has not seen) survive; deeper
// descendants answer to their own directory's floor.
func (mc *MetaCache) pruneSupersededTombstonesLocked(ctx context.Context, dirPath util.FullPath, snapshotTsNs int64) {
	var superseded [][]byte
	if err := mc.leveldbStore.VisitKvPrefix(ctx, entryVersionChildPrefix(dirPath), func(key, value []byte) error {
		if len(value) != 9 || value[8] != 1 {
			return nil
		}
		if int64(util.BytesToUint64(value[:8])) <= snapshotTsNs {
			superseded = append(superseded, key)
		}
		return nil
	}); err != nil {
		glog.V(1).Infof("prune tombstones %s: %v", dirPath, err)
		return
	}
	for _, key := range superseded {
		if err := mc.localStore.KvDelete(ctx, key); err != nil {
			glog.V(1).Infof("prune tombstone %s: %v", string(key), err)
		}
	}
}

// deleteChildVersionRecordsLocked drops a directory's direct-child version
// records when it is evicted. An uncached directory reads through to the filer
// and gates its own inserts, so its records fence nothing — keeping them only
// grows the store. A rebuild re-derives the floor and tombstones.
func (mc *MetaCache) deleteChildVersionRecordsLocked(ctx context.Context, dirPath util.FullPath) {
	var keys [][]byte
	if err := mc.leveldbStore.VisitKvPrefix(ctx, entryVersionChildPrefix(dirPath), func(key, value []byte) error {
		keys = append(keys, key)
		return nil
	}); err != nil {
		glog.V(1).Infof("collect version records %s: %v", dirPath, err)
		return
	}
	for _, key := range keys {
		if err := mc.localStore.KvDelete(ctx, key); err != nil {
			glog.V(1).Infof("delete version record %s: %v", string(key), err)
		}
	}
}

func (mc *MetaCache) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	if err = mc.localStore.DeleteEntry(ctx, fp); err != nil {
		return err
	}
	mc.clearEntryVersionLocked(ctx, fp)
	return nil
}
func (mc *MetaCache) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	delete(mc.dirVersionFloors, fp)
	mc.deleteChildVersionRecordsLocked(ctx, fp)
	return mc.localStore.DeleteFolderChildren(ctx, fp)
}

// SetPinnedChildFn installs a predicate reporting whether a child holds
// local-only state a rebuild must not discard. See deleteFolderChildrenForRebuild.
func (mc *MetaCache) SetPinnedChildFn(fn func(*filer.Entry) bool) {
	mc.pinnedChildFn = fn
}

// deleteFolderChildrenForRebuild clears a directory's cached children ahead of a
// rebuild, but keeps any child flagged pinned by pinnedChildFn — a local-only
// create not yet flushed to the filer. A rebuild refills from a filer listing
// that does not include such a create; a blind wipe would drop it and then
// markCachedFn publishes the directory authoritatively cached without a file the
// client created, so it vanishes from the mount.
func (mc *MetaCache) deleteFolderChildrenForRebuild(ctx context.Context, dirPath util.FullPath) error {
	mc.Lock()
	defer mc.Unlock()
	if mc.pinnedChildFn == nil {
		return mc.localStore.DeleteFolderChildren(ctx, dirPath)
	}
	var pinned []*filer.Entry
	if _, err := mc.localStore.ListDirectoryEntries(ctx, dirPath, "", true, math.MaxInt64, func(entry *filer.Entry) (bool, error) {
		if mc.pinnedChildFn(entry) {
			pinned = append(pinned, entry)
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err := mc.localStore.DeleteFolderChildren(ctx, dirPath); err != nil {
		return err
	}
	if len(pinned) > 0 {
		return mc.doBatchInsertEntries(ctx, pinned)
	}
	return nil
}

func (mc *MetaCache) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) error {
	mc.RLock()
	defer mc.RUnlock()

	if !mc.isCachedFn(dirPath) {
		// if this request comes after renaming, it should be fine
		glog.Warningf("unsynchronized dir: %v", dirPath)
	}

	_, err := mc.localStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, func(entry *filer.Entry) (bool, error) {
		if entry.TtlSec > 0 && entry.Crtime.Add(time.Duration(entry.TtlSec)*time.Second).Before(time.Now()) {
			return true, nil
		}
		mc.mapIdFromFilerToLocal(entry)
		return eachEntryFunc(entry)
	})
	if err != nil {
		return err
	}
	return err
}

func (mc *MetaCache) Shutdown() {
	done := make(chan error, 1)

	mc.applyStateMu.Lock()
	if !mc.applyClosed {
		mc.applyClosed = true
		mc.applyCh <- metadataApplyRequest{
			kind: metadataShutdown,
			done: done,
		}
	}
	mc.applyStateMu.Unlock()

	select {
	case <-done:
	case <-mc.applyDone:
	}

	<-mc.applyDone

	// The apply loop is the only dispatcher of entry invalidations; with it
	// stopped, drain and stop the invalidate worker before closing the store.
	mc.invalidateWorker.Shutdown()

	mc.Lock()
	defer mc.Unlock()
	mc.localStore.Shutdown()
}

func (mc *MetaCache) mapIdFromFilerToLocal(entry *filer.Entry) {
	entry.Attr.Uid, entry.Attr.Gid = mc.uidGidMapper.FilerToLocal(entry.Attr.Uid, entry.Attr.Gid)
}

func (mc *MetaCache) Debug() {
	if debuggable, ok := mc.localStore.(filer.Debuggable); ok {
		println("start debugging")
		debuggable.Debug(os.Stderr)
	}
}

// IsDirectoryCached returns true if the directory has been fully cached
// (i.e., all entries have been loaded via EnsureVisited or ReadDir).
func (mc *MetaCache) IsDirectoryCached(dirPath util.FullPath) bool {
	return mc.isCachedFn(dirPath)
}

func (mc *MetaCache) noteDirectoryUpdate(dirPath util.FullPath) {
	if mc.onDirectoryUpdate != nil {
		mc.onDirectoryUpdate(dirPath)
	}
}

func (mc *MetaCache) enqueueAndWait(ctx context.Context, req metadataApplyRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Use a non-cancellable context for the queued operation so a
	// cancelled caller doesn't abort a build/complete mid-way.
	req.ctx = context.Background()
	req.done = make(chan error, 1)
	if err := mc.enqueueApplyRequest(req); err != nil {
		return err
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (mc *MetaCache) enqueueApplyRequest(req metadataApplyRequest) error {
	mc.applyStateMu.Lock()
	if mc.applyClosed {
		mc.applyStateMu.Unlock()
		return errMetaCacheClosed
	}
	// Release the mutex before the potentially-blocking channel send so that
	// Shutdown can still acquire it to set applyClosed when the channel is full.
	mc.applyStateMu.Unlock()
	select {
	case mc.applyCh <- req:
		return nil
	case <-mc.applyDone:
		return errMetaCacheClosed
	}
}

func (mc *MetaCache) runApplyLoop() {
	defer close(mc.applyDone)

	for req := range mc.applyCh {
		req.done <- mc.handleApplyRequest(req)
		close(req.done)
		if req.kind == metadataShutdown {
			mc.drainApplyCh()
			return
		}
	}
}

// drainApplyCh non-blockingly drains any remaining requests from applyCh
// after a shutdown sentinel, signalling each caller so they don't block.
func (mc *MetaCache) drainApplyCh() {
	for {
		select {
		case req := <-mc.applyCh:
			req.done <- errMetaCacheClosed
			close(req.done)
		default:
			return
		}
	}
}

func (mc *MetaCache) handleApplyRequest(req metadataApplyRequest) error {
	switch req.kind {
	case metadataApplyEvent:
		return mc.applyMetadataResponseNow(req.ctx, req.resp, req.options)
	case metadataBeginBuild:
		return mc.beginDirectoryBuildNow(req.buildPath)
	case metadataCompleteBuild:
		return mc.completeDirectoryBuildNow(req.ctx, req.buildPath, req.snapshotTsNs)
	case metadataAbortBuild:
		return mc.abortDirectoryBuildNow(req.buildPath)
	case metadataPurgeDir:
		return mc.purgeDirectoryChildrenNow(req.ctx, req.buildPath, req.resetFn)
	case metadataShutdown:
		return nil
	default:
		return nil
	}
}

// EntryInvalidation describes one path's metadata change for an open-handle
// refresh.
type EntryInvalidation struct {
	Path    util.FullPath
	Entry   *filer_pb.Entry // entry now at path per the event; nil when the path was vacated
	TsNs    int64           // the event's filer log position; 0 for locally built events
	Deleted bool            // vacated by a delete, not a rename away — the file lives on elsewhere
	// RenamedTo is the destination when a rename vacated this path: the file
	// lives on there, so an open handle follows it rather than being orphaned.
	RenamedTo util.FullPath
	// Signatures from the event. The filer that logged it appends its own, so
	// this identifies the clock domain TsNs belongs to.
	Signatures []int32
}

type metadataResponseSideEffects struct {
	dirsToNotify  []util.FullPath
	invalidations []EntryInvalidation
}

func (mc *MetaCache) applyMetadataResponseNow(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions) error {
	if mc.shouldSkipDuplicateEvent(resp) {
		return nil
	}

	immediateEvents, bufferedEvents := mc.routeMetadataResponse(resp)
	if len(bufferedEvents) == 0 {
		return mc.applyMetadataResponseDirect(ctx, resp, options, false)
	}

	for _, immediateEvent := range immediateEvents {
		if err := mc.applyMetadataResponseDirect(ctx, immediateEvent, MetadataResponseApplyOptions{}, false); err != nil {
			return err
		}
	}
	// Apply side effects but skip directory notifications for dirs that are
	// currently being built. Notifying a building dir can trigger
	// markDirectoryReadThrough → DeleteFolderChildren, wiping entries that
	// EnsureVisited already inserted, leaving an incomplete cache.
	mc.applyMetadataSideEffectsSkippingBuildingDirs(resp, options)
	for buildDir, events := range bufferedEvents {
		state := mc.buildingDirs[buildDir]
		if state == nil {
			continue
		}
		state.bufferedEvents = append(state.bufferedEvents, events...)
	}
	return nil
}

func (mc *MetaCache) applyMetadataResponseDirect(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions, allowUncachedInsert bool) error {
	if _, err := mc.applyMetadataResponseLocked(ctx, resp, options, allowUncachedInsert); err != nil {
		return err
	}
	mc.applyMetadataSideEffects(resp, options)
	return nil
}

func (mc *MetaCache) applyMetadataSideEffects(resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions) {
	sideEffects := metadataResponseSideEffects{}
	if options.NotifyDirectories {
		sideEffects.dirsToNotify = collectDirectoryNotifications(resp)
	}
	if options.InvalidateEntries {
		sideEffects.invalidations = collectEntryInvalidations(resp)
	}
	for _, dirPath := range sideEffects.dirsToNotify {
		mc.noteDirectoryUpdate(dirPath)
	}
	mc.invalidateWorker.Enqueue(sideEffects.invalidations...)
}

// applyMetadataSideEffectsSkippingBuildingDirs is like applyMetadataSideEffects
// but suppresses directory notifications for dirs currently in buildingDirs.
// This prevents markDirectoryReadThrough from wiping entries mid-build.
func (mc *MetaCache) applyMetadataSideEffectsSkippingBuildingDirs(resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions) {
	sideEffects := metadataResponseSideEffects{}
	if options.NotifyDirectories {
		sideEffects.dirsToNotify = collectDirectoryNotifications(resp)
	}
	if options.InvalidateEntries {
		sideEffects.invalidations = collectEntryInvalidations(resp)
	}
	for _, dirPath := range sideEffects.dirsToNotify {
		if _, building := mc.buildingDirs[dirPath]; !building {
			mc.noteDirectoryUpdate(dirPath)
		}
	}
	mc.invalidateWorker.Enqueue(sideEffects.invalidations...)
}

// WaitForEntryInvalidations blocks until every invalidation enqueued so far
// has been processed by the invalidate worker. Intended for tests and
// shutdown paths that need the previously-synchronous behavior.
func (mc *MetaCache) WaitForEntryInvalidations() {
	mc.invalidateWorker.Drain()
}

func (mc *MetaCache) applyMetadataResponseLocked(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions, allowUncachedInsert bool) (metadataResponseSideEffects, error) {
	message := resp.GetEventNotification()
	if message == nil {
		return metadataResponseSideEffects{}, nil
	}

	var oldPath util.FullPath
	var newPath util.FullPath
	var newEntry *filer.Entry
	hideNewPath := false
	if message.OldEntry != nil {
		oldPath = util.NewFullPath(resp.Directory, message.OldEntry.Name)
	}

	if message.NewEntry != nil {
		dir := resp.Directory
		if message.NewParentPath != "" {
			dir = message.NewParentPath
		}
		newPath = util.NewFullPath(dir, message.NewEntry.Name)
		hideNewPath = mc.shouldHideEntry(newPath)
		if !hideNewPath {
			newEntry = filer.FromPbEntry(dir, message.NewEntry)
		}
	}

	mc.Lock()
	// Last-writer-wins per entry: an event at or below an entry's version is
	// already reflected in it, and applying it would roll the entry back while
	// the version keeps the newer claim. Each half is gated independently.
	if resp.TsNs != 0 {
		if oldPath != "" && mc.entryVersionBlocksLocked(ctx, oldPath, resp.TsNs) {
			oldPath = ""
		}
		if newEntry != nil && mc.entryVersionBlocksLocked(ctx, newEntry.FullPath, resp.TsNs) {
			newEntry = nil
		}
	}
	err := mc.atomicUpdateEntryFromFilerLocked(ctx, oldPath, newEntry, allowUncachedInsert, resp.TsNs)
	if err == nil && hideNewPath {
		if purgeErr := mc.purgeEntryLocked(ctx, newPath, message.NewEntry.IsDirectory); purgeErr != nil {
			err = purgeErr
		}
	}
	// When a directory is deleted or moved, remove its cached descendants
	// so stale children cannot be served from the local cache.
	if err == nil && oldPath != "" && message.OldEntry != nil && message.OldEntry.IsDirectory {
		isDelete := message.NewEntry == nil
		isMove := message.NewEntry != nil && (message.NewParentPath != resp.Directory || message.NewEntry.Name != message.OldEntry.Name)
		if isDelete || isMove {
			if deleteErr := mc.localStore.DeleteFolderChildren(ctx, oldPath); deleteErr != nil {
				glog.V(2).Infof("delete descendants of %s: %v", oldPath, deleteErr)
			}
		}
	}
	mc.Unlock()
	if err != nil {
		return metadataResponseSideEffects{}, err
	}
	return metadataResponseSideEffects{}, nil
}

func (mc *MetaCache) beginDirectoryBuildNow(dirPath util.FullPath) error {
	if _, found := mc.buildingDirs[dirPath]; found {
		return nil
	}
	mc.buildingDirs[dirPath] = &directoryBuildState{}
	return nil
}

func (mc *MetaCache) abortDirectoryBuildNow(dirPath util.FullPath) error {
	delete(mc.buildingDirs, dirPath)
	return nil
}

// purgeDirectoryChildrenNow runs in the apply loop, serialized with
// completeDirectoryBuildNow's markCachedFn, so no build publish interleaves
// between resetFn (clears the cached flag) and the store wipe. Skipping a
// building directory avoids deleting entries the build inserted but hasn't yet
// published. Together these keep a directory from ending up flagged cached over
// an empty store — which hides every file in it though they remain on the filer.
func (mc *MetaCache) purgeDirectoryChildrenNow(ctx context.Context, dirPath util.FullPath, resetFn func()) error {
	if mc.isBuildingDir(dirPath) {
		return nil
	}
	if resetFn != nil {
		resetFn()
	}
	mc.Lock()
	defer mc.Unlock()
	delete(mc.dirVersionFloors, dirPath)
	mc.deleteChildVersionRecordsLocked(ctx, dirPath)
	return mc.localStore.DeleteFolderChildren(ctx, dirPath)
}

func (mc *MetaCache) completeDirectoryBuildNow(ctx context.Context, dirPath util.FullPath, snapshotTsNs int64) error {
	state := mc.buildingDirs[dirPath]
	delete(mc.buildingDirs, dirPath)

	if state == nil {
		return nil
	}

	// The listing covered every child at its snapshot, so one directory floor
	// versions them all — a child only needs its own record once a later event
	// touches it. An unversioned listing (pre-upgrade filer) instead clears the
	// children's records, or a re-inserted entry would inherit a stale one.
	mc.Lock()
	if snapshotTsNs != 0 {
		mc.dirVersionFloors[dirPath] = snapshotTsNs
		mc.pruneSupersededTombstonesLocked(ctx, dirPath, snapshotTsNs)
	} else {
		delete(mc.dirVersionFloors, dirPath)
		mc.deleteChildVersionRecordsLocked(ctx, dirPath)
	}
	mc.Unlock()

	for _, event := range state.bufferedEvents {
		// When the server provided a snapshot timestamp, skip events that
		// the listing already included. When snapshotTsNs == 0 (empty
		// directory — server returned no entries and no snapshot), replay
		// ALL buffered events to avoid dropping mutations due to
		// client/server clock skew.
		if snapshotTsNs != 0 && event.TsNs != 0 && event.TsNs <= snapshotTsNs {
			continue
		}
		if err := mc.applyMetadataResponseDirect(ctx, event, MetadataResponseApplyOptions{}, true); err != nil {
			return err
		}
	}

	mc.markCachedFn(dirPath)

	// Re-invalidate every buffered event: each ran against a mid-build store,
	// so a handle can hold older state than the completed directory. After
	// markCachedFn, versioned at the snapshot to outrank the mid-build install.
	for _, event := range state.bufferedEvents {
		if event.TsNs < snapshotTsNs {
			event.TsNs = snapshotTsNs
		}
		mc.applyMetadataSideEffects(event, MetadataResponseApplyOptions{InvalidateEntries: true})
	}
	return nil
}

func (mc *MetaCache) routeMetadataResponse(resp *filer_pb.SubscribeMetadataResponse) ([]*filer_pb.SubscribeMetadataResponse, map[util.FullPath][]*filer_pb.SubscribeMetadataResponse) {
	message := resp.GetEventNotification()
	if message == nil {
		return []*filer_pb.SubscribeMetadataResponse{resp}, nil
	}

	oldDir, hasOld := metadataOldParentDir(resp)
	newDir, hasNew := metadataNewParentDir(resp)
	oldBuilding := hasOld && mc.isBuildingDir(oldDir)
	newBuilding := hasNew && mc.isBuildingDir(newDir)
	if !oldBuilding && !newBuilding {
		return []*filer_pb.SubscribeMetadataResponse{resp}, nil
	}

	bufferedEvents := make(map[util.FullPath][]*filer_pb.SubscribeMetadataResponse)
	var immediateEvents []*filer_pb.SubscribeMetadataResponse

	if hasOld && hasNew && oldDir != newDir {
		deleteEvent := metadataDeleteFragment(resp)
		createEvent := metadataCreateFragment(resp)
		if oldBuilding {
			bufferedEvents[oldDir] = append(bufferedEvents[oldDir], deleteEvent)
		} else {
			immediateEvents = append(immediateEvents, deleteEvent)
		}
		if newBuilding {
			bufferedEvents[newDir] = append(bufferedEvents[newDir], createEvent)
		} else {
			immediateEvents = append(immediateEvents, createEvent)
		}
		return immediateEvents, bufferedEvents
	}

	targetDir := newDir
	if hasOld {
		targetDir = oldDir
	}
	if mc.isBuildingDir(targetDir) {
		bufferedEvents[targetDir] = append(bufferedEvents[targetDir], resp)
		return nil, bufferedEvents
	}
	return []*filer_pb.SubscribeMetadataResponse{resp}, nil
}

func (mc *MetaCache) isBuildingDir(dirPath util.FullPath) bool {
	_, found := mc.buildingDirs[dirPath]
	return found
}

func metadataOldParentDir(resp *filer_pb.SubscribeMetadataResponse) (util.FullPath, bool) {
	if resp.GetEventNotification() == nil || resp.EventNotification.OldEntry == nil {
		return "", false
	}
	return util.FullPath(resp.Directory), true
}

func metadataNewParentDir(resp *filer_pb.SubscribeMetadataResponse) (util.FullPath, bool) {
	if resp.GetEventNotification() == nil || resp.EventNotification.NewEntry == nil {
		return "", false
	}
	newDir := resp.Directory
	if resp.EventNotification.NewParentPath != "" {
		newDir = resp.EventNotification.NewParentPath
	}
	return util.FullPath(newDir), true
}

func metadataDeleteFragment(resp *filer_pb.SubscribeMetadataResponse) *filer_pb.SubscribeMetadataResponse {
	if resp.GetEventNotification() == nil || resp.EventNotification.OldEntry == nil {
		return nil
	}
	return &filer_pb.SubscribeMetadataResponse{
		Directory: resp.Directory,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: proto.Clone(resp.EventNotification.OldEntry).(*filer_pb.Entry),
		},
		TsNs: resp.TsNs,
	}
}

func metadataCreateFragment(resp *filer_pb.SubscribeMetadataResponse) *filer_pb.SubscribeMetadataResponse {
	if resp.GetEventNotification() == nil || resp.EventNotification.NewEntry == nil {
		return nil
	}
	newDir := resp.Directory
	if resp.EventNotification.NewParentPath != "" {
		newDir = resp.EventNotification.NewParentPath
	}
	return &filer_pb.SubscribeMetadataResponse{
		Directory: newDir,
		EventNotification: &filer_pb.EventNotification{
			NewEntry:      proto.Clone(resp.EventNotification.NewEntry).(*filer_pb.Entry),
			NewParentPath: newDir,
		},
		TsNs: resp.TsNs,
	}
}

func metadataEventDedupKey(resp *filer_pb.SubscribeMetadataResponse) string {
	var oldName, newName, newParent string
	hasOld, hasNew := false, false
	if msg := resp.GetEventNotification(); msg != nil {
		if msg.OldEntry != nil {
			oldName = msg.OldEntry.Name
			hasOld = true
		}
		if msg.NewEntry != nil {
			newName = msg.NewEntry.Name
			hasNew = true
			newParent = msg.NewParentPath
		}
	}
	// Encode event shape (create/delete/update/rename) so structurally
	// different events with the same names are not collapsed.
	var shape byte
	switch {
	case hasOld && hasNew:
		if resp.Directory != newParent && newParent != "" {
			shape = 'R' // rename across directories
		} else {
			shape = 'U' // update in place
		}
	case hasOld:
		shape = 'D' // delete
	case hasNew:
		shape = 'C' // create
	}
	return fmt.Sprintf("%d|%c|%s|%s|%s|%s", resp.TsNs, shape, resp.Directory, oldName, newParent, newName)
}

func (mc *MetaCache) shouldSkipDuplicateEvent(resp *filer_pb.SubscribeMetadataResponse) bool {
	if resp == nil || resp.TsNs == 0 {
		return false
	}
	key := metadataEventDedupKey(resp)
	return !mc.dedupRing.Add(key)
}

type dedupRingBuffer struct {
	keys [recentEventDedupWindow]string
	head int
	size int
	set  map[string]struct{}
}

func newDedupRingBuffer() dedupRingBuffer {
	return dedupRingBuffer{
		set: make(map[string]struct{}, recentEventDedupWindow),
	}
}

func (r *dedupRingBuffer) Add(key string) bool {
	if _, found := r.set[key]; found {
		return false // duplicate
	}
	if r.size == recentEventDedupWindow {
		evicted := r.keys[r.head]
		delete(r.set, evicted)
	} else {
		r.size++
	}
	r.keys[r.head] = key
	r.set[key] = struct{}{}
	r.head = (r.head + 1) % recentEventDedupWindow
	return true // new entry
}

func collectDirectoryNotifications(resp *filer_pb.SubscribeMetadataResponse) []util.FullPath {
	message := resp.GetEventNotification()
	if message == nil {
		return nil
	}

	// At most 3 dirs: old parent, new parent, new child (if directory).
	// Use a fixed slice with linear dedup to avoid map allocation.
	var dirs [3]util.FullPath
	n := 0
	addUnique := func(p util.FullPath) {
		for i := 0; i < n; i++ {
			if dirs[i] == p {
				return
			}
		}
		dirs[n] = p
		n++
	}

	if message.OldEntry != nil {
		oldPath := util.NewFullPath(resp.Directory, message.OldEntry.Name)
		parent, _ := oldPath.DirAndName()
		addUnique(util.FullPath(parent))
	}
	if message.NewEntry != nil {
		newDir := resp.Directory
		if message.NewParentPath != "" {
			newDir = message.NewParentPath
		}
		newPath := util.NewFullPath(newDir, message.NewEntry.Name)
		parent, _ := newPath.DirAndName()
		addUnique(util.FullPath(parent))
		if message.NewEntry.IsDirectory {
			addUnique(newPath)
		}
	}

	return dirs[:n]
}

func collectEntryInvalidations(resp *filer_pb.SubscribeMetadataResponse) []EntryInvalidation {
	message := resp.GetEventNotification()
	if message == nil {
		return nil
	}

	var invalidations []EntryInvalidation
	signatures := message.Signatures
	if message.OldEntry != nil && message.NewEntry != nil {
		oldKey := util.NewFullPath(resp.Directory, message.OldEntry.Name)
		// Normalize NewParentPath: empty means same directory as resp.Directory
		newDir := resp.Directory
		if message.NewParentPath != "" {
			newDir = message.NewParentPath
		}
		if message.OldEntry.Name != message.NewEntry.Name || resp.Directory != newDir {
			newKey := util.NewFullPath(newDir, message.NewEntry.Name)
			invalidations = append(invalidations, EntryInvalidation{Path: oldKey, TsNs: resp.TsNs, Signatures: signatures, RenamedTo: newKey})
			invalidations = append(invalidations, EntryInvalidation{Path: newKey, Entry: message.NewEntry, TsNs: resp.TsNs, Signatures: signatures})
		} else {
			invalidations = append(invalidations, EntryInvalidation{Path: oldKey, Entry: message.NewEntry, TsNs: resp.TsNs, Signatures: signatures})
		}
		return invalidations
	}

	if filer_pb.IsCreate(resp) && message.NewEntry != nil {
		newDir := resp.Directory
		if message.NewParentPath != "" {
			newDir = message.NewParentPath
		}
		newKey := util.NewFullPath(newDir, message.NewEntry.Name)
		invalidations = append(invalidations, EntryInvalidation{Path: newKey, Entry: message.NewEntry, TsNs: resp.TsNs, Signatures: signatures})
	}

	if filer_pb.IsDelete(resp) && message.OldEntry != nil {
		oldKey := util.NewFullPath(resp.Directory, message.OldEntry.Name)
		invalidations = append(invalidations, EntryInvalidation{Path: oldKey, TsNs: resp.TsNs, Deleted: true, Signatures: signatures})
	}

	return invalidations
}
