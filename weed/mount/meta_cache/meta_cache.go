package meta_cache

import (
	"context"
	"errors"
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
	uidGidMapper      *UidGidMapper
	markCachedFn      func(fullpath util.FullPath)
	isCachedFn        func(fullpath util.FullPath) bool
	invalidateFunc    func(fullpath util.FullPath, entry *filer_pb.Entry)
	onDirectoryUpdate func(dir util.FullPath)
	visitGroup        singleflight.Group // deduplicates concurrent EnsureVisited calls for the same path
	applyCh           chan metadataApplyRequest
	applyDone         chan struct{}
	applyStateMu      sync.Mutex
	applyClosed       bool
	buildingDirs map[util.FullPath]*directoryBuildState
	dedupRing    dedupRingBuffer
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
	metadataShutdown
)

type metadataApplyRequest struct {
	ctx          context.Context
	kind         metadataApplyRequestKind
	resp         *filer_pb.SubscribeMetadataResponse
	options      MetadataResponseApplyOptions
	buildPath    util.FullPath
	snapshotTsNs int64
	done         chan error
}

func NewMetaCache(dbFolder string, uidGidMapper *UidGidMapper, root util.FullPath,
	markCachedFn func(path util.FullPath), isCachedFn func(path util.FullPath) bool, invalidateFunc func(util.FullPath, *filer_pb.Entry), onDirectoryUpdate func(dir util.FullPath)) *MetaCache {
	leveldbStore, virtualStore := openMetaStore(dbFolder)
	mc := &MetaCache{
		root:              root,
		localStore:        virtualStore,
		leveldbStore:      leveldbStore,
		markCachedFn:      markCachedFn,
		isCachedFn:        isCachedFn,
		uidGidMapper:      uidGidMapper,
		onDirectoryUpdate: onDirectoryUpdate,
		invalidateFunc: func(fullpath util.FullPath, entry *filer_pb.Entry) {
			invalidateFunc(fullpath, entry)
		},
		applyCh:        make(chan metadataApplyRequest, 128),
		applyDone:      make(chan struct{}),
		buildingDirs: make(map[util.FullPath]*directoryBuildState),
		dedupRing:    newDedupRingBuffer(),
	}
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
	return mc.localStore.InsertEntry(ctx, entry)
}

// doBatchInsertEntries inserts multiple entries using LevelDB's batch write.
// This is more efficient than inserting entries one by one.
func (mc *MetaCache) doBatchInsertEntries(ctx context.Context, entries []*filer.Entry) error {
	return mc.leveldbStore.BatchInsertEntries(ctx, entries)
}

func (mc *MetaCache) AtomicUpdateEntryFromFiler(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	return mc.atomicUpdateEntryFromFilerLocked(ctx, oldPath, newEntry, false)
}

func (mc *MetaCache) atomicUpdateEntryFromFilerLocked(ctx context.Context, oldPath util.FullPath, newEntry *filer.Entry, allowUncachedInsert bool) error {
	entry, err := mc.localStore.FindEntry(ctx, oldPath)
	if err != nil && err != filer_pb.ErrNotFound {
		glog.Errorf("Metacache: find entry error: %v", err)
		return err
	}
	if entry != nil {
		if oldPath != "" {
			if newEntry != nil && oldPath == newEntry.FullPath {
				// skip the unnecessary deletion
				// leave the update to the following InsertEntry operation
			} else {
				ctx = context.WithValue(ctx, "OP", "MV")
				glog.V(3).Infof("DeleteEntry %s", oldPath)
				if err := mc.localStore.DeleteEntry(ctx, oldPath); err != nil {
					return err
				}
			}
		}
	} else {
		// println("unknown old directory:", oldDir)
	}

	if newEntry != nil {
		newDir, _ := newEntry.DirAndName()
		if allowUncachedInsert || mc.isCachedFn(util.FullPath(newDir)) {
			glog.V(3).Infof("InsertEntry %s/%s", newDir, newEntry.Name())
			if err := mc.localStore.InsertEntry(ctx, newEntry); err != nil {
				return err
			}
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

func (mc *MetaCache) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	mc.Lock()
	defer mc.Unlock()
	return mc.localStore.UpdateEntry(ctx, entry)
}

func (mc *MetaCache) FindEntry(ctx context.Context, fp util.FullPath) (entry *filer.Entry, err error) {
	mc.RLock()
	defer mc.RUnlock()
	entry, err = mc.localStore.FindEntry(ctx, fp)
	if err != nil {
		return nil, err
	}
	if entry.TtlSec > 0 && entry.Crtime.Add(time.Duration(entry.TtlSec)*time.Second).Before(time.Now()) {
		return nil, filer_pb.ErrNotFound
	}
	mc.mapIdFromFilerToLocal(entry)
	return
}

func (mc *MetaCache) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	return mc.localStore.DeleteEntry(ctx, fp)
}
func (mc *MetaCache) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	mc.Lock()
	defer mc.Unlock()
	return mc.localStore.DeleteFolderChildren(ctx, fp)
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
	case metadataShutdown:
		return nil
	default:
		return nil
	}
}

type metadataInvalidation struct {
	path  util.FullPath
	entry *filer_pb.Entry
}

type metadataResponseSideEffects struct {
	dirsToNotify  []util.FullPath
	invalidations []metadataInvalidation
}

func (mc *MetaCache) applyMetadataResponseNow(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, options MetadataResponseApplyOptions) error {
	if mc.shouldSkipDuplicateEvent(resp) {
		return nil
	}

	immediateEvents, bufferedEvents := mc.routeMetadataResponse(resp)
	if len(bufferedEvents) == 0 {
		return mc.applyMetadataResponseDirect(ctx, resp, options, false)
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
	for _, immediateEvent := range immediateEvents {
		if err := mc.applyMetadataResponseDirect(ctx, immediateEvent, MetadataResponseApplyOptions{}, false); err != nil {
			return err
		}
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
	for _, invalidation := range sideEffects.invalidations {
		mc.invalidateFunc(invalidation.path, invalidation.entry)
	}
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
	for _, invalidation := range sideEffects.invalidations {
		mc.invalidateFunc(invalidation.path, invalidation.entry)
	}
}

func (mc *MetaCache) applyMetadataResponseLocked(ctx context.Context, resp *filer_pb.SubscribeMetadataResponse, _ MetadataResponseApplyOptions, allowUncachedInsert bool) (metadataResponseSideEffects, error) {
	message := resp.GetEventNotification()
	if message == nil {
		return metadataResponseSideEffects{}, nil
	}

	var oldPath util.FullPath
	var newEntry *filer.Entry
	if message.OldEntry != nil {
		oldPath = util.NewFullPath(resp.Directory, message.OldEntry.Name)
	}

	if message.NewEntry != nil {
		dir := resp.Directory
		if message.NewParentPath != "" {
			dir = message.NewParentPath
		}
		newEntry = filer.FromPbEntry(dir, message.NewEntry)
	}

	mc.Lock()
	err := mc.atomicUpdateEntryFromFilerLocked(ctx, oldPath, newEntry, allowUncachedInsert)
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

func (mc *MetaCache) completeDirectoryBuildNow(ctx context.Context, dirPath util.FullPath, snapshotTsNs int64) error {
	state := mc.buildingDirs[dirPath]
	delete(mc.buildingDirs, dirPath)

	if state == nil {
		return nil
	}

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

func collectEntryInvalidations(resp *filer_pb.SubscribeMetadataResponse) []metadataInvalidation {
	message := resp.GetEventNotification()
	if message == nil {
		return nil
	}

	var invalidations []metadataInvalidation
	if message.OldEntry != nil && message.NewEntry != nil {
		oldKey := util.NewFullPath(resp.Directory, message.OldEntry.Name)
		invalidations = append(invalidations, metadataInvalidation{path: oldKey, entry: message.OldEntry})
		// Normalize NewParentPath: empty means same directory as resp.Directory
		newDir := resp.Directory
		if message.NewParentPath != "" {
			newDir = message.NewParentPath
		}
		if message.OldEntry.Name != message.NewEntry.Name || resp.Directory != newDir {
			newKey := util.NewFullPath(newDir, message.NewEntry.Name)
			invalidations = append(invalidations, metadataInvalidation{path: newKey, entry: message.NewEntry})
		}
		return invalidations
	}

	if filer_pb.IsCreate(resp) && message.NewEntry != nil {
		newDir := resp.Directory
		if message.NewParentPath != "" {
			newDir = message.NewParentPath
		}
		newKey := util.NewFullPath(newDir, message.NewEntry.Name)
		invalidations = append(invalidations, metadataInvalidation{path: newKey, entry: message.NewEntry})
	}

	if filer_pb.IsDelete(resp) && message.OldEntry != nil {
		oldKey := util.NewFullPath(resp.Directory, message.OldEntry.Name)
		invalidations = append(invalidations, metadataInvalidation{path: oldKey, entry: message.OldEntry})
	}

	return invalidations
}
