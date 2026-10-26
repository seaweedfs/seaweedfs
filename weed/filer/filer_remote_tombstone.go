package filer

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/protobuf/proto"
)

const (
	// remoteDeletionTombstoneTTL bounds a tombstone when no write-back sync
	// offset ever confirms the remote delete. A remote object re-created
	// outside the filer at a deleted path stays hidden for this long.
	remoteDeletionTombstoneTTL = 24 * time.Hour
	// remoteDeletionTombstoneLimit bounds tracked paths; past it new
	// tombstones are dropped after an expired sweep still leaves no room.
	remoteDeletionTombstoneLimit = 1 << 16
)

// remoteDeletionTombstones tracks paths deleted under a remote mount whose
// remote object may still exist because the write-back daemon has not
// consumed the delete event yet. A lazy remote fetch or listing must not
// resurrect them. A tombstone lifts when the path is written again, when the
// mount's persisted sync offset passes the delete event, or on TTL.
//
// Tombstones recorded before their metadata event lands (the synchronous
// delete path) are marked pending: the sync offset orders against event
// timestamps, which only the event itself knows, so a pending tombstone can
// only be lifted by the event confirming it or by TTL. Once the event stamps
// the real timestamp the tombstone is releasable by the offset.
// remoteDeletionTombstone carries the delete's commit stamp — the basis for
// ordering it against other mutations — and, once the event lands, the local
// stream position it was delivered under, which the write-back offset must
// pass before the remote delete counts as consumed.
type remoteDeletionTombstone struct {
	commitTsNs int64
	streamTsNs int64
}

type remoteDeletionTombstones struct {
	mu      sync.Mutex
	files   map[string]remoteDeletionTombstone // file path -> delete event
	dirs    map[string]remoteDeletionTombstone // deleted directory path -> event; covers its subtree
	pending map[string]bool                   // tombstone path recorded ahead of its event
}

func newRemoteDeletionTombstones() *remoteDeletionTombstones {
	return &remoteDeletionTombstones{
		files:   make(map[string]remoteDeletionTombstone),
		dirs:    make(map[string]remoteDeletionTombstone),
		pending: make(map[string]bool),
	}
}

// add records a tombstone ahead of its metadata event — the timestamp is the
// local delete time, a lower bound the sync offset cannot order against.
func (t *remoteDeletionTombstones) add(path string, isDir bool, tsNs int64) {
	t.upsert(path, isDir, tsNs, 0, false)
}

// addFromEvent records a tombstone stamped by the delete event itself:
// commitTsNs orders it against other mutations, streamTsNs is the delivery
// position the mount's sync offset must pass to release it.
func (t *remoteDeletionTombstones) addFromEvent(path string, isDir bool, commitTsNs, streamTsNs int64) {
	t.upsert(path, isDir, commitTsNs, streamTsNs, true)
}

func (t *remoteDeletionTombstones) upsert(path string, isDir bool, commitTsNs, streamTsNs int64, fromEvent bool) {
	if t == nil {
		return
	}
	if streamTsNs == 0 {
		// No delivery position yet — the offset can never vouch for it.
		streamTsNs = math.MaxInt64
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// An ancestor directory tombstone at least as new already covers the
	// path; recording it again only spends capacity.
	for p := path; ; {
		i := strings.LastIndexByte(p, '/')
		if i <= 0 {
			break
		}
		p = p[:i]
		if ancestor, ok := t.dirs[p]; ok && ancestor.commitTsNs >= commitTsNs {
			return
		}
	}
	m := t.files
	if isDir {
		m = t.dirs
	}
	if cur, ok := m[path]; ok {
		if commitTsNs < cur.commitTsNs {
			return
		}
		m[path] = remoteDeletionTombstone{commitTsNs: commitTsNs, streamTsNs: streamTsNs}
		if fromEvent {
			delete(t.pending, path)
		} else {
			// a newer local delete restamps the tombstone ahead of its
			// event — the offset cannot vouch for it until the event lands
			t.pending[path] = true
		}
		return
	}
	if len(t.files)+len(t.dirs) >= remoteDeletionTombstoneLimit {
		t.evictExpiredLocked(time.Now().UnixNano())
		if len(t.files)+len(t.dirs) >= remoteDeletionTombstoneLimit {
			glog.V(0).Infof("remote deletion tombstones full (%d), skipping %s", remoteDeletionTombstoneLimit, path)
			return
		}
	}
	m[path] = remoteDeletionTombstone{commitTsNs: commitTsNs, streamTsNs: streamTsNs}
	if !fromEvent {
		t.pending[path] = true
	}
	if isDir {
		t.dropCoveredLocked(path, commitTsNs)
	}
}

// dropCoveredLocked removes descendant tombstones a new directory tombstone
// subsumes: their deletes predate it, so the ancestor already hides those
// remote objects. Descendants deleted later keep their own tombstone.
// Caller must hold t.mu.
func (t *remoteDeletionTombstones) dropCoveredLocked(dirPath string, tsNs int64) {
	prefix := dirPath + "/"
	for p, cur := range t.files {
		if cur.commitTsNs <= tsNs && strings.HasPrefix(p, prefix) {
			delete(t.files, p)
			delete(t.pending, p)
		}
	}
	for p, cur := range t.dirs {
		if cur.commitTsNs <= tsNs && strings.HasPrefix(p, prefix) {
			delete(t.dirs, p)
			delete(t.pending, p)
		}
	}
}

// drop removes the exact tombstone recorded for path, e.g. when the delete
// that recorded it fails before touching anything.
func (t *remoteDeletionTombstones) drop(path string, tsNs int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.dirs[path]; ok && cur.commitTsNs <= tsNs {
		delete(t.dirs, path)
		delete(t.pending, path)
	}
	if cur, ok := t.files[path]; ok && cur.commitTsNs <= tsNs {
		delete(t.files, path)
		delete(t.pending, path)
	}
}

// clear drops a file tombstone when a write at the path is at least as new as
// the delete; a replayed older create must not lift a newer delete.
func (t *remoteDeletionTombstones) clear(path string, tsNs int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.files[path]; ok && tsNs >= cur.commitTsNs {
		delete(t.files, path)
		delete(t.pending, path)
	}
}

// blockedSince returns the tombstone governing path — its own file tombstone
// or the newest one from a deleted ancestor directory — and whether that
// tombstone is still waiting for its event. commitTsNs == 0 means clear.
func (t *remoteDeletionTombstones) blockedSince(path string) (streamTsNs, commitTsNs int64, pending bool) {
	if t == nil {
		return 0, 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.files[path]; ok {
		streamTsNs, commitTsNs, pending = cur.streamTsNs, cur.commitTsNs, t.pending[path]
	}
	for p := path; ; {
		if cur, ok := t.dirs[p]; ok && cur.commitTsNs > commitTsNs {
			streamTsNs, commitTsNs, pending = cur.streamTsNs, cur.commitTsNs, t.pending[p]
		}
		i := strings.LastIndexByte(p, '/')
		if i <= 0 {
			break
		}
		p = p[:i]
	}
	return streamTsNs, commitTsNs, pending
}

// releaseThrough drops the tombstones governing path whose remote delete the
// write-back offset has passed, i.e. recorded at or below streamTsNs.
// Pending tombstones carry no delivery position yet and are never dropped.
func (t *remoteDeletionTombstones) releaseThrough(path string, tsNs int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.files[path]; ok && cur.streamTsNs <= tsNs {
		delete(t.files, path)
		delete(t.pending, path)
	}
	for p := path; ; {
		if cur, ok := t.dirs[p]; ok && cur.streamTsNs <= tsNs {
			delete(t.dirs, p)
			delete(t.pending, p)
		}
		i := strings.LastIndexByte(p, '/')
		if i <= 0 {
			break
		}
		p = p[:i]
	}
}

// releaseExpired drops the tombstones governing path that outlived the TTL —
// the only release a tombstone still waiting for its event can get.
func (t *remoteDeletionTombstones) releaseExpired(path string, nowNs int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.files[path]; ok && nowNs-cur.commitTsNs >= int64(remoteDeletionTombstoneTTL) {
		delete(t.files, path)
		delete(t.pending, path)
	}
	for p := path; ; {
		if cur, ok := t.dirs[p]; ok && nowNs-cur.commitTsNs >= int64(remoteDeletionTombstoneTTL) {
			delete(t.dirs, p)
			delete(t.pending, p)
		}
		i := strings.LastIndexByte(p, '/')
		if i <= 0 {
			break
		}
		p = p[:i]
	}
}

func (t *remoteDeletionTombstones) evictExpiredLocked(nowNs int64) {
	for p, cur := range t.files {
		if nowNs-cur.commitTsNs >= int64(remoteDeletionTombstoneTTL) {
			delete(t.files, p)
			delete(t.pending, p)
		}
	}
	for p, cur := range t.dirs {
		if nowNs-cur.commitTsNs >= int64(remoteDeletionTombstoneTTL) {
			delete(t.dirs, p)
			delete(t.pending, p)
		}
	}
}

// noteRemoteDeletion records a delete of path under a remote mount so lazy
// remote reads skip it until the remote delete is confirmed.
func (f *Filer) noteRemoteDeletion(p util.FullPath, isDir bool, tsNs int64) {
	if f.RemoteStorage == nil || f.remoteTombstones == nil {
		return
	}
	if _, remoteLoc := f.RemoteStorage.FindMountDirectory(p); remoteLoc == nil {
		return
	}
	f.remoteTombstones.add(string(p), isDir, tsNs)
}

// unnoteRemoteDeletion retracts a tombstone when the delete that recorded it
// fails before touching anything under path.
func (f *Filer) unnoteRemoteDeletion(p util.FullPath, tsNs int64) {
	if f.remoteTombstones == nil {
		return
	}
	f.remoteTombstones.drop(string(p), tsNs)
}

// isRemoteDeletionPending reports whether a remote write-back delete for p is
// still owed: p was deleted under mountDir and neither a rewrite, the mount's
// sync offset, nor the TTL has lifted the tombstone.
func (f *Filer) isRemoteDeletionPending(ctx context.Context, p util.FullPath, mountDir util.FullPath) bool {
	if f.remoteTombstones == nil {
		return false
	}
	streamTsNs, commitTsNs, pending := f.remoteTombstones.blockedSince(string(p))
	if commitTsNs == 0 {
		return false
	}
	if pending {
		// Recorded ahead of its delete event — the write-back offset cannot
		// vouch for it yet; only the TTL lifts it.
		if f.remoteDeletionExpired(commitTsNs) {
			f.remoteTombstones.releaseExpired(string(p), time.Now().UnixNano())
			return false
		}
		return true
	}
	if f.remoteDeletionConsumed(ctx, mountDir, streamTsNs) {
		f.remoteTombstones.releaseThrough(string(p), streamTsNs)
		return false
	}
	return true
}

func (f *Filer) remoteDeletionExpired(tsNs int64) bool {
	return time.Now().UnixNano()-tsNs >= int64(remoteDeletionTombstoneTTL)
}

func (f *Filer) remoteDeletionConsumed(ctx context.Context, mountDir util.FullPath, tsNs int64) bool {
	if f.remoteDeletionExpired(tsNs) {
		return true
	}
	offset, err := f.readRemoteSyncOffset(ctx, mountDir)
	return err == nil && offset >= tsNs
}

// readRemoteSyncOffset reads the write-back daemon's persisted watermark for
// mountDir straight from the local store: every event at or below it has been
// applied to the remote.
func (f *Filer) readRemoteSyncOffset(ctx context.Context, mountDir util.FullPath) (int64, error) {
	value, err := f.Store.KvGet(ctx, remote_storage.SyncOffsetKey(string(mountDir)))
	if err != nil {
		return 0, err
	}
	if len(value) < 8 {
		return 0, nil
	}
	return int64(util.BytesToUint64(value)), nil
}

// RebuildRemoteDeletionTombstones gates lazy remote reads and replays the
// persisted metadata log from the oldest write-back offset across mounts,
// restoring tombstones for deletes committed before a restart but not yet
// applied to the remote. Every filer writes its log files under the same
// directory, so pending peer deletes replay too; only events still inside
// the unflushed buffer tail are missed. The gate is set synchronously so no
// lazy read can slip in before replay starts, and it stays closed until a
// replay succeeds.
func (f *Filer) RebuildRemoteDeletionTombstones(ctx context.Context) {
	if f.RemoteStorage == nil || f.remoteTombstones == nil {
		return
	}
	mounts := f.RemoteStorage.MountedDirectories()
	if len(mounts) == 0 {
		return
	}
	done := make(chan struct{})
	f.remoteTombstonesDone.Store(&done)
	go f.rebuildRemoteDeletionTombstones(ctx, mounts, done)
}

func (f *Filer) rebuildRemoteDeletionTombstones(ctx context.Context, mounts []util.FullPath, done chan struct{}) {
	// the replay itself lists directories; do not let it wait on its own gate
	ctx = context.WithValue(ctx, lazyFetchContextKey{}, true)
	startTsNs := f.remoteDeletionRebuildStartTsNs(ctx, mounts)
	openGate := func() {
		close(done)
		f.remoteTombstonesDone.Store(nil)
	}
	// Deletes made after startup are recorded through the live delete and
	// event paths, so replay can only be missing deletes committed before
	// the restart — and those have all crossed the tombstone TTL once this
	// deadline passes. Holding the gate longer protects nothing.
	replayDeadline := time.Now().Add(remoteDeletionTombstoneTTL)
	backoff := 2 * time.Second
	for {
		_, _, err := f.ReadPersistedLogBuffer(ctx, log_buffer.NewMessagePosition(startTsNs, 0), 0,
			func(logEntry *filer_pb.LogEntry) (bool, error) {
				event := &filer_pb.SubscribeMetadataResponse{}
				if err := proto.Unmarshal(logEntry.Data, event); err != nil {
					return false, nil
				}
				// Same normalization as delivery: the payload ts is the
				// commit stamp, the wrapper ts the stream position.
				if event.MutationTsNs == 0 {
					event.MutationTsNs = event.TsNs
				}
				event.TsNs = logEntry.TsNs
				f.onRemoteDeletionEvents(event)
				return false, nil
			})
		if err == nil {
			openGate()
			return
		}
		glog.WarningfCtx(ctx, "rebuild remote deletion tombstones: %v", err)
		if !time.Now().Before(replayDeadline) {
			glog.ErrorfCtx(ctx, "rebuild remote deletion tombstones: giving up after %v", remoteDeletionTombstoneTTL)
			openGate()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
}

// remoteDeletionRebuildStartTsNs returns the oldest write-back offset across
// mounts — the earliest event the daemon may not have applied. Mounts without
// a recorded offset replay from the TTL floor: events older than it would
// build tombstones that are already expired.
func (f *Filer) remoteDeletionRebuildStartTsNs(ctx context.Context, mounts []util.FullPath) int64 {
	startTsNs := int64(math.MaxInt64)
	for _, dir := range mounts {
		offset, err := f.readRemoteSyncOffset(ctx, dir)
		if err != nil {
			glog.WarningfCtx(ctx, "read remote sync offset for %s: %v", dir, err)
			offset = 0
		}
		if offset < startTsNs {
			startTsNs = offset
		}
	}
	ttlFloor := time.Now().Add(-remoteDeletionTombstoneTTL).UnixNano()
	if startTsNs == int64(math.MaxInt64) || startTsNs < ttlFloor {
		return ttlFloor
	}
	return startTsNs
}

// onRemoteDeletionEvents folds peer and local metadata events into the
// tombstone set: a delete or rename source is tombstoned at the event
// timestamp, a create/update/rename target lifts a file tombstone.
func (f *Filer) onRemoteDeletionEvents(event *filer_pb.SubscribeMetadataResponse) {
	message := event.EventNotification
	if message == nil {
		return
	}
	// The commit stamp orders the delete against other mutations; event.TsNs
	// is the delivery position the write-back offset must pass.
	commitTsNs := eventMutationTsNs(event)
	if message.OldEntry != nil && !IsMetadataOnlyDelete(message.OldEntry) {
		sourcePath := filer_pb.MetadataEventSourceFullPath(event)
		if message.NewEntry == nil || sourcePath != filer_pb.MetadataEventTargetFullPath(event) {
			if f.RemoteStorage != nil && f.remoteTombstones != nil {
				if _, remoteLoc := f.RemoteStorage.FindMountDirectory(util.FullPath(sourcePath)); remoteLoc != nil {
					f.remoteTombstones.addFromEvent(sourcePath, message.OldEntry.IsDirectory, commitTsNs, event.TsNs)
				}
			}
		}
	}
	if message.NewEntry != nil {
		f.remoteTombstones.clear(filer_pb.MetadataEventTargetFullPath(event), commitTsNs)
	}
}
