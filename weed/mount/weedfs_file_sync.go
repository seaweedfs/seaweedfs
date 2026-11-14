package mount

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// metadataFlushTimeout bounds the metadata commit of a close()/fsync flush
// (CreateEntry and its retries) so an overwhelmed filer cannot wedge the
// calling process forever. It is deliberately generous: a healthy CreateEntry
// completes in well under a second, so this only fires on a genuinely stuck
// filer, never on a normal flush. It is the flush's commitBudget, so it does
// not cover the uploads that come first. A variable so tests can shorten it.
var metadataFlushTimeout = 30 * time.Second

// commitBudget is the time one flush may spend in the filer calls of its
// metadata commit, added up over its attempts. Nothing else is charged to it:
// the data upload, the wait for the handle's locks, the merge of bloated
// chunks and the manifest upload can each outlast the whole budget. A deadline
// that also ran through that work was spent by it; CreateEntry then failed at
// once with "context deadline exceeded", and close() returned an error for
// data that was already stored. A nil budget sets no bound. The attempts run
// one after another, so it needs no lock.
type commitBudget struct {
	left time.Duration
}

// begin returns ctx limited to what is left of the budget, for one filer call.
// The returned func ends the call and charges the budget the time it took.
func (b *commitBudget) begin(ctx context.Context) (context.Context, func()) {
	if b == nil {
		return ctx, func() {}
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, b.left)
	return ctx, func() {
		cancel()
		b.left -= time.Since(start)
	}
}

// spent reports whether the budget has run out, so that another attempt could
// only fail.
func (b *commitBudget) spent() bool {
	return b != nil && b.left <= 0
}

/**
 * Flush method
 *
 * This is called on each close() of the opened file.
 *
 * Since file descriptors can be duplicated (dup, dup2, fork), for
 * one open call there may be many flush calls.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * NOTE: the name of the method is misleading, since (unlike
 * fsync) the filesystem is not forced to flush pending writes.
 * One reason to flush data is if the filesystem wants to return
 * write errors during close.  However, such use is non-portable
 * because POSIX does not require [close] to wait for delayed I/O to
 * complete.
 *
 * If the filesystem supports file locking operations (setlk,
 * getlk) it should remove all locks belonging to 'fi->owner'.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to flush() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 *
 * [close]: http://pubs.opengroup.org/onlinepubs/9699919799/functions/close.html
 */
func (wfs *WFS) Flush(cancel <-chan struct{}, in *fuse.FlushIn) fuse.Status {
	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		// If handle is not found, it might have been already released
		// This is not an error condition for FLUSH
		if in.LockOwner != 0 && wfs.releasePosixOwner(in.NodeId, in.LockOwner) {
			wfs.forgetLockOwner(in.NodeId, in.LockOwner)
		}
		return fuse.OK
	}

	// FlushIn.LockOwner is populated by some FUSE kernels even when the process
	// did not hold byte-range locks. Only force the synchronous close path when
	// this owner actually has POSIX locks to release; otherwise writebackCache
	// would silently degrade to a blocking flush for ordinary close().
	hasPosixLocks := wfs.hasPosixOwner(in.NodeId, in.LockOwner)
	allowAsync := !hasPosixLocks

	// Bound the metadata commit with a deadline instead of tying it to the FUSE
	// cancel channel. A FUSE interrupt is not a process kill: Go's async
	// preemption (SIGURG) makes a close() under load emit an interrupt on nearly
	// every flush (see go-fuse RawFileSystem docs), so cancelling the in-flight
	// metadata CreateEntry on that interrupt turned healthy concurrent close()s
	// into EIO. The deadline still keeps close() from hanging forever against an
	// overwhelmed filer without failing benign flushes. doFlush charges it only
	// for the commit's filer calls, so a slow upload cannot use it up.
	status := wfs.doFlush(context.Background(), fh, in.Uid, in.Gid, allowAsync, metadataFlushTimeout)
	if in.LockOwner != 0 && wfs.releasePosixOwner(in.NodeId, in.LockOwner) {
		wfs.forgetLockOwner(in.NodeId, in.LockOwner)
	}
	return status
}

/**
 * Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to fsync() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */
func (wfs *WFS) Fsync(cancel <-chan struct{}, in *fuse.FsyncIn) (code fuse.Status) {

	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return fuse.ENOENT
	}

	// Fsync is an explicit sync request — always flush synchronously
	return wfs.doFlush(context.Background(), fh, in.Uid, in.Gid, false, metadataFlushTimeout)

}

// doFlush uploads the handle's dirty data, then commits its metadata to the
// filer. metadataTimeout is the commitBudget of that commit: what its filer
// calls may take over all attempts, whatever the uploads before and between
// them took. 0 sets no overall bound, leaving only the per-call filer
// timeouts. ctx should carry no deadline of its own: it would run through the
// uploads as well.
func (wfs *WFS) doFlush(ctx context.Context, fh *FileHandle, uid, gid uint32, allowAsync bool, metadataTimeout time.Duration) fuse.Status {

	// flush works at fh level
	fileFullPath := fh.FullPath()
	fh.RememberPath(fileFullPath)
	dir, name := fileFullPath.DirAndName()
	// send the data to the OS
	glog.V(4).Infof("doFlush %s fh %d", fileFullPath, fh.fh)

	// When writebackCache is enabled and this is a close()-triggered Flush (not fsync),
	// defer the expensive data upload + metadata flush to a background goroutine.
	// This allows the calling process (e.g., rsync) to proceed to the next file immediately.
	// POSIX does not require close() to wait for delayed I/O to complete.
	if allowAsync && wfs.option.WritebackCache && fh.dirtyMetadata {
		if wfs.IsOverQuotaWithUncommitted() {
			return fuse.Status(syscall.ENOSPC)
		}
		fh.asyncFlushPending = true
		fh.asyncFlushUid = uid
		fh.asyncFlushGid = gid
		glog.V(3).Infof("doFlush async deferred %s fh %d", fileFullPath, fh.fh)
		return fuse.OK
	}

	// Synchronous flush path (normal mode, fsync, or no dirty data)
	fh.asyncFlushPending = false

	// Check quota including uncommitted writes for real-time enforcement
	isOverQuota := wfs.IsOverQuotaWithUncommitted()
	if !isOverQuota {
		if err := fh.dirtyPages.FlushData(); err != nil {
			glog.Errorf("%v doFlush: %v", fileFullPath, err)
			return writeErrorToFuseStatus(err)
		}
	}

	if !fh.dirtyMetadata {
		return fuse.OK
	}

	// Skip metadata flush if the file was unlinked while open.
	// The filer entry is already gone; flushing would recreate it.
	if fh.isDeleted {
		glog.V(3).Infof("doFlush %s fh %d: file was unlinked, skipping metadata flush", fileFullPath, fh.fh)
		return fuse.OK
	}

	if isOverQuota {
		return fuse.Status(syscall.ENOSPC)
	}

	// The attempts share one budget, charged only for their filer calls.
	var commit *commitBudget
	if metadataTimeout > 0 {
		commit = &commitBudget{left: metadataTimeout}
	}

	publication := &contentPublication{failure: fh.writebackContentError}
	if err := retryMetadataFlushIf(ctx, func() error {
		return wfs.flushMetadataToFiler(ctx, fh, dir, name, uid, gid, commit, publication)
	}, func(err error) bool {
		return !commit.spent() && isRetryableFilerError(err)
	}, func(nextAttempt, totalAttempts int, backoff time.Duration, err error) {
		glog.Warningf("%v fh %d flush: retrying metadata flush (attempt %d/%d) after %v: %v",
			fileFullPath, fh.fh, nextAttempt, totalAttempts, backoff, err)
	}); err != nil {
		if fh.dirtyContent {
			publication.retire(err)
		}
		glog.Errorf("%v fh %d flush: %v", fileFullPath, fh.fh, err)
		return grpcErrorToFuseStatus(err)
	}

	if IsDebugFileReadWrite {
		fh.mirrorFile.Sync()
	}

	return fuse.OK
}

// flushMetadataToFiler sends the file's chunk references and attributes to the filer.
// This is shared between the synchronous doFlush path and the async flush completion.
//
// When -dlm is enabled, the distributed lock is already held by the FileHandle
// from open-for-write through close, so no additional distributed lock is
// needed here. The local fhLockTable lock below serializes within this mount.
//
// commit bounds the filer calls made here, over all attempts of one flush; nil
// leaves them to the per-call filer timeouts.
func (wfs *WFS) flushMetadataToFiler(ctx context.Context, fh *FileHandle, dir, name string, uid, gid uint32, commit *commitBudget, publications ...*contentPublication) error {
	publication := &contentPublication{}
	if len(publications) > 0 {
		publication = publications[0]
	}
	fileFullPath, dir, name := wfs.flushTarget(fh, dir, name)
	var unlock func()
	var fhActiveLock *util.ActiveLock
	// An unlink or rename can land while the flush waits for its locks.
	for attempt := 0; ; attempt++ {
		// Attribute acknowledgement and local retention must complete before a
		// whole-entry flush can publish. Take this before the handle's active lock.
		unlock = wfs.lockMetadataPublication(fileFullPath)
		fhActiveLock = fh.wfs.fhLockTable.AcquireLock("doFlush", fh.fh, util.ExclusiveLock)
		nowPath, nowDir, nowName := wfs.flushTarget(fh, dir, name)
		if nowPath == fileFullPath && nowDir == dir && nowName == name {
			break
		}
		fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
		unlock()
		if attempt == 3 {
			return fmt.Errorf("flush %s: name changed while waiting for its locks, now %s", fileFullPath, nowPath)
		}
		fileFullPath, dir, name = nowPath, nowDir, nowName
	}
	defer func() {
		fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
		unlock()
	}()

	glog.V(4).Infof("flushMetadataToFiler %s/%s inode %d fh %d", dir, name, fh.inode, fh.fh)

	// Re-check under the lock: Unlink sets the flag under it, so a flush that
	// was already past the earlier check cannot write the entry back after
	// the delete removed it.
	if fh.isDeleted {
		glog.V(3).Infof("flushMetadataToFiler %s fh %d: file was unlinked, skipping", fileFullPath, fh.fh)
		return nil
	}
	if fh.linkRefresh != nil {
		linkCtx, end := commit.begin(ctx)
		err := wfs.adoptStoredLink(linkCtx, fh, util.NewFullPath(dir, name), fh.linkRefresh)
		end()
		if err != nil {
			return err
		}
	}

	entry := fh.GetEntry()
	entry.Name = name // this flush may be just after a rename operation

	if entry.Attributes != nil {
		entry.Attributes.Mime = fh.contentType
		if entry.Attributes.Uid == 0 {
			entry.Attributes.Uid = uid
		}
		if entry.Attributes.Gid == 0 {
			entry.Attributes.Gid = gid
		}
		// Do not stamp mtime/ctime here. Write/SetAttr already maintain
		// them on the entry; overwriting at flush time clobbered user-set
		// mtime (utimes/touch -m -d) once the deferred flush ran.
	}

	glog.V(4).Infof("%s set chunks: %v", fileFullPath, len(entry.GetChunks()))

	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(entry.GetChunks())

	chunks, _, err := filer.CompactFileChunks(context.Background(), wfs.LookupFn(), nonManifestChunks)
	if err != nil {
		return fmt.Errorf("compact chunks of %s: %w", fileFullPath, err)
	}

	if mergedChunks, mergeErr := wfs.maybeMergeChunks(fileFullPath, chunks, manifestChunks); mergeErr != nil {
		glog.V(0).Infof("maybeMergeChunks %s: %v", fileFullPath, mergeErr)
	} else if mergedChunks != nil {
		chunks = mergedChunks
		manifestChunks = nil
		fh.noteRewrite()
	}

	// no chunk deleter here: a failed fold reports the blobs it saved
	chunks, manifestErr := filer.MaybeManifestize(wfs.saveDataAsChunk(fileFullPath), nil, chunks)
	if manifestErr != nil {
		// not good, but should be ok
		glog.V(0).Infof("MaybeManifestize: %v", manifestErr)
	}
	entry.Chunks = append(chunks, manifestChunks...)

	// Clone the proto entry for the filer request so that mapPbIdFromLocalToFiler
	// does not mutate the file handle's live entry. Without the clone, a concurrent
	// Lookup can observe filer-side uid/gid on the file handle entry and return it
	// to the kernel, which caches it and then rejects opens by the local user.
	requestEntry := proto.Clone(entry.GetEntry()).(*filer_pb.Entry)
	request := &filer_pb.CreateEntryRequest{
		Directory:                string(dir),
		Entry:                    requestEntry,
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	}

	// Snapshot with local ids before the request mapping mutates the clone:
	// on ack this becomes the handle's base, judged against future events.
	baseSnapshot := proto.Clone(requestEntry).(*filer_pb.Entry)
	wfs.mapPbIdFromLocalToFiler(request.Entry)

	// The chunk merge and the manifest above read and upload chunks, which can
	// take longer than the whole commit budget. It is charged only from here.
	commitCtx, end := commit.begin(ctx)
	resp, merged, err := wfs.createContentEntry(commitCtx, fh, request, baseSnapshot, false, publication)
	end()
	if err != nil {
		glog.Errorf("fh flush create %s: %v", fileFullPath, err)
		return &contentPublicationError{cause: fmt.Errorf("fh flush create %s: %w", fileFullPath, err), request: proto.Clone(request).(*filer_pb.CreateEntryRequest)}
	}
	if merged != nil {
		fh.rebaseEntry(merged, requestEntry)
		baseSnapshot = merged
	}

	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataUpdateEvent(string(dir), request.Entry)
		if event != nil {
			event.TsNs = ackVersionTsNs(resp)
		}
	}
	// The filer acknowledged this state at the event's log position (or, for
	// a no-op create, at the response's log position); older queued
	// subscription events must not roll the handle back.
	fh.setAuthoritativeBase(wfs.ackedBase(resp, baseSnapshot))
	fh.advanceEntryVersion(ackVersionTsNs(resp), resp.GetLogSignature())
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("flush %s: best-effort metadata apply failed: %v", fileFullPath, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(dir))
	}

	if err == nil {
		fh.pendingAppend = false
		fh.dirtyMetadata = false
		fh.dirtyContent = false
	}

	return err
}

// flushTarget returns the lock path and the name a flush of fh publishes
// through: the caller's name while it still names the file, else the name the
// file has now.
func (wfs *WFS) flushTarget(fh *FileHandle, dir, name string) (util.FullPath, string, string) {
	fileFullPath := fh.FullPath()
	if resolved := util.FullPath(dir).Child(name); fileFullPath != "" && fileFullPath != resolved {
		if inode, found := wfs.inodeToPath.GetInode(resolved); !found || inode != fh.inode {
			dir, name = fileFullPath.DirAndName()
		}
	}
	return fileFullPath, dir, name
}

// createContentEntry publishes a handle's whole entry only over the filer
// state the handle observed. Deferred creates expect no entry; existing
// handles compare the full base, including inode identity. The unary call
// keeps a refused condition apart from an I/O failure; the mutation stream
// reduces it to EIO.
//
// When the stored entry moved on, the handle's own changes since its base are
// replayed onto it and published against it, so a peer's acknowledged chunks
// survive. merged is that entry in local ids; nil when the request went as is.
// With writes still buffered, the merge must keep room for them.
func (wfs *WFS) createContentEntry(ctx context.Context, fh *FileHandle, request *filer_pb.CreateEntryRequest, local *filer_pb.Entry, buffered bool, publication *contentPublication) (resp *filer_pb.CreateEntryResponse, merged *filer_pb.Entry, err error) {
	base := fh.baseEntry.Load()
	guard := &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}
	if base != nil {
		expected := proto.Clone(base).(*filer_pb.Entry)
		wfs.mapPbIdFromLocalToFiler(expected)
		guard = &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: expected}
	}
	sent := request.Entry
	for attempt := 0; ; attempt++ {
		if guardErr := wfs.checkContentPublication(fh, guard.ExpectedEntry); guardErr != nil {
			return nil, nil, guardErr
		}
		request.Condition = &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{guard}}
		err = wfs.withFilerClient(ctx, false, wfs.filerRPCWait(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			var callErr error
			resp, callErr = filer_pb.CreateEntryWithResponse(ctx, client, request)
			if errors.Is(callErr, filer_pb.ErrPreconditionFailed) {
				publication.refused = true
			}
			return callErr
		})
		if err == nil && request.Entry != sent {
			merged = proto.Clone(request.Entry).(*filer_pb.Entry)
			wfs.mapPbIdFromFilerToLocal(merged)
		}
		if err == nil || base == nil || attempt >= maxContentFlushMerges || !errors.Is(err, filer_pb.ErrPreconditionFailed) {
			return resp, merged, err
		}
		stored := wfs.lookupRefusedEntry(ctx, request.Directory, request.Entry.Name)
		if stored == nil {
			return resp, nil, err
		}
		mark := fh.markSince(base)
		next, ok := mergeContentEntry(stored, base, local, sent, mark.truncatedTo, mark.rewrote)
		if !ok || buffered && !mergeKeepsBuffered(next, local) {
			return resp, nil, err
		}
		glog.V(1).Infof("content flush %s/%s: entry changed since the handle's base, merging", request.Directory, request.Entry.Name)
		request.Entry = next
		guard = &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: stored}
	}
}

// lookupRefusedEntry reads the entry a refused create ran into, within one
// filer call: a read that stalls must not hold or replay the refusal.
func (wfs *WFS) lookupRefusedEntry(ctx context.Context, dir, name string) (stored *filer_pb.Entry) {
	ctx, cancel := context.WithTimeout(ctx, wfs.filerRPCWait())
	defer cancel()
	_ = wfs.withFilerClient(ctx, false, wfs.filerRPCWait(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
		found, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{Directory: dir, Name: name})
		if err == nil {
			stored = found.Entry
		}
		return err
	})
	return stored
}

// maxContentFlushMerges bounds how often one flush rebases onto a moving entry.
const maxContentFlushMerges = 3

// mergeContentEntry replays what local changed since base onto stored: the
// chunks it added or dropped, and the attributes, extended keys and inline
// content it changed, taking those values from sent (local in stored's id
// space). Everything else keeps the stored value. A local truncation keeps
// the handle's size and clips stored chunks at the lowest size truncated to
// (truncatedTo, or -1); otherwise the stored size stands unless the handle
// changed its size or wrote past it.
// A different file at the path, inline content changed on both sides,
// inline content next to chunks (reads stop at the inline bytes), a manifest
// across the truncate (its members keep their sizes), or a local
// whole-file rewrite (rewrote) over a peer's chunk change cannot be merged:
// the rewrite carries the handle's stale view.
func mergeContentEntry(stored, base, local, sent *filer_pb.Entry, truncatedTo int64, rewrote bool) (*filer_pb.Entry, bool) {
	if !sameContentFile(stored, base) {
		return nil, false
	}
	merged := proto.Clone(stored).(*filer_pb.Entry)
	merged.Name = sent.Name
	if !bytes.Equal(local.Content, base.Content) {
		if !bytes.Equal(stored.Content, base.Content) {
			return nil, false
		}
		merged.Content = sent.Content
	}

	inBase := make(map[string]bool, len(base.Chunks))
	for _, c := range base.Chunks {
		inBase[contentChunkKey(c)] = true
	}
	inLocal := make(map[string]bool, len(local.Chunks))
	for _, c := range local.Chunks {
		inLocal[contentChunkKey(c)] = true
	}
	localSize := filer.FileSize(local)
	if truncatedTo < 0 && localSize < filer.FileSize(base) {
		truncatedTo = int64(localSize)
	}
	peerChanged := len(stored.Chunks) != len(base.Chunks)
	merged.Chunks = merged.Chunks[:0]
	for _, c := range stored.Chunks {
		key := contentChunkKey(c)
		peerChanged = peerChanged || !inBase[key]
		if inBase[key] && !inLocal[key] || truncatedTo >= 0 && c.Offset >= truncatedTo {
			continue
		}
		c = proto.Clone(c).(*filer_pb.FileChunk)
		if truncatedTo >= 0 && c.Offset+int64(c.Size) > truncatedTo {
			if c.IsChunkManifest {
				return nil, false
			}
			c.Size = uint64(truncatedTo - c.Offset)
		}
		merged.Chunks = append(merged.Chunks, c)
	}
	if rewrote && peerChanged {
		return nil, false
	}
	size := int64(filer.FileSize(stored))
	if localSize != filer.FileSize(base) {
		size = max(size, int64(localSize))
	}
	for _, c := range local.Chunks {
		if !inBase[contentChunkKey(c)] {
			merged.Chunks = append(merged.Chunks, proto.Clone(c).(*filer_pb.FileChunk))
			size = max(size, c.Offset+int64(c.Size))
		}
	}
	if len(merged.Content) > 0 && len(merged.Chunks) > 0 {
		return nil, false
	}

	if merged.Attributes == nil {
		merged.Attributes = &filer_pb.FuseAttributes{}
	}
	baseAttrs, localAttrs := base.GetAttributes(), local.GetAttributes()
	if baseAttrs == nil {
		baseAttrs = &filer_pb.FuseAttributes{}
	}
	if localAttrs == nil {
		localAttrs = &filer_pb.FuseAttributes{}
	}
	m, b, l := merged.Attributes.ProtoReflect(), baseAttrs.ProtoReflect(), localAttrs.ProtoReflect()
	s := proto.Clone(sent.GetAttributes()).(*filer_pb.FuseAttributes).ProtoReflect()
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Name() == "file_size" || l.Get(fd).Equal(b.Get(fd)) {
			continue
		}
		if s.Has(fd) {
			m.Set(fd, s.Get(fd))
		} else {
			m.Clear(fd)
		}
	}

	for k, v := range local.Extended {
		if old, found := base.Extended[k]; !found || !bytes.Equal(old, v) {
			if merged.Extended == nil {
				merged.Extended = make(map[string][]byte)
			}
			merged.Extended[k] = sent.Extended[k]
		}
	}
	for k := range base.Extended {
		if _, found := local.Extended[k]; !found {
			delete(merged.Extended, k)
		}
	}

	if truncatedTo >= 0 {
		size = int64(localSize)
	}
	merged.Attributes.FileSize = uint64(size)
	// A peer truncate must not turn local pending data into a successful hole.
	if filer.FileSize(stored) < filer.FileSize(base) && contentGapAfter(merged, filer.FileSize(stored)) {
		return nil, false
	}
	return merged, true
}

// sameContentFile reports whether stored is still the regular file base was:
// the same inode, or for an entry without one, the same creation time. A
// synthesized base with neither cannot tell.
func sameContentFile(stored, base *filer_pb.Entry) bool {
	if stored == nil || base == nil || stored.IsDirectory || !bytes.Equal(stored.HardLinkId, base.HardLinkId) {
		return false
	}
	if inode := base.GetAttributes().GetInode(); inode != 0 {
		return stored.GetAttributes().GetInode() == inode
	}
	crtime := base.GetAttributes().GetCrtime()
	return crtime == 0 || stored.GetAttributes().GetCrtime() == crtime
}

// contentChunkKey identifies a chunk reference: truncation shrinks a chunk in
// place, so the same file id at another size is a different reference. Both
// file id encodings give one key, and the chunk is left as is.
func contentChunkKey(c *filer_pb.FileChunk) string {
	return fmt.Sprintf("%s@%d+%d", contentFileIdKey(c), c.Offset, c.Size)
}

func contentFileIdKey(c *filer_pb.FileChunk) string {
	fid := c.Fid
	if fid == nil {
		if parsed, err := filer_pb.ToFileIdObject(c.FileId); err == nil {
			fid = parsed
		} else {
			return c.FileId
		}
	}
	return fmt.Sprintf("%d,%x,%x", fid.VolumeId, fid.FileKey, fid.Cookie)
}

// ackedBase is the base an acknowledged create leaves: the entry the filer
// stored, in local ids. The filer stamps commit_ts_ns, crtime, inode and atime
// and may rewrite chunks, so the requested entry would never match it again.
// A filer that sends no event leaves the requested entry.
func (wfs *WFS) ackedBase(resp *filer_pb.CreateEntryResponse, requested *filer_pb.Entry) *filer_pb.Entry {
	stored := resp.GetMetadataEvent().GetEventNotification().GetNewEntry()
	if stored == nil {
		return requested
	}
	base := proto.Clone(stored).(*filer_pb.Entry)
	wfs.mapPbIdFromFilerToLocal(base)
	return base
}

// shouldMergeChunks reports whether the non-manifest chunks are bloated
// enough to justify re-reading and re-uploading the file. The condition
// is: sum of compacted chunk sizes > 2 * logical file size.
func shouldMergeChunks(compactedChunks []*filer_pb.FileChunk, manifestChunks []*filer_pb.FileChunk) (totalChunkSize, fileSize uint64, merge bool) {
	for _, chunk := range compactedChunks {
		totalChunkSize += chunk.Size
	}
	// Count manifest coverage toward stored total. Each manifest holds
	// sub-chunks on volume servers that cover approximately Size bytes.
	// Without this, overlapping manifests accumulate undetected because
	// the merge condition only saw the (small) non-manifest chunk total.
	for _, chunk := range manifestChunks {
		totalChunkSize += chunk.Size
	}
	allChunks := make([]*filer_pb.FileChunk, 0, len(compactedChunks)+len(manifestChunks))
	allChunks = append(allChunks, compactedChunks...)
	allChunks = append(allChunks, manifestChunks...)
	fileSize = filer.TotalSize(allChunks)
	merge = fileSize > 0 && totalChunkSize > 2*fileSize
	return
}

// maybeMergeChunks re-reads and re-uploads file data as properly sized chunks
// when the total stored chunk data significantly exceeds the logical file size,
// which happens after many random writes create partially-overlapping small chunks.
func (wfs *WFS) maybeMergeChunks(fileFullPath util.FullPath, compactedChunks []*filer_pb.FileChunk, manifestChunks []*filer_pb.FileChunk) ([]*filer_pb.FileChunk, error) {
	totalChunkSize, fileSize, merge := shouldMergeChunks(compactedChunks, manifestChunks)
	if !merge {
		return nil, nil
	}

	glog.V(0).Infof("%.1fx chunk bloat detected on %s (%d chunks, %d stored vs %d content), merging",
		float64(totalChunkSize)/float64(fileSize), fileFullPath, len(compactedChunks), totalChunkSize, fileSize)

	ctx := context.Background()
	allChunks := make([]*filer_pb.FileChunk, 0, len(compactedChunks)+len(manifestChunks))
	allChunks = append(allChunks, compactedChunks...)
	allChunks = append(allChunks, manifestChunks...)
	reader := filer.NewChunkStreamReaderFromLookup(ctx, wfs.LookupFn(), allChunks)
	defer reader.Close()

	saveFunc := wfs.saveDataAsChunk(fileFullPath)
	chunkSize := wfs.option.ChunkSizeLimit
	if int64(fileSize) < chunkSize {
		chunkSize = int64(fileSize)
	}

	var newChunks []*filer_pb.FileChunk
	var offset int64
	buf := make([]byte, chunkSize)

	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			chunk, uploadErr := saveFunc(bytes.NewReader(buf[:n]), "", offset, 0, uint64(n))
			if uploadErr != nil {
				return nil, uploadErr
			}
			newChunks = append(newChunks, chunk)
			offset += int64(n)
		}
		if readErr != nil {
			if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
				break
			}
			return nil, readErr
		}
	}

	glog.V(0).Infof("merged %s: %d chunks -> %d chunks", fileFullPath, len(compactedChunks)+len(manifestChunks), len(newChunks))

	return newChunks, nil
}
