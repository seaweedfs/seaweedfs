package mount

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

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
		if in.LockOwner != 0 {
			wfs.posixLocks.ReleasePosixOwner(in.NodeId, in.LockOwner)
		}
		return fuse.OK
	}

	// When a closing lock owner is present, flush synchronously before waking any
	// blocked POSIX lock waiters so write-serialized callers cannot overtake each other.
	allowAsync := in.LockOwner == 0
	status := wfs.doFlush(fh, in.Uid, in.Gid, allowAsync)
	if in.LockOwner != 0 {
		wfs.posixLocks.ReleasePosixOwner(in.NodeId, in.LockOwner)
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
	return wfs.doFlush(fh, in.Uid, in.Gid, false)

}

func (wfs *WFS) doFlush(fh *FileHandle, uid, gid uint32, allowAsync bool) fuse.Status {

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
			return fuse.EIO
		}
	}

	if !fh.dirtyMetadata {
		return fuse.OK
	}

	if isOverQuota {
		return fuse.Status(syscall.ENOSPC)
	}

	if err := retryMetadataFlush(func() error {
		return wfs.flushMetadataToFiler(fh, dir, name, uid, gid)
	}, func(nextAttempt, totalAttempts int, backoff time.Duration, err error) {
		glog.Warningf("%v fh %d flush: retrying metadata flush (attempt %d/%d) after %v: %v",
			fileFullPath, fh.fh, nextAttempt, totalAttempts, backoff, err)
	}); err != nil {
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
func (wfs *WFS) flushMetadataToFiler(fh *FileHandle, dir, name string, uid, gid uint32) error {
	fileFullPath := fh.FullPath()

	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("doFlush", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

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
		entry.Attributes.Mtime = time.Now().Unix()
	}

	glog.V(4).Infof("%s set chunks: %v", fileFullPath, len(entry.GetChunks()))

	manifestChunks, nonManifestChunks := filer.SeparateManifestChunks(entry.GetChunks())

	chunks, _ := filer.CompactFileChunks(context.Background(), wfs.LookupFn(), nonManifestChunks)
	chunks, manifestErr := filer.MaybeManifestize(wfs.saveDataAsChunk(fileFullPath), chunks)
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

	wfs.mapPbIdFromLocalToFiler(request.Entry)

	resp, err := wfs.streamCreateEntry(context.Background(), request)
	if err != nil {
		glog.Errorf("fh flush create %s: %v", fileFullPath, err)
		return fmt.Errorf("fh flush create %s: %v", fileFullPath, err)
	}

	event := resp.GetMetadataEvent()
	if event == nil {
		event = metadataUpdateEvent(string(dir), request.Entry)
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("flush %s: best-effort metadata apply failed: %v", fileFullPath, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(dir))
	}

	if err == nil {
		fh.dirtyMetadata = false
	}

	return err
}
