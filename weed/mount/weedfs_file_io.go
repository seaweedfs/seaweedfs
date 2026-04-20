package mount

import (
	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

/**
	 * Open a file
	 *
	 * Open flags are available in fi->flags. The following rules
	 * apply.
	 *
	 *  - Creation (O_CREAT, O_EXCL, O_NOCTTY) flags will be
	 *    filtered out / handled by the kernel.
	 *
	 *  - Access modes (O_RDONLY, O_WRONLY, O_RDWR) should be used
	 *    by the filesystem to check if the operation is
	 *    permitted.  If the ``-o default_permissions`` mount
	 *    option is given, this check is already done by the
	 *    kernel before calling open() and may thus be omitted by
	 *    the filesystem.
	 *
	 *  - When writeback caching is enabled, the kernel may send
	 *    read requests even for files opened with O_WRONLY. The
	 *    filesystem should be prepared to handle this.
	 *
	 *  - When writeback caching is disabled, the filesystem is
	 *    expected to properly handle the O_APPEND flag and ensure
	 *    that each write is appending to the end of the file.
	 *
         *  - When writeback caching is enabled, the kernel will
	 *    handle O_APPEND. However, unless all changes to the file
	 *    come through the kernel this will not work reliably. The
	 *    filesystem should thus either ignore the O_APPEND flag
	 *    (and let the kernel handle it), or return an error
	 *    (indicating that reliably O_APPEND is not available).
	 *
	 * Filesystem may store an arbitrary file handle (pointer,
	 * index, etc) in fi->fh, and use this in other all other file
	 * operations (read, write, flush, release, fsync).
	 *
	 * Filesystem may also implement stateless file I/O and not store
	 * anything in fi->fh.
	 *
	 * There are also some flags (direct_io, keep_cache) which the
	 * filesystem may set in fi, to change the way the file is opened.
	 * See fuse_file_info structure in <fuse_common.h> for more details.
	 *
	 * If this request is answered with an error code of ENOSYS
	 * and FUSE_CAP_NO_OPEN_SUPPORT is set in
	 * `fuse_conn_info.capable`, this is treated as success and
	 * future calls to open and release will also succeed without being
	 * sent to the filesystem process.
	 *
	 * Valid replies:
	 *   fuse_reply_open
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param ino the inode number
	 * @param fi file information
*/
func (wfs *WFS) Open(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	var fileHandle *FileHandle
	fileHandle, status = wfs.AcquireHandle(in.NodeId, in.Flags, in.Uid, in.Gid)
	if status == fuse.OK {
		out.Fh = uint64(fileHandle.fh)
		out.OpenFlags = 0

		// For read-only opens, set FOPEN_KEEP_CACHE when the file's mtime
		// has not changed since the last open.  This tells the kernel to
		// preserve its existing page cache, avoiding redundant reads.
		if in.Flags&fuse.O_ANYWRITE == 0 {
			if entry := fileHandle.GetEntry(); entry != nil && entry.Attributes != nil {
				wfs.applyKeepCacheFlag(in.NodeId, entry, out)
			}
		}
	}
	return status
}

/**
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open call there will be exactly one release call (unless
 * the filesystem is force-unmounted).
 *
 * The filesystem may reply with an error, but error values are
 * not returned to close() or munmap() which triggered the
 * release.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 * fi->flags will contain the same flags as for open.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */
const openMtimeCacheMaxSize = 8192

// applyKeepCacheFlag compares the entry's mtime (seconds + nanoseconds) against
// the last-seen value and sets FOPEN_KEEP_CACHE when unchanged.
func (wfs *WFS) applyKeepCacheFlag(inode uint64, entry *LockedEntry, out *fuse.OpenOut) {
	currentMtime := [2]int64{entry.Attributes.Mtime, int64(entry.Attributes.MtimeNs)}
	wfs.openMtimeMu.Lock()
	prev, loaded := wfs.openMtimeCache[inode]
	if loaded && prev == currentMtime {
		out.OpenFlags |= fuse.FOPEN_KEEP_CACHE
	} else {
		if len(wfs.openMtimeCache) >= openMtimeCacheMaxSize {
			for k := range wfs.openMtimeCache {
				delete(wfs.openMtimeCache, k)
				break
			}
		}
		wfs.openMtimeCache[inode] = currentMtime
	}
	wfs.openMtimeMu.Unlock()
}

// invalidateOpenMtimeCache removes an inode's cached mtime so the next Open
// does not set FOPEN_KEEP_CACHE with stale kernel page cache data.
func (wfs *WFS) invalidateOpenMtimeCache(inode uint64) {
	wfs.openMtimeMu.Lock()
	delete(wfs.openMtimeCache, inode)
	wfs.openMtimeMu.Unlock()
}

func (wfs *WFS) Release(cancel <-chan struct{}, in *fuse.ReleaseIn) {
	// Flush is usually sent before Release, but the FUSE protocol does not
	// guarantee it. Route every Release through doFlush so a dirty handle
	// (e.g. a deferred create with no intervening Flush) is not dropped.
	// doFlush itself inspects dirtyMetadata / asyncFlushPending and fast-paths
	// the clean case, so the duplicate call after a normal Flush is cheap.
	if fh := wfs.GetHandle(FileHandleId(in.Fh)); fh != nil {
		allowAsync := in.ReleaseFlags&fuse.FUSE_RELEASE_FLOCK_UNLOCK == 0
		if status := wfs.doFlush(fh, in.Uid, in.Gid, allowAsync); status != fuse.OK {
			glog.Warningf("release fh %d inode %d: fallback flush failed: %v", in.Fh, in.NodeId, status)
		}
	}
	if in.ReleaseFlags&fuse.FUSE_RELEASE_FLOCK_UNLOCK != 0 {
		wfs.posixLocks.ReleaseFlockOwner(in.NodeId, in.LockOwner)
	}
	wfs.ReleaseHandle(FileHandleId(in.Fh))
}
