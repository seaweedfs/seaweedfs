package mount

import (
	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (wfs *WFS) AcquireHandle(inode uint64, flags, uid, gid uint32) (fileHandle *FileHandle, status fuse.Status) {
	// If there is an in-flight async flush for this inode, wait for it to
	// complete before reopening.  Otherwise the new handle would be built
	// from pre-close filer metadata and its next flush could overwrite the
	// data that was just written asynchronously.
	wfs.waitForPendingAsyncFlush(inode)

	var entry *filer_pb.Entry
	var path util.FullPath
	path, _, entry, status = wfs.maybeReadEntry(inode)
	if status == fuse.OK {
		if wormEnforced, _ := wfs.wormEnforcedForEntry(path, entry); wormEnforced && flags&fuse.O_ANYWRITE != 0 {
			return nil, fuse.EPERM
		}
		// Check unix permission bits for the requested access mode.
		if entry != nil && entry.Attributes != nil {
			if mask := openFlagsToAccessMask(flags); mask != 0 && !hasAccess(uid, gid, entry.Attributes.Uid, entry.Attributes.Gid, entry.Attributes.FileMode, mask) {
				return nil, fuse.EACCES
			}
		}
		// need to AcquireFileHandle again to ensure correct handle counter
		fileHandle = wfs.fhMap.AcquireFileHandle(wfs, inode, entry)
		fileHandle.RememberPath(path)
	}
	return
}

// ReleaseHandle is called from FUSE Release.  For handles with a pending
// async flush, the map removal and the pendingAsyncFlush registration are
// done under a single lock hold so that a concurrent AcquireHandle cannot
// slip through the gap between the two (P1-1 TOCTOU fix).
//
// The handle intentionally stays in fhMap during the drain so that rename
// and unlink can still find it via FindFileHandle (P1-2 fix).  It is
// removed from fhMap only after the drain completes (RemoveFileHandle).
func (wfs *WFS) ReleaseHandle(handleId FileHandleId) {
	// Hold pendingAsyncFlushMu across the counter decrement and the
	// pending-flush registration.  Lock ordering: pendingAsyncFlushMu → fhMap.
	wfs.pendingAsyncFlushMu.Lock()
	fhToRelease := wfs.fhMap.ReleaseByHandle(handleId)
	if fhToRelease != nil && fhToRelease.asyncFlushPending {
		done := make(chan struct{})
		wfs.pendingAsyncFlush[fhToRelease.inode] = done
		// Add(1) while holding the mutex so WaitForAsyncFlush cannot
		// observe a zero counter and close the channel before we send.
		wfs.asyncFlushWg.Add(1)
		wfs.pendingAsyncFlushMu.Unlock()

		// Send after unlock to avoid deadlock — workers acquire the
		// same mutex during cleanup.
		wfs.asyncFlushCh <- &asyncFlushItem{fh: fhToRelease, done: done}
		return
	}
	wfs.pendingAsyncFlushMu.Unlock()

	if fhToRelease != nil {
		fhToRelease.ReleaseHandle()
	}
}

// waitForPendingAsyncFlush blocks until any in-flight async flush for
// the given inode completes.  Called from AcquireHandle before building
// new handle state, so the filer metadata reflects the flushed data.
func (wfs *WFS) waitForPendingAsyncFlush(inode uint64) {
	wfs.pendingAsyncFlushMu.Lock()
	done, found := wfs.pendingAsyncFlush[inode]
	wfs.pendingAsyncFlushMu.Unlock()

	if found {
		glog.V(3).Infof("waitForPendingAsyncFlush: waiting for inode %d", inode)
		<-done
	}
}

func (wfs *WFS) GetHandle(handleId FileHandleId) *FileHandle {
	return wfs.fhMap.GetFileHandle(handleId)
}
