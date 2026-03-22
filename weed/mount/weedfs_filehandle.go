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
		// need to AcquireFileHandle again to ensure correct handle counter
		fileHandle = wfs.fhMap.AcquireFileHandle(wfs, inode, entry)
	}
	return
}

func (wfs *WFS) ReleaseHandle(handleId FileHandleId) {
	fhToRelease := wfs.fhMap.ReleaseByHandle(handleId)
	wfs.finalizeFileHandle(fhToRelease)
}

// finalizeFileHandle either destroys the FileHandle immediately or, if an async
// flush is pending (writebackCache mode), submits a background goroutine to
// complete the data upload + metadata flush first.
func (wfs *WFS) finalizeFileHandle(fh *FileHandle) {
	if fh == nil {
		return
	}
	if fh.asyncFlushPending {
		wfs.submitAsyncFlush(fh)
	} else {
		fh.ReleaseHandle()
	}
}

// submitAsyncFlush registers an in-flight async flush for this inode and
// starts the background goroutine.  The per-inode channel allows
// AcquireHandle to block if the same inode is reopened before the flush
// finishes, preventing stale metadata from overwriting the flushed data.
func (wfs *WFS) submitAsyncFlush(fh *FileHandle) {
	done := make(chan struct{})

	wfs.pendingAsyncFlushMu.Lock()
	wfs.pendingAsyncFlush[fh.inode] = done
	wfs.pendingAsyncFlushMu.Unlock()

	wfs.asyncFlushWg.Add(1)
	go func() {
		defer wfs.asyncFlushWg.Done()
		defer func() {
			close(done)
			wfs.pendingAsyncFlushMu.Lock()
			delete(wfs.pendingAsyncFlush, fh.inode)
			wfs.pendingAsyncFlushMu.Unlock()
		}()
		wfs.completeAsyncFlush(fh)
	}()
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
