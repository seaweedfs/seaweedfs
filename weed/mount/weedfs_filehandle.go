package mount

import (
	"fmt"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
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

	path, pathStatus := wfs.inodeToPath.GetPath(inode)
	if pathStatus != fuse.OK {
		return nil, pathStatus
	}

	// A fresh lookup returns the entry with the filer log position it
	// reflects; the handle takes that entry and version as one decision in
	// AcquireFileHandleWithVersion, so a slower concurrent opener cannot
	// overwrite a newer install. An existing handle keeps its own entry and
	// version — they did not come from this lookup.
	var entry *filer_pb.Entry
	var entryVersionTsNs int64
	freshLookup := false
	if existingFh, found := wfs.fhMap.FindFileHandle(inode); found {
		entry = existingFh.UpdateEntry(func(entry *filer_pb.Entry) {
			if entry != nil && existingFh.entry.Attributes == nil {
				entry.Attributes = &filer_pb.FuseAttributes{}
			}
		})
		entryVersionTsNs = existingFh.entryVersionTsNs.Load()
	} else {
		entry, entryVersionTsNs, status = wfs.maybeLoadEntryWithVersion(path)
		if status != fuse.OK {
			return nil, status
		}
		freshLookup = true
	}
	if wormEnforced, _ := wfs.wormEnforcedForEntry(path, entry); wormEnforced && flags&fuse.O_ANYWRITE != 0 {
		return nil, fuse.EPERM
	}
	// Check unix permission bits for the requested access mode. With
	// default_permissions the kernel already enforced them before this
	// open, so the check (and its supplementary-group lookup) is redundant.
	if !wfs.option.DefaultPermissions && entry != nil && entry.Attributes != nil {
		fileUid, fileGid := entry.Attributes.Uid, entry.Attributes.Gid
		if wfs.option.UidGidMapper != nil {
			fileUid, fileGid = wfs.option.UidGidMapper.FilerToLocal(fileUid, fileGid)
		}
		if mask := openFlagsToAccessMask(flags); mask != 0 && !hasAccess(uid, gid, fileUid, fileGid, entry.Attributes.FileMode, mask) {
			return nil, fuse.EACCES
		}
	}
	// need to AcquireFileHandle again to ensure correct handle counter
	var existed bool
	fileHandle, existed = wfs.fhMap.AcquireFileHandleWithVersion(wfs, inode, entry, entryVersionTsNs)
	fileHandle.RememberPath(path)
	if existed && freshLookup {
		// Another opener created the handle while our lookup was in flight.
		// Install under the handle lock every user synchronizes on, and only
		// state that provably improves on what the handle holds: reject
		// dirty handles (local writes would be lost), unknown versions (an
		// unversioned response cannot outrank anything), and anything not
		// strictly newer.
		fhActiveLock := wfs.fhLockTable.AcquireLock("AcquireHandle", fileHandle.fh, util.ExclusiveLock)
		if entryVersionTsNs != 0 && entryVersionTsNs > fileHandle.entryVersionTsNs.Load() &&
			!fileHandle.dirtyMetadata && entry != fileHandle.GetEntry().GetEntry() {
			fileHandle.SetEntry(entry)
			fileHandle.advanceEntryVersionTsNs(entryVersionTsNs)
		}
		wfs.fhLockTable.ReleaseLock(fileHandle.fh, fhActiveLock)
	}

	// Acquire distributed lock for write opens. The lock is held with
	// auto-renewal until the file handle is released (close).
	// Use the filer path as the lock key since inode numbers are
	// assigned per-mount and differ across mount instances.
	if wfs.lockClient != nil && flags&fuse.O_ANYWRITE != 0 && fileHandle.dlmLock == nil {
		owner := fmt.Sprintf("mount-%d", wfs.signature)
		fileHandle.dlmLock = wfs.lockClient.NewBlockingLongLivedLock(
			string(path), owner, lock_manager.LiveLockTTL,
		)
		glog.V(1).Infof("DLM lock acquired for %s", path)
	}
	return fileHandle, fuse.OK
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
