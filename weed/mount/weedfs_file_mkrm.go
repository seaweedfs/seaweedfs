package mount

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 */
func (wfs *WFS) Create(cancel <-chan struct{}, in *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	if s := checkName(name); s != fuse.OK {
		return s
	}

	dirFullPath, code := wfs.inodeToPath.GetPath(in.NodeId)
	if code != fuse.OK {
		return code
	}

	entryFullPath := dirFullPath.Child(name)
	var inode uint64

	newEntry, code := wfs.maybeLoadEntry(entryFullPath)
	if code == fuse.OK {
		if newEntry == nil || newEntry.Attributes == nil {
			return fuse.EIO
		}
		if in.Flags&syscall.O_EXCL != 0 {
			glog.V(0).Infof("Create O_EXCL %s: already exists (uid=%d gid=%d mode=%o)",
				entryFullPath, newEntry.Attributes.Uid, newEntry.Attributes.Gid, newEntry.Attributes.FileMode)
			return fuse.Status(syscall.EEXIST)
		}
		inode = wfs.inodeToPath.Lookup(entryFullPath, newEntry.Attributes.Crtime, false, len(newEntry.HardLinkId) > 0, newEntry.Attributes.Inode, true)
		fileHandle, status := wfs.AcquireHandle(inode, in.Flags, in.Uid, in.Gid)
		if status != fuse.OK {
			return status
		}
		if in.Flags&syscall.O_TRUNC != 0 && in.Flags&fuse.O_ANYWRITE != 0 {
			if code = wfs.truncateEntry(entryFullPath, newEntry); code != fuse.OK {
				wfs.ReleaseHandle(fileHandle.fh)
				return code
			}
			newEntry = fileHandle.GetEntry().GetEntry()
		}

		wfs.outputPbEntry(&out.EntryOut, inode, newEntry)
		out.Fh = uint64(fileHandle.fh)
		out.OpenFlags = 0
		return fuse.OK
	}
	if code != fuse.ENOENT {
		return code
	}

	inode, newEntry, code = wfs.createRegularFile(dirFullPath, name, in.Mode, in.Uid, in.Gid, 0, true)
	if code == fuse.Status(syscall.EEXIST) && in.Flags&syscall.O_EXCL == 0 {
		// Race: another process created the file between our check and create.
		// Reopen the winner's entry.
		newEntry, code = wfs.maybeLoadEntry(entryFullPath)
		if code != fuse.OK {
			return code
		}
		if newEntry == nil || newEntry.Attributes == nil {
			return fuse.EIO
		}
		inode = wfs.inodeToPath.Lookup(entryFullPath, newEntry.Attributes.Crtime, false, len(newEntry.HardLinkId) > 0, newEntry.Attributes.Inode, true)
		fileHandle, status := wfs.AcquireHandle(inode, in.Flags, in.Uid, in.Gid)
		if status != fuse.OK {
			return status
		}
		if in.Flags&syscall.O_TRUNC != 0 && in.Flags&fuse.O_ANYWRITE != 0 {
			if code = wfs.truncateEntry(entryFullPath, newEntry); code != fuse.OK {
				wfs.ReleaseHandle(fileHandle.fh)
				return code
			}
			newEntry = fileHandle.GetEntry().GetEntry()
		}
		wfs.outputPbEntry(&out.EntryOut, inode, newEntry)
		out.Fh = uint64(fileHandle.fh)
		out.OpenFlags = 0
		return fuse.OK
	} else if code != fuse.OK {
		return code
	} else {
		inode = wfs.inodeToPath.Lookup(entryFullPath, newEntry.Attributes.Crtime, false, false, inode, true)
	}

	wfs.outputPbEntry(&out.EntryOut, inode, newEntry)

	// For deferred creates, bypass AcquireHandle (which calls maybeReadEntry
	// and would fail since the entry is not yet on the filer or in the meta cache).
	// We already have the entry from createRegularFile, so create the handle directly.
	fileHandle := wfs.fhMap.AcquireFileHandle(wfs, inode, newEntry)
	fileHandle.RememberPath(entryFullPath)
	// Mark dirty so the deferred filer create happens on Flush,
	// even if the file is closed without any writes.
	fileHandle.dirtyMetadata = true

	// Acquire DLM lock for new file creation (Create bypasses AcquireHandle
	// so we must acquire the lock here). Always lock on Create since file
	// creation is inherently a write operation.
	if wfs.lockClient != nil && fileHandle.dlmLock == nil {
		owner := fmt.Sprintf("mount-%d", wfs.signature)
		fileHandle.dlmLock = wfs.lockClient.NewBlockingLongLivedLock(
			string(entryFullPath), owner, lock_manager.LiveLockTTL,
		)
		glog.V(1).Infof("DLM lock acquired for new file %s", entryFullPath)
	}

	out.Fh = uint64(fileHandle.fh)
	out.OpenFlags = 0

	return fuse.OK
}

/** Create a file node
 *
 * This is called for creation of all non-directory, non-symlink
 * nodes.  If the filesystem defines a create() method, then for
 * regular files that will be called instead.
 */
func (wfs *WFS) Mknod(cancel <-chan struct{}, in *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {

	if s := checkName(name); s != fuse.OK {
		return s
	}

	dirFullPath, code := wfs.inodeToPath.GetPath(in.NodeId)
	if code != fuse.OK {
		return
	}

	inode, newEntry, code := wfs.createRegularFile(dirFullPath, name, in.Mode, in.Uid, in.Gid, in.Rdev, false)
	if code != fuse.OK {
		return code
	}

	// this is to increase nlookup counter
	entryFullPath := dirFullPath.Child(name)
	inode = wfs.inodeToPath.Lookup(entryFullPath, newEntry.Attributes.Crtime, false, false, inode, true)

	wfs.outputPbEntry(out, inode, newEntry)

	return fuse.OK

}

/** Remove a file */
func (wfs *WFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {

	dirFullPath, code := wfs.inodeToPath.GetPath(header.NodeId)
	if code != fuse.OK {
		if code == fuse.ENOENT {
			return fuse.OK
		}
		return code
	}
	entryFullPath := dirFullPath.Child(name)

	entry, code := wfs.maybeLoadEntry(entryFullPath)
	if code != fuse.OK {
		if code == fuse.ENOENT {
			return fuse.OK
		}
		return code
	}

	if wormEnforced, _ := wfs.wormEnforcedForEntry(entryFullPath, entry); wormEnforced {
		return fuse.EPERM
	}

	// Before deleting from the filer, mark any draining async-flush handle
	// as deleted and wait for it to complete.  Without this, the async flush
	// can race with the filer delete and recreate the just-unlinked entry
	// (the worker checks isDeleted, but it may have already passed that check
	// before Unlink sets the flag).  By waiting here, any in-flight flush
	// finishes first; even if it recreated the entry, the filer delete below
	// will remove it again.
	if inode, found := wfs.inodeToPath.GetInode(entryFullPath); found {
		if fh, fhFound := wfs.fhMap.FindFileHandle(inode); fhFound {
			fh.isDeleted = true
		}
		wfs.waitForPendingAsyncFlush(inode)
	} else if entry != nil && entry.Attributes != nil && entry.Attributes.Inode != 0 {
		inodeFromEntry := entry.Attributes.Inode
		if fh, fhFound := wfs.fhMap.FindFileHandle(inodeFromEntry); fhFound {
			fh.isDeleted = true
		}
		wfs.waitForPendingAsyncFlush(inodeFromEntry)
	}

	// first, ensure the filer store can correctly delete
	glog.V(3).Infof("remove file: %v", entryFullPath)
	// Always let the filer decide whether to delete chunks based on its authoritative data.
	// The filer has the correct hard link count and will only delete chunks when appropriate.
	deleteReq := &filer_pb.DeleteEntryRequest{
		Directory:    string(dirFullPath),
		Name:         name,
		IsDeleteData: true,
		Signatures:   []int32{wfs.signature},
	}
	resp, err := wfs.streamDeleteEntry(context.Background(), deleteReq)
	if err != nil {
		glog.V(0).Infof("remove %s: %v", entryFullPath, err)
		return fuse.OK
	}

	var event *filer_pb.SubscribeMetadataResponse
	if resp != nil && resp.MetadataEvent != nil {
		event = resp.MetadataEvent
	} else {
		event = metadataDeleteEvent(string(dirFullPath), name, false)
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("unlink %s: best-effort metadata apply failed: %v", entryFullPath, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(dirFullPath)
	}
	wfs.inodeToPath.TouchDirectory(dirFullPath)

	wfs.inodeToPath.RemovePath(entryFullPath)

	return fuse.OK

}

func (wfs *WFS) createRegularFile(dirFullPath util.FullPath, name string, mode uint32, uid, gid, rdev uint32, deferFilerCreate bool) (inode uint64, newEntry *filer_pb.Entry, code fuse.Status) {
	if wfs.IsOverQuotaWithUncommitted() {
		return 0, nil, fuse.Status(syscall.ENOSPC)
	}

	// Verify write+search permission on the parent directory.
	parentEntry, parentStatus := wfs.maybeLoadEntry(dirFullPath)
	if parentStatus != fuse.OK {
		return 0, nil, parentStatus
	}
	if parentEntry == nil || parentEntry.Attributes == nil {
		return 0, nil, fuse.EIO
	}
	// Map parent dir uid/gid from filer-space to local-space so the
	// permission check compares like with like (caller uid/gid are local).
	parentUid, parentGid := parentEntry.Attributes.Uid, parentEntry.Attributes.Gid
	if wfs.option.UidGidMapper != nil {
		parentUid, parentGid = wfs.option.UidGidMapper.FilerToLocal(parentUid, parentGid)
	}
	if !hasAccess(uid, gid, parentUid, parentGid, parentEntry.Attributes.FileMode, fuse.W_OK|fuse.X_OK) {
		return 0, nil, fuse.Status(syscall.EACCES)
	}

	entryFullPath := dirFullPath.Child(name)
	if _, status := wfs.maybeLoadEntry(entryFullPath); status == fuse.OK {
		return 0, nil, fuse.Status(syscall.EEXIST)
	} else if status != fuse.ENOENT {
		return 0, nil, status
	}
	fileMode := toOsFileMode(mode)
	now := time.Now().Unix()
	inode = wfs.inodeToPath.AllocateInode(entryFullPath, now)

	newEntry = &filer_pb.Entry{
		Name:        name,
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    now,
			Crtime:   now,
			FileMode: uint32(fileMode),
			Uid:      uid,
			Gid:      gid,
			TtlSec:   wfs.option.TtlSec,
			Rdev:     rdev,
			Inode:    inode,
		},
	}

	if deferFilerCreate {
		// Defer the filer gRPC call to flush time. The caller (Create) will
		// build a file handle directly from newEntry, bypassing AcquireHandle.
		// Insert a local placeholder into the metadata cache so that
		// maybeLoadEntry() can find the file (e.g., duplicate-create checks,
		// stat, readdir). The actual filer entry is created by flushMetadataToFiler.
		// We use InsertEntry directly instead of applyLocalMetadataEvent to avoid
		// triggering directory hot-threshold eviction that would wipe the entry.
		if insertErr := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(string(dirFullPath), newEntry)); insertErr != nil {
			glog.Warningf("createFile %s: insert local entry: %v", entryFullPath, insertErr)
		}
		glog.V(3).Infof("createFile %s: deferred to flush", entryFullPath)
		return inode, newEntry, fuse.OK
	}

	wfs.mapPbIdFromLocalToFiler(newEntry)
	defer wfs.mapPbIdFromFilerToLocal(newEntry)

	request := &filer_pb.CreateEntryRequest{
		Directory:                string(dirFullPath),
		Entry:                    newEntry,
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	}

	glog.V(1).Infof("createFile: %v", request)
	resp, err := wfs.streamCreateEntry(context.Background(), request)
	if err != nil {
		glog.V(0).Infof("createFile %s: %v", entryFullPath, err)
	} else {
		event := resp.GetMetadataEvent()
		if event == nil {
			event = metadataCreateEvent(string(dirFullPath), newEntry)
		}
		if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
			glog.Warningf("createFile %s: best-effort metadata apply failed: %v", entryFullPath, applyErr)
			wfs.inodeToPath.InvalidateChildrenCache(dirFullPath)
		}
		wfs.inodeToPath.TouchDirectory(dirFullPath)
	}

	glog.V(3).Infof("createFile %s: %v", entryFullPath, err)

	if err != nil {
		return 0, nil, grpcErrorToFuseStatus(err)
	}

	return inode, newEntry, fuse.OK
}

func (wfs *WFS) truncateEntry(entryFullPath util.FullPath, entry *filer_pb.Entry) fuse.Status {
	if entry == nil {
		return fuse.EIO
	}
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}

	entry.Content = nil
	entry.Chunks = nil
	entry.Attributes.FileSize = 0
	entry.Attributes.Mtime = time.Now().Unix()

	if code := wfs.saveEntry(entryFullPath, entry); code != fuse.OK {
		return code
	}

	if inode, found := wfs.inodeToPath.GetInode(entryFullPath); found {
		if fh, fhFound := wfs.fhMap.FindFileHandle(inode); fhFound {
			fhActiveLock := fh.wfs.fhLockTable.AcquireLock("truncateEntry", fh.fh, util.ExclusiveLock)
			fh.ResetDirtyPages()
			fh.SetEntry(entry)
			fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
		}
	}

	return fuse.OK
}
