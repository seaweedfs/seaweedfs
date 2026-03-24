package mount

import (
	"context"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
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

	inode, newEntry, code = wfs.createRegularFile(dirFullPath, name, in.Mode, in.Uid, in.Gid, 0)
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

	fileHandle, status := wfs.AcquireHandle(inode, in.Flags, in.Uid, in.Gid)
	if status != fuse.OK {
		return status
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

	inode, newEntry, code := wfs.createRegularFile(dirFullPath, name, in.Mode, in.Uid, in.Gid, in.Rdev)
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

	// first, ensure the filer store can correctly delete
	glog.V(3).Infof("remove file: %v", entryFullPath)
	// Always let the filer decide whether to delete chunks based on its authoritative data.
	// The filer has the correct hard link count and will only delete chunks when appropriate.
	resp, err := filer_pb.RemoveWithResponse(context.Background(), wfs, string(dirFullPath), name, true, false, false, false, []int32{wfs.signature})
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

	// If there is an async-draining handle for this file, mark it as deleted
	// so the background flush skips the metadata write instead of recreating
	// the just-unlinked entry.  The handle is still in fhMap during drain.
	if inode, found := wfs.inodeToPath.GetInode(entryFullPath); found {
		if fh, fhFound := wfs.fhMap.FindFileHandle(inode); fhFound {
			fh.isDeleted = true
		}
	}

	wfs.inodeToPath.RemovePath(entryFullPath)

	return fuse.OK

}

func (wfs *WFS) createRegularFile(dirFullPath util.FullPath, name string, mode uint32, uid, gid, rdev uint32) (inode uint64, newEntry *filer_pb.Entry, code fuse.Status) {
	if wfs.IsOverQuotaWithUncommitted() {
		return 0, nil, fuse.Status(syscall.ENOSPC)
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

	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		wfs.mapPbIdFromLocalToFiler(newEntry)
		defer wfs.mapPbIdFromFilerToLocal(newEntry)

		request := &filer_pb.CreateEntryRequest{
			Directory:                string(dirFullPath),
			Entry:                    newEntry,
			Signatures:               []int32{wfs.signature},
			SkipCheckParentDirectory: true,
		}

		glog.V(1).Infof("createFile: %v", request)
		resp, err := filer_pb.CreateEntryWithResponse(context.Background(), client, request)
		if err != nil {
			glog.V(0).Infof("createFile %s: %v", entryFullPath, err)
			return err
		}

		event := resp.GetMetadataEvent()
		if event == nil {
			event = metadataCreateEvent(string(dirFullPath), newEntry)
		}
		if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
			glog.Warningf("createFile %s: best-effort metadata apply failed: %v", entryFullPath, applyErr)
			wfs.inodeToPath.InvalidateChildrenCache(dirFullPath)
		}
		wfs.inodeToPath.TouchDirectory(dirFullPath)

		return nil
	})

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
