package mount

import (
	"context"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

/** Create a directory
 *
 * Note that the mode argument may not have the type specification
 * bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
 * correct directory type bits use  mode|S_IFDIR
 * */
func (wfs *WFS) Mkdir(cancel <-chan struct{}, in *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {

	if wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}

	if s := checkName(name); s != fuse.OK {
		return s
	}

	now := time.Now().Unix()
	newEntry := &filer_pb.Entry{
		Name:        name,
		IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    now,
			Crtime:   now,
			Ctime:    now,
			FileMode: uint32(os.ModeDir) | in.Mode&^uint32(wfs.option.Umask),
			Uid:      in.Uid,
			Gid:      in.Gid,
		},
	}

	dirFullPath, code := wfs.inodeToPath.GetPath(in.NodeId)
	if code != fuse.OK {
		return
	}

	entryFullPath := dirFullPath.Child(name)

	wfs.mapPbIdFromLocalToFiler(newEntry)
	// Defer restoring to local uid/gid AFTER the entry is sent to the filer
	// but BEFORE outputPbEntry writes attributes to the kernel.  We restore
	// explicitly below instead of using defer so the kernel gets local values.

	request := &filer_pb.CreateEntryRequest{
		Directory:                string(dirFullPath),
		Entry:                    newEntry,
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	}

	glog.V(1).Infof("mkdir: %v", request)
	resp, err := wfs.streamCreateEntry(context.Background(), request)
	if err != nil {
		glog.V(0).Infof("mkdir %s: %v", entryFullPath, err)
	} else {
		event := resp.GetMetadataEvent()
		if event == nil {
			event = metadataCreateEvent(string(dirFullPath), newEntry)
		}
		if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
			glog.Warningf("mkdir %s: best-effort metadata apply failed: %v", entryFullPath, applyErr)
			wfs.inodeToPath.InvalidateChildrenCache(dirFullPath)
		}
		wfs.inodeToPath.TouchDirectory(dirFullPath)
		wfs.touchDirMtimeCtime(dirFullPath)
	}

	glog.V(3).Infof("mkdir %s: %v", entryFullPath, err)

	if err != nil {
		wfs.mapPbIdFromFilerToLocal(newEntry)
		return fuse.EIO
	}

	// Map uid/gid back to local-space before writing attributes to the
	// kernel.  The kernel (especially macFUSE) caches these and uses them
	// for subsequent permission checks on children.
	wfs.mapPbIdFromFilerToLocal(newEntry)

	inode := wfs.inodeToPath.Lookup(entryFullPath, newEntry.Attributes.Crtime, true, false, 0, true)

	wfs.outputPbEntry(out, inode, newEntry)

	return fuse.OK

}

/** Remove a directory */
func (wfs *WFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {

	if name == "." {
		return fuse.Status(syscall.EINVAL)
	}
	if name == ".." {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	dirFullPath, code := wfs.inodeToPath.GetPath(header.NodeId)
	if code != fuse.OK {
		return
	}
	entryFullPath := dirFullPath.Child(name)

	// POSIX: enforce sticky bit on the parent directory.
	if dirEntry, dirCode := wfs.maybeLoadEntry(dirFullPath); dirCode == fuse.OK && dirEntry != nil && dirEntry.Attributes != nil {
		targetUid := uint32(0)
		if targetEntry, targetCode := wfs.maybeLoadEntry(entryFullPath); targetCode == fuse.OK && targetEntry != nil && targetEntry.Attributes != nil {
			targetUid = targetEntry.Attributes.Uid
		}
		if code := checkStickyBit(dirEntry.Attributes.FileMode, dirEntry.Attributes.Uid, targetUid, header.Uid); code != fuse.OK {
			return code
		}
	}

	glog.V(3).Infof("remove directory: %v", entryFullPath)
	deleteReq := &filer_pb.DeleteEntryRequest{
		Directory:            string(dirFullPath),
		Name:                 name,
		IsDeleteData:         true,
		IgnoreRecursiveError: true, // ignore recursion error since the OS should manage it
		Signatures:           []int32{wfs.signature},
	}
	resp, err := wfs.streamDeleteEntry(context.Background(), deleteReq)
	if err != nil {
		glog.V(1).Infof("remove %s: %v", entryFullPath, err)
		if strings.Contains(err.Error(), filer.MsgFailDelNonEmptyFolder) {
			return fuse.Status(syscall.ENOTEMPTY)
		}
		return fuse.ENOENT
	}

	event := metadataDeleteEvent(string(dirFullPath), name, true)
	if resp != nil && resp.MetadataEvent != nil {
		event = resp.MetadataEvent
	}
	if applyErr := wfs.applyLocalMetadataEvent(context.Background(), event); applyErr != nil {
		glog.Warningf("rmdir %s: best-effort metadata apply failed: %v", entryFullPath, applyErr)
		wfs.inodeToPath.InvalidateChildrenCache(dirFullPath)
	}
	wfs.inodeToPath.RemovePath(entryFullPath)
	wfs.inodeToPath.TouchDirectory(dirFullPath)
	wfs.touchDirMtimeCtime(dirFullPath)

	return fuse.OK

}
