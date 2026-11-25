package mount

import (
	"syscall"
	"unicode/utf8"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Lookup is called by the kernel when the VFS wants to know
// about a file inside a directory. Many lookup calls can
// occur in parallel, but only one call happens for each (dir,
// name) pair.

func (wfs *WFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {

	if !utf8.ValidString(name) {
		return fuse.ENOENT
	}
	var s fuse.Status
	if name, s = checkName(name); s != fuse.OK {
		return s
	}

	var fullFilePath util.FullPath
	var possibleInode uint64
	if name == "." || name == ".." {
		if fullFilePath, possibleInode, code = wfs.resolveDotName(header.NodeId, name); code != fuse.OK {
			return code
		}
		if fullFilePath == util.FullPath(wfs.option.FilerMountRootPath) {
			wfs.outputRootEntry(out)
			return fuse.OK
		}
	} else {
		dirPath, code := wfs.inodeToPath.GetPath(header.NodeId)
		if code != fuse.OK {
			return code
		}
		fullFilePath = dirPath.Child(name)
	}

	// Use shared lookup logic that checks cache first, then filer if needed
	ctx, cancelFunc := ctxFromCancel(cancel)
	defer cancelFunc()
	localEntry, _, status := wfs.lookupEntry(ctx, fullFilePath)
	if status != fuse.OK {
		if status == fuse.ENOENT && name == "." {
			return fuse.Status(syscall.ESTALE)
		}
		return status
	}
	inode := possibleInode
	if name == "." {
		if !sameFile(inode, fullFilePath, localEntry) || !wfs.inodeToPath.LookupHandle(fullFilePath, inode, localEntry.IsDirectory()) {
			return fuse.Status(syscall.ESTALE)
		}
	} else {
		inode = wfs.inodeToPath.Lookup(fullFilePath, localEntry.Crtime.Unix(), localEntry.IsDirectory(), len(localEntry.HardLinkId) > 0, localEntry.Inode, true)
	}

	if fh, found := wfs.fhMap.FindFileHandle(inode); found {
		fh.entryLock.RLock()
		if entry := fh.GetEntry().GetEntry(); entry != nil {
			glog.V(4).Infof("lookup opened file %s size %d", fullFilePath, filer.FileSize(entry))
			dir, _ := fullFilePath.DirAndName()
			localEntry = filer.FromPbEntry(dir, entry)
		}
		fh.entryLock.RUnlock()
	}

	wfs.outputFilerEntry(out, inode, localEntry)

	if localEntry.IsDirectory() {
		wfs.applyInMemoryDirMtime(&out.Attr, inode)
		if wfs.option.PosixDirNlink {
			wfs.applyDirNlink(&out.Attr, fullFilePath)
		}
	}

	return fuse.OK

}

// resolveDotName answers the lookups the kernel sends to decode a file
// handle: "." on the handle's node id, which it may have forgotten, and ".."
// for a directory's parent. "." keeps the handle's node id, or fails ESTALE
// when its path now belongs to another inode.
func (wfs *WFS) resolveDotName(nodeId uint64, name string) (util.FullPath, uint64, fuse.Status) {
	path, code := wfs.inodeToPath.GetPath(nodeId)
	if code != fuse.OK {
		forgotten, found := wfs.inodeToPath.GetForgottenPath(nodeId)
		if !found {
			return "", 0, fuse.Status(syscall.ESTALE)
		}
		if inode, known := wfs.inodeToPath.GetInode(forgotten); known && inode != nodeId {
			return "", 0, fuse.Status(syscall.ESTALE)
		}
		path = forgotten
	}
	if name == "." {
		return path, nodeId, fuse.OK
	}
	if path == util.FullPath(wfs.option.FilerMountRootPath) {
		return path, 0, fuse.OK
	}
	dir, _ := path.DirAndName()
	return util.FullPath(dir), 0, fuse.OK
}

func (wfs *WFS) outputRootEntry(out *fuse.EntryOut) {
	wfs.inodeToPath.IncrementNlookup(1)
	var attr fuse.AttrOut
	wfs.setRootAttr(&attr)
	out.NodeId = 1
	out.Generation = 1
	out.EntryValid = wfs.entryValidSec
	out.AttrValid = attr.AttrValid
	out.Attr = attr.Attr
}

// sameFile reports whether entry, found at path, is the file inode names, and
// not one created at that path since.
func sameFile(inode uint64, path util.FullPath, entry *filer.Entry) bool {
	if entry.Inode != 0 {
		return entry.Inode == inode
	}
	return path.AsInode(entry.Crtime.Unix()) == inode
}
