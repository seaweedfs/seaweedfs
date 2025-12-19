package mount

import (
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Lookup is called by the kernel when the VFS wants to know
// about a file inside a directory. Many lookup calls can
// occur in parallel, but only one call happens for each (dir,
// name) pair.

func (wfs *WFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {

	if s := checkName(name); s != fuse.OK {
		return s
	}

	dirPath, code := wfs.inodeToPath.GetPath(header.NodeId)
	if code != fuse.OK {
		return
	}

	fullFilePath := dirPath.Child(name)

	// Use shared lookup logic that checks cache first, then filer if needed
	localEntry, status := wfs.lookupEntry(fullFilePath)
	if status != fuse.OK {
		return status
	}

	inode := wfs.inodeToPath.Lookup(fullFilePath, localEntry.Crtime.Unix(), localEntry.IsDirectory(), len(localEntry.HardLinkId) > 0, localEntry.Inode, true)

	if fh, found := wfs.fhMap.FindFileHandle(inode); found {
		fh.entryLock.RLock()
		if entry := fh.GetEntry().GetEntry(); entry != nil {
			glog.V(4).Infof("lookup opened file %s size %d", dirPath.Child(localEntry.Name()), filer.FileSize(entry))
			localEntry = filer.FromPbEntry(string(dirPath), entry)
		}
		fh.entryLock.RUnlock()
	}

	wfs.outputFilerEntry(out, inode, localEntry)

	return fuse.OK

}
