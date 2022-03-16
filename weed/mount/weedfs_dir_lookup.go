package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/mount/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/hanwen/go-fuse/v2/fuse"
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

	visitErr := meta_cache.EnsureVisited(wfs.metaCache, wfs, dirPath, nil)
	if visitErr != nil {
		glog.Errorf("dir Lookup %s: %v", dirPath, visitErr)
		return fuse.EIO
	}
	localEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), fullFilePath)
	if cacheErr == filer_pb.ErrNotFound {
		return fuse.ENOENT
	}

	if localEntry == nil {
		// glog.V(3).Infof("dir Lookup cache miss %s", fullFilePath)
		entry, err := filer_pb.GetEntry(wfs, fullFilePath)
		if err != nil {
			glog.V(1).Infof("dir GetEntry %s: %v", fullFilePath, err)
			return fuse.ENOENT
		}
		localEntry = filer.FromPbEntry(string(dirPath), entry)
	} else {
		glog.V(4).Infof("dir Lookup cache hit %s", fullFilePath)
	}

	if localEntry == nil {
		return fuse.ENOENT
	}

	inode := wfs.inodeToPath.Lookup(fullFilePath, localEntry.Crtime.Unix(), localEntry.IsDirectory(), len(localEntry.HardLinkId) > 0, localEntry.Inode, true)

	if fh, found := wfs.fhmap.FindFileHandle(inode); found && fh.entry != nil {
		glog.V(4).Infof("lookup opened file %s size %d", dirPath.Child(localEntry.Name()), filer.FileSize(fh.entry))
		localEntry = filer.FromPbEntry(string(dirPath), fh.entry)
	}

	wfs.outputFilerEntry(out, inode, localEntry)

	return fuse.OK

}
