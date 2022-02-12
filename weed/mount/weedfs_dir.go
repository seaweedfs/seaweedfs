package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filesys/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/hanwen/go-fuse/v2/fuse"
	"math"
)

// Directory handling

func (wfs *WFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	return fuse.OK
}
func (wfs *WFS) ReleaseDir(input *fuse.ReleaseIn) {
}
func (wfs *WFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (wfs *WFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	return wfs.doReadDirectory(input, out, false)
}

func (wfs *WFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	return wfs.doReadDirectory(input, out, true)
}

func (wfs *WFS) doReadDirectory(input *fuse.ReadIn, out *fuse.DirEntryList, isPlusMode bool) fuse.Status {
	dirPath := wfs.inodeToPath.GetPath(input.NodeId)

	var dirEntry fuse.DirEntry
	processEachEntryFn := func(entry *filer.Entry, isLast bool) bool {
		dirEntry.Name = entry.Name()
		inode := wfs.inodeToPath.GetInode(dirPath.Child(dirEntry.Name))
		dirEntry.Ino = inode
		dirEntry.Mode = toSystemMode(entry.Mode)
		if !isPlusMode {
			if !out.AddDirEntry(dirEntry) {
				return false
			}

		} else {
			entryOut := out.AddDirLookupEntry(dirEntry)
			if entryOut == nil {
				return false
			}
			entryOut.Generation = 1
			entryOut.EntryValid = 1
			entryOut.AttrValid = 1
			wfs.setAttrByFilerEntry(&entryOut.Attr, inode, entry)
		}
		return true
	}

	// TODO remove this with checking whether directory is not forgotten
	if err := meta_cache.EnsureVisited(wfs.metaCache, wfs, dirPath); err != nil {
		glog.Errorf("dir ReadDirAll %s: %v", dirPath, err)
		return fuse.EIO
	}
	listErr := wfs.metaCache.ListDirectoryEntries(context.Background(), dirPath, "", false, int64(math.MaxInt32), func(entry *filer.Entry) bool {
		return processEachEntryFn(entry, false)
	})
	if listErr != nil {
		glog.Errorf("list meta cache: %v", listErr)
		return fuse.EIO
	}
	return fuse.OK
}
