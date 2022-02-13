package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filesys/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/hanwen/go-fuse/v2/fuse"
	"math"
	"os"
)

// Directory handling

func (wfs *WFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	if !wfs.inodeToPath.HasInode(input.NodeId) {
		return fuse.ENOENT
	}
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

	println("input size", input.Size, "offset", input.Offset, "pid", input.Caller.Pid)

	var counter uint64
	var dirEntry fuse.DirEntry
	if input.Offset == 0 {
		counter++
		dirEntry.Ino = input.NodeId
		dirEntry.Name = "."
		dirEntry.Mode = toSystemMode(os.ModeDir)
		out.AddDirEntry(dirEntry)

		counter++
		parentDir, _ := dirPath.DirAndName()
		parentInode := wfs.inodeToPath.GetInode(util.FullPath(parentDir))
		dirEntry.Ino = parentInode
		dirEntry.Name = ".."
		dirEntry.Mode = toSystemMode(os.ModeDir)
		out.AddDirEntry(dirEntry)

	}

	processEachEntryFn := func(entry *filer.Entry, isLast bool) bool {
		counter++
		if counter <= input.Offset {
			return true
		}
		dirEntry.Name = entry.Name()
		inode := wfs.inodeToPath.GetInode(dirPath.Child(dirEntry.Name))
		println("entry", dirEntry.Name, "inode", inode)
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
			wfs.outputEntry(entryOut, inode, entry)
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
