package mount

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"math"
	"sync"
)

type DirectoryHandleId uint64

const (
	directoryStreamBaseOffset = 2 // . & ..
)

type DirectoryHandle struct {
	isFinished        bool
	entryStream       []*filer.Entry
	entryStreamOffset uint64
}

func (dh *DirectoryHandle) reset() {
	*dh = DirectoryHandle{
		isFinished:        false,
		entryStream:       []*filer.Entry{},
		entryStreamOffset: directoryStreamBaseOffset,
	}
}

type DirectoryHandleToInode struct {
	// shares the file handle id sequencer with FileHandleToInode{nextFh}
	sync.Mutex
	dir2inode map[DirectoryHandleId]*DirectoryHandle
}

func NewDirectoryHandleToInode() *DirectoryHandleToInode {
	return &DirectoryHandleToInode{
		dir2inode: make(map[DirectoryHandleId]*DirectoryHandle),
	}
}

func (wfs *WFS) AcquireDirectoryHandle() (DirectoryHandleId, *DirectoryHandle) {
	fh := FileHandleId(util.RandomUint64())

	wfs.dhMap.Lock()
	defer wfs.dhMap.Unlock()
	dh := new(DirectoryHandle)
	dh.reset()
	wfs.dhMap.dir2inode[DirectoryHandleId(fh)] = dh
	return DirectoryHandleId(fh), dh
}

func (wfs *WFS) GetDirectoryHandle(dhid DirectoryHandleId) *DirectoryHandle {
	wfs.dhMap.Lock()
	defer wfs.dhMap.Unlock()
	if dh, found := wfs.dhMap.dir2inode[dhid]; found {
		return dh
	}
	dh := new(DirectoryHandle)
	dh.reset()
	wfs.dhMap.dir2inode[dhid] = dh
	return dh
}

func (wfs *WFS) ReleaseDirectoryHandle(dhid DirectoryHandleId) {
	wfs.dhMap.Lock()
	defer wfs.dhMap.Unlock()
	delete(wfs.dhMap.dir2inode, dhid)
}

// Directory handling

/** Open directory
 *
 * Unless the 'default_permissions' mount option is given,
 * this method should check if opendir is permitted for this
 * directory. Optionally opendir may also return an arbitrary
 * filehandle in the fuse_file_info structure, which will be
 * passed to readdir, releasedir and fsyncdir.
 */
func (wfs *WFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	if !wfs.inodeToPath.HasInode(input.NodeId) {
		return fuse.ENOENT
	}
	dhid, _ := wfs.AcquireDirectoryHandle()
	out.Fh = uint64(dhid)
	return fuse.OK
}

/** Release directory
 *
 * If the directory has been removed after the call to opendir, the
 * path parameter will be NULL.
 */
func (wfs *WFS) ReleaseDir(input *fuse.ReleaseIn) {
	wfs.ReleaseDirectoryHandle(DirectoryHandleId(input.Fh))
}

/** Synchronize directory contents
 *
 * If the directory has been removed after the call to opendir, the
 * path parameter will be NULL.
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 */
func (wfs *WFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

/** Read directory
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 */
func (wfs *WFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	return wfs.doReadDirectory(input, out, false)
}

func (wfs *WFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	return wfs.doReadDirectory(input, out, true)
}

func (wfs *WFS) doReadDirectory(input *fuse.ReadIn, out *fuse.DirEntryList, isPlusMode bool) fuse.Status {
	dh := wfs.GetDirectoryHandle(DirectoryHandleId(input.Fh))
	if input.Offset == 0 {
		dh.reset()
	} else if dh.isFinished && input.Offset >= dh.entryStreamOffset {
		entryCurrentIndex := input.Offset - dh.entryStreamOffset
		if uint64(len(dh.entryStream)) <= entryCurrentIndex {
			return fuse.OK
		}
	}

	isEarlyTerminated := false
	dirPath, code := wfs.inodeToPath.GetPath(input.NodeId)
	if code != fuse.OK {
		return code
	}

	var dirEntry fuse.DirEntry
	processEachEntryFn := func(entry *filer.Entry) bool {
		dirEntry.Name = entry.Name()
		dirEntry.Mode = toSyscallMode(entry.Mode)
		inode := wfs.inodeToPath.Lookup(dirPath.Child(dirEntry.Name), entry.Crtime.Unix(), entry.IsDirectory(), len(entry.HardLinkId) > 0, entry.Inode, isPlusMode)
		dirEntry.Ino = inode
		if !isPlusMode {
			if !out.AddDirEntry(dirEntry) {
				isEarlyTerminated = true
				return false
			}
		} else {
			entryOut := out.AddDirLookupEntry(dirEntry)
			if entryOut == nil {
				isEarlyTerminated = true
				return false
			}
			if fh, found := wfs.fhMap.FindFileHandle(inode); found {
				glog.V(4).Infof("readdir opened file %s", dirPath.Child(dirEntry.Name))
				entry = filer.FromPbEntry(string(dirPath), fh.GetEntry().GetEntry())
			}
			wfs.outputFilerEntry(entryOut, inode, entry)
		}
		return true
	}

	if input.Offset < directoryStreamBaseOffset {
		if !isPlusMode {
			if input.Offset == 0 {
				out.AddDirEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "."})
			}
			out.AddDirEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".."})
		} else {
			if input.Offset == 0 {
				out.AddDirLookupEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "."})
			}
			out.AddDirLookupEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".."})
		}
		input.Offset = directoryStreamBaseOffset
	}

	var lastEntryName string
	if input.Offset >= dh.entryStreamOffset {
		if input.Offset > dh.entryStreamOffset {
			entryPreviousIndex := (input.Offset - dh.entryStreamOffset) - 1
			if uint64(len(dh.entryStream)) > entryPreviousIndex {
				lastEntryName = dh.entryStream[entryPreviousIndex].Name()
				dh.entryStream = dh.entryStream[entryPreviousIndex:]
				dh.entryStreamOffset = input.Offset - 1
			}
		}
		entryCurrentIndex := input.Offset - dh.entryStreamOffset
		for uint64(len(dh.entryStream)) > entryCurrentIndex {
			entry := dh.entryStream[entryCurrentIndex]
			if processEachEntryFn(entry) {
				lastEntryName = entry.Name()
				entryCurrentIndex++
			} else {
				// early terminated
				return fuse.OK
			}
		}
	}

	var err error
	if err = meta_cache.EnsureVisited(wfs.metaCache, wfs, dirPath); err != nil {
		glog.Errorf("dir ReadDirAll %s: %v", dirPath, err)
		return fuse.EIO
	}
	listErr := wfs.metaCache.ListDirectoryEntries(context.Background(), dirPath, lastEntryName, false, int64(math.MaxInt32), func(entry *filer.Entry) bool {
		dh.entryStream = append(dh.entryStream, entry)
		return processEachEntryFn(entry)
	})
	if listErr != nil {
		glog.Errorf("list meta cache: %v", listErr)
		return fuse.EIO
	}

	if !isEarlyTerminated {
		dh.isFinished = true
	}

	return fuse.OK
}
