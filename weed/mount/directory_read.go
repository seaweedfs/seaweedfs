package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filesys/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"math"
	"os"
	"syscall"
)

var _ = fs.NodeReaddirer(&Directory{})
var _ = fs.NodeGetattrer(&Directory{})

func (dir *Directory) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	return 0
}

func (dir *Directory) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {

	dirPath := util.FullPath(dir.FullPath())
	glog.V(4).Infof("Readdir %s", dirPath)

	sourceChan := make(chan fuse.DirEntry, 64)

	stream := newDirectoryListStream(sourceChan)

	processEachEntryFn := func(entry *filer.Entry, isLast bool) {
		sourceChan <- fuse.DirEntry{
			Mode: uint32(entry.Mode),
			Name: entry.Name(),
			Ino:  dirPath.Child(entry.Name()).AsInode(os.ModeDir),
		}
	}

	if err := meta_cache.EnsureVisited(dir.wfs.metaCache, dir.wfs, dirPath); err != nil {
		glog.Errorf("dir ReadDirAll %s: %v", dirPath, err)
		return nil, fs.ToErrno(os.ErrInvalid)
	}
	go func() {
		dir.wfs.metaCache.ListDirectoryEntries(context.Background(), dirPath, "", false, int64(math.MaxInt32), func(entry *filer.Entry) bool {
			processEachEntryFn(entry, false)
			return true
		})
		close(sourceChan)
	}()

	return stream, fs.OK
}

var _ = fs.DirStream(&DirectoryListStream{})

type DirectoryListStream struct {
	next       fuse.DirEntry
	sourceChan chan fuse.DirEntry
	isStarted  bool
	hasNext    bool
}

func newDirectoryListStream(ch chan fuse.DirEntry) *DirectoryListStream {
	return &DirectoryListStream{
		sourceChan: ch,
	}
}

func (i *DirectoryListStream) HasNext() bool {
	if !i.isStarted {
		i.next, i.hasNext = <-i.sourceChan
		i.isStarted = true
	}
	return i.hasNext
}
func (i *DirectoryListStream) Next() (fuse.DirEntry, syscall.Errno) {
	t := i.next
	i.next, i.hasNext = <-i.sourceChan
	return t, fs.OK
}
func (i *DirectoryListStream) Close() {
}
