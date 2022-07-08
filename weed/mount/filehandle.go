package mount

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"golang.org/x/exp/slices"
	"sync"
)

type FileHandleId uint64

type FileHandle struct {
	fh        FileHandleId
	counter   int64
	entry     *filer_pb.Entry
	entryLock sync.Mutex
	inode     uint64
	wfs       *WFS

	// cache file has been written to
	dirtyMetadata  bool
	dirtyPages     *PageWriter
	entryViewCache []filer.VisibleInterval
	reader         *filer.ChunkReadAt
	contentType    string
	handle         uint64
	sync.Mutex

	isDeleted bool
}

func newFileHandle(wfs *WFS, handleId FileHandleId, inode uint64, entry *filer_pb.Entry) *FileHandle {
	fh := &FileHandle{
		fh:      handleId,
		counter: 1,
		inode:   inode,
		wfs:     wfs,
	}
	// dirtyPages: newContinuousDirtyPages(file, writeOnly),
	fh.dirtyPages = newPageWriter(fh, wfs.option.ChunkSizeLimit)
	if entry != nil {
		entry.Attributes.FileSize = filer.FileSize(entry)
	}

	return fh
}

func (fh *FileHandle) FullPath() util.FullPath {
	fp, _ := fh.wfs.inodeToPath.GetPath(fh.inode)
	return fp
}

func (fh *FileHandle) GetEntry() *filer_pb.Entry {
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()
	return fh.entry
}
func (fh *FileHandle) SetEntry(entry *filer_pb.Entry) {
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()
	fh.entry = entry
}

func (fh *FileHandle) AddChunks(chunks []*filer_pb.FileChunk) {
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()

	// find the earliest incoming chunk
	newChunks := chunks
	earliestChunk := newChunks[0]
	for i := 1; i < len(newChunks); i++ {
		if lessThan(earliestChunk, newChunks[i]) {
			earliestChunk = newChunks[i]
		}
	}

	if fh.entry == nil {
		return
	}

	// pick out-of-order chunks from existing chunks
	for _, chunk := range fh.entry.Chunks {
		if lessThan(earliestChunk, chunk) {
			chunks = append(chunks, chunk)
		}
	}

	// sort incoming chunks
	slices.SortFunc(chunks, func(a, b *filer_pb.FileChunk) bool {
		return lessThan(a, b)
	})

	glog.V(4).Infof("%s existing %d chunks adds %d more", fh.FullPath(), len(fh.entry.Chunks), len(chunks))

	fh.entry.Chunks = append(fh.entry.Chunks, newChunks...)
	fh.entryViewCache = nil
}

func (fh *FileHandle) CloseReader() {
	if fh.reader != nil {
		fh.reader.Close()
	}
}

func (fh *FileHandle) Release() {
	fh.dirtyPages.Destroy()
	fh.CloseReader()
}

func lessThan(a, b *filer_pb.FileChunk) bool {
	if a.Mtime == b.Mtime {
		return a.Fid.FileKey < b.Fid.FileKey
	}
	return a.Mtime < b.Mtime
}
