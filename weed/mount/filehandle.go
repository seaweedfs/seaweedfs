package mount

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
	"sort"
	"sync"
)

type FileHandleId uint64

type FileHandle struct {
	fh           FileHandleId
	counter      int64
	entry        *filer_pb.Entry
	chunkAddLock sync.Mutex
	inode        uint64
	wfs          *WFS

	// cache file has been written to
	dirtyMetadata  bool
	dirtyPages     *PageWriter
	entryViewCache []filer.VisibleInterval
	reader         io.ReaderAt
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
	return fh.wfs.inodeToPath.GetPath(fh.inode)
}

func (fh *FileHandle) addChunks(chunks []*filer_pb.FileChunk) {

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
	sort.Slice(chunks, func(i, j int) bool {
		return lessThan(chunks[i], chunks[j])
	})

	glog.V(4).Infof("%s existing %d chunks adds %d more", fh.FullPath(), len(fh.entry.Chunks), len(chunks))

	fh.chunkAddLock.Lock()
	fh.entry.Chunks = append(fh.entry.Chunks, newChunks...)
	fh.entryViewCache = nil
	fh.chunkAddLock.Unlock()
}

func lessThan(a, b *filer_pb.FileChunk) bool {
	if a.Mtime == b.Mtime {
		return a.Fid.FileKey < b.Fid.FileKey
	}
	return a.Mtime < b.Mtime
}
