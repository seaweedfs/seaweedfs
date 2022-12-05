package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/semaphore"
	"math"
)

type FileHandleId uint64

type FileHandle struct {
	fh      FileHandleId
	counter int64
	entry   *LockedEntry
	inode   uint64
	wfs     *WFS

	// cache file has been written to
	dirtyMetadata  bool
	dirtyPages     *PageWriter
	entryViewCache []filer.VisibleInterval
	reader         *filer.ChunkReadAt
	contentType    string
	handle         uint64
	orderedMutex   *semaphore.Weighted

	isDeleted bool
}

func newFileHandle(wfs *WFS, handleId FileHandleId, inode uint64, entry *filer_pb.Entry) *FileHandle {
	fh := &FileHandle{
		fh:           handleId,
		counter:      1,
		inode:        inode,
		wfs:          wfs,
		orderedMutex: semaphore.NewWeighted(int64(math.MaxInt64)),
	}
	// dirtyPages: newContinuousDirtyPages(file, writeOnly),
	fh.dirtyPages = newPageWriter(fh, wfs.option.ChunkSizeLimit)
	if entry != nil {
		entry.Attributes.FileSize = filer.FileSize(entry)
	}
	fh.entry = &LockedEntry{
		Entry: entry,
	}

	return fh
}

func (fh *FileHandle) FullPath() util.FullPath {
	fp, _ := fh.wfs.inodeToPath.GetPath(fh.inode)
	return fp
}

func (fh *FileHandle) GetEntry() *filer_pb.Entry {
	return fh.entry.GetEntry()
}

func (fh *FileHandle) SetEntry(entry *filer_pb.Entry) {
	fh.entry.SetEntry(entry)
}

func (fh *FileHandle) UpdateEntry(fn func(entry *filer_pb.Entry)) *filer_pb.Entry {
	return fh.entry.UpdateEntry(fn)
}

func (fh *FileHandle) AddChunks(chunks []*filer_pb.FileChunk) {

	if fh.entry == nil {
		return
	}

	// find the earliest incoming chunk
	newChunks := chunks
	earliestChunk := newChunks[0]
	for i := 1; i < len(newChunks); i++ {
		if lessThan(earliestChunk, newChunks[i]) {
			earliestChunk = newChunks[i]
		}
	}

	// pick out-of-order chunks from existing chunks
	for _, chunk := range fh.entry.GetChunks() {
		if lessThan(earliestChunk, chunk) {
			chunks = append(chunks, chunk)
		}
	}

	// sort incoming chunks
	slices.SortFunc(chunks, func(a, b *filer_pb.FileChunk) bool {
		return lessThan(a, b)
	})

	glog.V(4).Infof("%s existing %d chunks adds %d more", fh.FullPath(), len(fh.entry.GetChunks()), len(chunks))

	fh.entry.AppendChunks(newChunks)
	fh.entryViewCache = nil
}

func (fh *FileHandle) CloseReader() {
	if fh.reader != nil {
		_ = fh.reader.Close()
		fh.reader = nil
	}
}

func (fh *FileHandle) Release() {
	fh.dirtyPages.Destroy()
	fh.CloseReader()
}

func lessThan(a, b *filer_pb.FileChunk) bool {
	if a.ModifiedTsNs == b.ModifiedTsNs {
		return a.Fid.FileKey < b.Fid.FileKey
	}
	return a.ModifiedTsNs < b.ModifiedTsNs
}
