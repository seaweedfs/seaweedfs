package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/sync/semaphore"
	"math"
	"os"
	"sync"
)

type FileHandleId uint64

var IsDebug = true

type FileHandle struct {
	fh        FileHandleId
	counter   int64
	entry     *LockedEntry
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
	orderedMutex   *semaphore.Weighted

	isDeleted bool

	// for debugging
	mirrorFile *os.File
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

	if IsDebug {
		var err error
		fh.mirrorFile, err = os.OpenFile("/tmp/sw/"+entry.Name, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			println("failed to create mirror:", err.Error())
		}
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
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()

	if fh.entry == nil {
		return
	}

	fh.entry.AppendChunks(chunks)
	fh.entryViewCache = nil
}

func (fh *FileHandle) CloseReader() {
	if fh.reader != nil {
		_ = fh.reader.Close()
		fh.reader = nil
	}
}

func (fh *FileHandle) Release() {
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()

	glog.V(4).Infof("Release %s fh %d", fh.entry.Name, fh.handle)

	fh.dirtyPages.Destroy()
	fh.CloseReader()
	if IsDebug {
		fh.mirrorFile.Close()
	}
}
