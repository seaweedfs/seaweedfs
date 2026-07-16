package mount

import (
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/page_writer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type ChunkedDirtyPages struct {
	fh             *FileHandle
	writeWaitGroup sync.WaitGroup
	lastErrLock    sync.Mutex
	lastErr        error
	collection     string
	replication    string
	uploadPipeline *page_writer.UploadPipeline
	hasWrites      bool
}

var (
	_ = page_writer.DirtyPages(&ChunkedDirtyPages{})
)

func newMemoryChunkPages(fh *FileHandle, chunkSize int64) *ChunkedDirtyPages {

	dirtyPages := &ChunkedDirtyPages{
		fh: fh,
	}

	swapFileDir := fh.wfs.option.getUniqueCacheDirForWrite()

	dirtyPages.uploadPipeline = page_writer.NewUploadPipeline(fh.wfs.concurrentWriters, chunkSize,
		dirtyPages.saveChunkedFileIntervalToStorage, fh.wfs.option.ConcurrentWriters, swapFileDir,
		fh.wfs.writeBufferAccountant)

	return dirtyPages
}

func (pages *ChunkedDirtyPages) AddPage(offset int64, data []byte, isSequential bool, tsNs int64) error {
	// A failed chunk upload already dropped its data, so this handle can never
	// flush cleanly. Reject further writes so the writer fails fast instead of
	// pushing the rest of the file through a pipeline that cannot persist it.
	if err := pages.LastError(); err != nil {
		return err
	}
	pages.hasWrites = true

	glog.V(4).Infof("%v memory AddPage [%d, %d)", pages.fh.fh, offset, offset+int64(len(data)))
	_, err := pages.uploadPipeline.SaveDataAt(data, offset, isSequential, tsNs)

	return err
}

func (pages *ChunkedDirtyPages) FlushData() error {
	if !pages.hasWrites {
		return nil
	}
	pages.uploadPipeline.FlushAll()
	if err := pages.LastError(); err != nil {
		return fmt.Errorf("flush data: %w", err)
	}
	return nil
}

// LastError returns the first chunk upload failure on this handle. It is
// sticky: the failed chunk's data is gone, so the handle stays poisoned.
func (pages *ChunkedDirtyPages) LastError() error {
	pages.lastErrLock.Lock()
	defer pages.lastErrLock.Unlock()
	return pages.lastErr
}

func (pages *ChunkedDirtyPages) setLastError(err error) {
	pages.lastErrLock.Lock()
	defer pages.lastErrLock.Unlock()
	if pages.lastErr == nil {
		pages.lastErr = err
	}
}

func (pages *ChunkedDirtyPages) ReadDirtyDataAt(data []byte, startOffset int64, tsNs int64) (maxStop int64) {
	if !pages.hasWrites {
		return
	}
	return pages.uploadPipeline.MaybeReadDataAt(data, startOffset, tsNs)
}

func (pages *ChunkedDirtyPages) saveChunkedFileIntervalToStorage(reader io.Reader, offset int64, size int64, modifiedTsNs int64, cleanupFn func()) {

	defer cleanupFn()

	fileFullPath := pages.fh.FullPath()
	fileName := fileFullPath.Name()
	chunk, err := pages.fh.wfs.saveDataAsChunk(fileFullPath)(reader, fileName, offset, modifiedTsNs, uint64(size))
	if err != nil {
		glog.V(0).Infof("%v saveToStorage [%d,%d): %v", fileFullPath, offset, offset+size, err)
		pages.setLastError(err)
		return
	}
	pages.fh.AddChunks([]*filer_pb.FileChunk{chunk})
	pages.fh.entryChunkGroup.AddChunk(chunk)
	glog.V(3).Infof("%v saveToStorage %s [%d,%d)", fileFullPath, chunk.FileId, offset, offset+size)

}

func (pages *ChunkedDirtyPages) Destroy() {
	pages.uploadPipeline.Shutdown()
}

func (pages *ChunkedDirtyPages) EvictOneWritableChunk() bool {
	return pages.uploadPipeline.EvictOneWritableChunk()
}

func (pages *ChunkedDirtyPages) ProactiveFlush(nowNs, idleThresholdNs, maxHoldNs, fillRatio int64, frontierLag int, isSequential bool) bool {
	return pages.uploadPipeline.ProactiveFlush(nowNs, idleThresholdNs, maxHoldNs, fillRatio, frontierLag, isSequential)
}

func (pages *ChunkedDirtyPages) LockForRead(startOffset, stopOffset int64) {
	pages.uploadPipeline.LockForRead(startOffset, stopOffset)
}
func (pages *ChunkedDirtyPages) UnlockForRead(startOffset, stopOffset int64) {
	pages.uploadPipeline.UnlockForRead(startOffset, stopOffset)
}
