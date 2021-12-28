package filesys

import (
	"bytes"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filesys/page_writer"
	"io"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type ContinuousDirtyPages struct {
	intervals      *page_writer.ContinuousIntervals
	f              *File
	writeWaitGroup sync.WaitGroup
	chunkAddLock   sync.Mutex
	lastErr        error
	collection     string
	replication    string
}

func newContinuousDirtyPages(file *File) *ContinuousDirtyPages {
	dirtyPages := &ContinuousDirtyPages{
		intervals: &page_writer.ContinuousIntervals{},
		f:         file,
	}
	return dirtyPages
}

func (pages *ContinuousDirtyPages) AddPage(offset int64, data []byte) {

	glog.V(4).Infof("%s AddPage [%d,%d)", pages.f.fullpath(), offset, offset+int64(len(data)))

	if len(data) > int(pages.f.wfs.option.ChunkSizeLimit) {
		// this is more than what buffer can hold.
		pages.flushAndSave(offset, data)
	}

	pages.intervals.AddInterval(data, offset)

	if pages.intervals.TotalSize() >= pages.f.wfs.option.ChunkSizeLimit {
		pages.saveExistingLargestPageToStorage()
	}

	return
}

func (pages *ContinuousDirtyPages) flushAndSave(offset int64, data []byte) {

	// flush existing
	pages.saveExistingPagesToStorage()

	// flush the new page
	pages.saveToStorage(bytes.NewReader(data), offset, int64(len(data)))

	return
}

func (pages *ContinuousDirtyPages) FlushData() error {

	pages.saveExistingPagesToStorage()
	pages.writeWaitGroup.Wait()
	if pages.lastErr != nil {
		return fmt.Errorf("flush data: %v", pages.lastErr)
	}
	return nil
}

func (pages *ContinuousDirtyPages) saveExistingPagesToStorage() {
	for pages.saveExistingLargestPageToStorage() {
	}
}

func (pages *ContinuousDirtyPages) saveExistingLargestPageToStorage() (hasSavedData bool) {

	maxList := pages.intervals.RemoveLargestIntervalLinkedList()
	if maxList == nil {
		return false
	}

	entry := pages.f.getEntry()
	if entry == nil {
		return false
	}

	fileSize := int64(entry.Attributes.FileSize)

	chunkSize := min(maxList.Size(), fileSize-maxList.Offset())
	if chunkSize == 0 {
		return false
	}

	pages.saveToStorage(maxList.ToReader(), maxList.Offset(), chunkSize)

	return true
}

func (pages *ContinuousDirtyPages) saveToStorage(reader io.Reader, offset int64, size int64) {

	mtime := time.Now().UnixNano()
	pages.writeWaitGroup.Add(1)
	writer := func() {
		defer pages.writeWaitGroup.Done()

		reader = io.LimitReader(reader, size)
		chunk, collection, replication, err := pages.f.wfs.saveDataAsChunk(pages.f.fullpath())(reader, pages.f.Name, offset)
		if err != nil {
			glog.V(0).Infof("%s saveToStorage [%d,%d): %v", pages.f.fullpath(), offset, offset+size, err)
			pages.lastErr = err
			return
		}
		chunk.Mtime = mtime
		pages.collection, pages.replication = collection, replication
		pages.chunkAddLock.Lock()
		defer pages.chunkAddLock.Unlock()
		pages.f.addChunks([]*filer_pb.FileChunk{chunk})
		glog.V(3).Infof("%s saveToStorage [%d,%d)", pages.f.fullpath(), offset, offset+size)
	}

	if pages.f.wfs.concurrentWriters != nil {
		pages.f.wfs.concurrentWriters.Execute(writer)
	} else {
		go writer()
	}
}

func (pages *ContinuousDirtyPages) ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64) {
	return pages.intervals.ReadDataAt(data, startOffset)
}

func (pages *ContinuousDirtyPages) GetStorageOptions() (collection, replication string) {
	return pages.collection, pages.replication
}
func (pages ContinuousDirtyPages) Destroy() {
}
