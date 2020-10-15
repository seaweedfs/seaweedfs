package filesys

import (
	"bytes"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"sync"
)

type ContinuousDirtyPages struct {
	intervals        *ContinuousIntervals
	f                *File
	writeWaitGroup   sync.WaitGroup
	chunkSaveErrChan chan error
	lock             sync.Mutex
	collection       string
	replication      string
}

func newDirtyPages(file *File) *ContinuousDirtyPages {
	return &ContinuousDirtyPages{
		intervals:        &ContinuousIntervals{},
		f:                file,
		chunkSaveErrChan: make(chan error, 8),
	}
}

func (pages *ContinuousDirtyPages) AddPage(offset int64, data []byte) {

	glog.V(4).Infof("%s AddPage [%d,%d) of %d bytes", pages.f.fullpath(), offset, offset+int64(len(data)), pages.f.entry.Attributes.FileSize)

	if len(data) > int(pages.f.wfs.option.ChunkSizeLimit) {
		// this is more than what buffer can hold.
		pages.flushAndSave(offset, data)
	}

	pages.intervals.AddInterval(data, offset)

	if pages.intervals.TotalSize() > pages.f.wfs.option.ChunkSizeLimit {
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

func (pages *ContinuousDirtyPages) saveExistingPagesToStorage() {
	for pages.saveExistingLargestPageToStorage() {
	}
}

func (pages *ContinuousDirtyPages) saveExistingLargestPageToStorage() (hasSavedData bool) {

	maxList := pages.intervals.RemoveLargestIntervalLinkedList()
	if maxList == nil {
		return false
	}

	fileSize := int64(pages.f.entry.Attributes.FileSize)

	chunkSize := min(maxList.Size(), fileSize-maxList.Offset())
	if chunkSize == 0 {
		return false
	}

	pages.saveToStorage(maxList.ToReader(), maxList.Offset(), chunkSize)

	return true
}

func (pages *ContinuousDirtyPages) saveToStorage(reader io.Reader, offset int64, size int64) {

	pages.writeWaitGroup.Add(1)
	go func() {
		defer pages.writeWaitGroup.Done()

		dir, _ := pages.f.fullpath().DirAndName()

		reader = io.LimitReader(reader, size)
		chunk, collection, replication, err := pages.f.wfs.saveDataAsChunk(dir)(reader, pages.f.Name, offset)
		if err != nil {
			glog.V(0).Infof("%s saveToStorage [%d,%d): %v", pages.f.fullpath(), offset, offset+size, err)
			pages.chunkSaveErrChan <- err
			return
		}
		pages.collection, pages.replication = collection, replication
		pages.f.addChunks([]*filer_pb.FileChunk{chunk})
		pages.chunkSaveErrChan <- nil
	}()
}

func maxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func (pages *ContinuousDirtyPages) ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64) {
	return pages.intervals.ReadDataAt(data, startOffset)
}
