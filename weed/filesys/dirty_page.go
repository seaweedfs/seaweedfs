package filesys

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type ContinuousDirtyPages struct {
	intervals   *ContinuousIntervals
	f           *File
	lock        sync.Mutex
	collection  string
	replication string
}

func newDirtyPages(file *File) *ContinuousDirtyPages {
	return &ContinuousDirtyPages{
		intervals: &ContinuousIntervals{},
		f:         file,
	}
}

var counter = int32(0)

func (pages *ContinuousDirtyPages) AddPage(offset int64, data []byte) (chunks []*filer_pb.FileChunk, err error) {

	glog.V(5).Infof("%s AddPage [%d,%d) of %d bytes", pages.f.fullpath(), offset, offset+int64(len(data)), pages.f.entry.Attributes.FileSize)

	if len(data) > int(pages.f.wfs.option.ChunkSizeLimit) {
		// this is more than what buffer can hold.
		return pages.flushAndSave(offset, data)
	}

	pages.intervals.AddInterval(data, offset)

	var chunk *filer_pb.FileChunk
	var hasSavedData bool

	if pages.intervals.TotalSize() > pages.f.wfs.option.ChunkSizeLimit {
		chunk, hasSavedData, err = pages.saveExistingLargestPageToStorage()
		if hasSavedData {
			chunks = append(chunks, chunk)
		}
	}

	return
}

func (pages *ContinuousDirtyPages) flushAndSave(offset int64, data []byte) (chunks []*filer_pb.FileChunk, err error) {

	var chunk *filer_pb.FileChunk
	var newChunks []*filer_pb.FileChunk

	// flush existing
	if newChunks, err = pages.saveExistingPagesToStorage(); err == nil {
		if newChunks != nil {
			chunks = append(chunks, newChunks...)
		}
	} else {
		return
	}

	// flush the new page
	if chunk, err = pages.saveToStorage(bytes.NewReader(data), offset, int64(len(data))); err == nil {
		if chunk != nil {
			glog.V(4).Infof("%s/%s flush big request [%d,%d) to %s", pages.f.dir.FullPath(), pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size), chunk.FileId)
			chunks = append(chunks, chunk)
		}
	} else {
		glog.V(0).Infof("%s/%s failed to flush2 [%d,%d): %v", pages.f.dir.FullPath(), pages.f.Name, chunk.Offset, chunk.Offset+int64(chunk.Size), err)
		return
	}

	return
}

func (pages *ContinuousDirtyPages) saveExistingPagesToStorage() (chunks []*filer_pb.FileChunk, err error) {

	var hasSavedData bool
	var chunk *filer_pb.FileChunk

	for {

		chunk, hasSavedData, err = pages.saveExistingLargestPageToStorage()
		if !hasSavedData {
			return chunks, err
		}

		if err == nil {
			if chunk != nil {
				chunks = append(chunks, chunk)
			}
		} else {
			return
		}
	}

}

func (pages *ContinuousDirtyPages) saveExistingLargestPageToStorage() (chunk *filer_pb.FileChunk, hasSavedData bool, err error) {

	maxList := pages.intervals.RemoveLargestIntervalLinkedList()
	if maxList == nil {
		return nil, false, nil
	}

	fileSize := int64(pages.f.entry.Attributes.FileSize)
	for {
		chunkSize := min(maxList.Size(), fileSize-maxList.Offset())
		if chunkSize == 0 {
			return
		}
		chunk, err = pages.saveToStorage(maxList.ToReader(), maxList.Offset(), chunkSize)
		if err == nil {
			if chunk != nil {
				hasSavedData = true
			}
			glog.V(4).Infof("saveToStorage %s %s [%d,%d) of %d bytes", pages.f.fullpath(), chunk.GetFileIdString(), maxList.Offset(), maxList.Offset()+chunkSize, fileSize)
			return
		} else {
			glog.V(0).Infof("%s saveToStorage [%d,%d): %v", pages.f.fullpath(), maxList.Offset(), maxList.Offset()+chunkSize, err)
			time.Sleep(5 * time.Second)
		}
	}

}

func (pages *ContinuousDirtyPages) saveToStorage(reader io.Reader, offset int64, size int64) (*filer_pb.FileChunk, error) {

	dir, _ := pages.f.fullpath().DirAndName()

	reader = io.LimitReader(reader, size)
	chunk, collection, replication, err := pages.f.wfs.saveDataAsChunk(dir)(reader, pages.f.Name, offset)
	if err != nil {
		return nil, err
	}
	pages.collection, pages.replication = collection, replication

	return chunk, nil

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
