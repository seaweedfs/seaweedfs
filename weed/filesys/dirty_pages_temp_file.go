package filesys

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type TempFileDirtyPages struct {
	f                *File
	tf               *os.File
	writtenIntervals *WrittenContinuousIntervals
	writeOnly        bool
	writeWaitGroup   sync.WaitGroup
	pageAddLock      sync.Mutex
	chunkAddLock     sync.Mutex
	lastErr          error
	collection       string
	replication      string
}

var (
	tmpDir = filepath.Join(os.TempDir(), "sw")
)

func init() {
	os.Mkdir(tmpDir, 0755)
}

func newTempFileDirtyPages(file *File, writeOnly bool) *TempFileDirtyPages {

	tempFile := &TempFileDirtyPages{
		f:                file,
		writeOnly:        writeOnly,
		writtenIntervals: &WrittenContinuousIntervals{},
	}

	return tempFile
}

func (pages *TempFileDirtyPages) AddPage(offset int64, data []byte) {

	pages.pageAddLock.Lock()
	defer pages.pageAddLock.Unlock()

	if pages.tf == nil {
		tf, err := os.CreateTemp(tmpDir, "")
		if err != nil {
			glog.Errorf("create temp file: %v", err)
			pages.lastErr = err
			return
		}
		pages.tf = tf
		pages.writtenIntervals.tempFile = tf
		pages.writtenIntervals.lastOffset = 0
	}

	writtenOffset := pages.writtenIntervals.lastOffset
	dataSize := int64(len(data))

	// glog.V(4).Infof("%s AddPage %v at %d [%d,%d)", pages.f.fullpath(), pages.tf.Name(), writtenOffset, offset, offset+dataSize)

	if _, err := pages.tf.WriteAt(data, writtenOffset); err != nil {
		pages.lastErr = err
	} else {
		pages.writtenIntervals.AddInterval(writtenOffset, len(data), offset)
		pages.writtenIntervals.lastOffset += dataSize
	}

	// pages.writtenIntervals.debug()

	return
}

func (pages *TempFileDirtyPages) FlushData() error {

	pages.saveExistingPagesToStorage()
	pages.writeWaitGroup.Wait()
	if pages.lastErr != nil {
		return fmt.Errorf("flush data: %v", pages.lastErr)
	}
	pages.pageAddLock.Lock()
	defer pages.pageAddLock.Unlock()
	if pages.tf != nil {

		pages.writtenIntervals.tempFile = nil
		pages.writtenIntervals.lists = nil

		pages.tf.Close()
		os.Remove(pages.tf.Name())
		pages.tf = nil
	}
	return nil
}

func (pages *TempFileDirtyPages) saveExistingPagesToStorage() {

	pageSize := pages.f.wfs.option.ChunkSizeLimit

	// glog.V(4).Infof("%v saveExistingPagesToStorage %d lists", pages.f.Name, len(pages.writtenIntervals.lists))

	for _, list := range pages.writtenIntervals.lists {
		listStopOffset := list.Offset() + list.Size()
		for uploadedOffset:=int64(0); uploadedOffset < listStopOffset; uploadedOffset += pageSize {
			start, stop := max(list.Offset(), uploadedOffset), min(listStopOffset, uploadedOffset+pageSize)
			if start >= stop {
				continue
			}
			// glog.V(4).Infof("uploading %v [%d,%d) %d/%d", pages.f.Name, start, stop, i, len(pages.writtenIntervals.lists))
			pages.saveToStorage(list.ToReader(start, stop), start, stop-start)
		}
	}

}

func (pages *TempFileDirtyPages) saveToStorage(reader io.Reader, offset int64, size int64) {

	mtime := time.Now().UnixNano()
	pages.writeWaitGroup.Add(1)
	writer := func() {
		defer pages.writeWaitGroup.Done()

		reader = io.LimitReader(reader, size)
		chunk, collection, replication, err := pages.f.wfs.saveDataAsChunk(pages.f.fullpath(), pages.writeOnly)(reader, pages.f.Name, offset)
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
		glog.V(3).Infof("%s saveToStorage %s [%d,%d)", pages.f.fullpath(), chunk.FileId, offset, offset+size)
	}

	if pages.f.wfs.concurrentWriters != nil {
		pages.f.wfs.concurrentWriters.Execute(writer)
	} else {
		go writer()
	}
}

func (pages *TempFileDirtyPages) ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64) {
	return pages.writtenIntervals.ReadDataAt(data, startOffset)
}

func (pages *TempFileDirtyPages) GetStorageOptions() (collection, replication string) {
	return pages.collection, pages.replication
}

func (pages *TempFileDirtyPages) SetWriteOnly(writeOnly bool) {
	if pages.writeOnly {
		pages.writeOnly = writeOnly
	}
}

func (pages *TempFileDirtyPages) GetWriteOnly() (writeOnly bool) {
	return pages.writeOnly
}
