package filesys

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filesys/page_writer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"os"
	"sync"
	"time"
)

type TempFileDirtyPages struct {
	f              *File
	writeWaitGroup sync.WaitGroup
	pageAddLock    sync.Mutex
	chunkAddLock   sync.Mutex
	lastErr        error
	collection     string
	replication    string
	chunkedFile    *page_writer.ChunkedFileWriter
}

func newTempFileDirtyPages(file *File, chunkSize int64) *TempFileDirtyPages {

	tempFile := &TempFileDirtyPages{
		f:           file,
		chunkedFile: page_writer.NewChunkedFileWriter(file.wfs.option.getTempFilePageDir(), chunkSize),
	}

	return tempFile
}

func (pages *TempFileDirtyPages) AddPage(offset int64, data []byte) {

	pages.pageAddLock.Lock()
	defer pages.pageAddLock.Unlock()

	glog.V(4).Infof("%v tempfile AddPage [%d, %d)", pages.f.fullpath(), offset, offset+int64(len(data)))
	if _, err := pages.chunkedFile.WriteAt(data, offset); err != nil {
		pages.lastErr = err
	}

	return
}

func (pages *TempFileDirtyPages) FlushData() error {
	pages.saveChunkedFileToStorage()
	pages.writeWaitGroup.Wait()
	if pages.lastErr != nil {
		return fmt.Errorf("flush data: %v", pages.lastErr)
	}
	pages.chunkedFile.Reset()
	return nil
}

func (pages *TempFileDirtyPages) ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64) {
	return pages.chunkedFile.ReadDataAt(data, startOffset)
}

func (pages *TempFileDirtyPages) GetStorageOptions() (collection, replication string) {
	return pages.collection, pages.replication
}

func (pages *TempFileDirtyPages) saveChunkedFileToStorage() {

	pages.chunkedFile.ProcessEachInterval(func(file *os.File, logicChunkIndex page_writer.LogicChunkIndex, interval *page_writer.ChunkWrittenInterval) {
		reader := page_writer.NewFileIntervalReader(pages.chunkedFile, logicChunkIndex, interval)
		pages.saveChunkedFileIntevalToStorage(reader, int64(logicChunkIndex)*pages.chunkedFile.ChunkSize+interval.StartOffset, interval.Size())
	})

}

func (pages *TempFileDirtyPages) saveChunkedFileIntevalToStorage(reader io.Reader, offset int64, size int64) {

	mtime := time.Now().UnixNano()
	pages.writeWaitGroup.Add(1)
	writer := func() {
		defer pages.writeWaitGroup.Done()

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
		glog.V(3).Infof("%s saveToStorage %s [%d,%d)", pages.f.fullpath(), chunk.FileId, offset, offset+size)
	}

	if pages.f.wfs.concurrentWriters != nil {
		pages.f.wfs.concurrentWriters.Execute(writer)
	} else {
		go writer()
	}

}

func (pages TempFileDirtyPages) Destroy() {
	pages.chunkedFile.Reset()
}
