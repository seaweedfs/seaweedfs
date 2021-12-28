package filesys

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filesys/page_writer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"
	"sync"
	"time"
)

type StreamDirtyPages struct {
	f              *File
	writeWaitGroup sync.WaitGroup
	pageAddLock    sync.Mutex
	chunkAddLock   sync.Mutex
	lastErr        error
	collection     string
	replication    string
	chunkedStream  *page_writer.ChunkedStreamWriter
}

func newStreamDirtyPages(file *File, chunkSize int64) *StreamDirtyPages {

	dirtyPages := &StreamDirtyPages{
		f:             file,
		chunkedStream: page_writer.NewChunkedStreamWriter(chunkSize),
	}

	dirtyPages.chunkedStream.SetSaveToStorageFunction(dirtyPages.saveChunkedFileIntevalToStorage)

	return dirtyPages
}

func (pages *StreamDirtyPages) AddPage(offset int64, data []byte) {

	pages.pageAddLock.Lock()
	defer pages.pageAddLock.Unlock()

	glog.V(4).Infof("%v stream AddPage [%d, %d)", pages.f.fullpath(), offset, offset+int64(len(data)))
	if _, err := pages.chunkedStream.WriteAt(data, offset); err != nil {
		pages.lastErr = err
	}

	return
}

func (pages *StreamDirtyPages) FlushData() error {
	pages.saveChunkedFileToStorage()
	pages.writeWaitGroup.Wait()
	if pages.lastErr != nil {
		return fmt.Errorf("flush data: %v", pages.lastErr)
	}
	pages.chunkedStream.Reset()
	return nil
}

func (pages *StreamDirtyPages) ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64) {
	return pages.chunkedStream.ReadDataAt(data, startOffset)
}

func (pages *StreamDirtyPages) GetStorageOptions() (collection, replication string) {
	return pages.collection, pages.replication
}

func (pages *StreamDirtyPages) saveChunkedFileToStorage() {

	pages.chunkedStream.FlushAll()

}

func (pages *StreamDirtyPages) saveChunkedFileIntevalToStorage(reader io.Reader, offset int64, size int64, cleanupFn func()) {

	mtime := time.Now().UnixNano()
	pages.writeWaitGroup.Add(1)
	writer := func() {
		defer pages.writeWaitGroup.Done()
		defer cleanupFn()

		chunk, collection, replication, err := pages.f.wfs.saveDataAsChunk(pages.f.fullpath())(reader, pages.f.Name, offset)
		if err != nil {
			glog.V(0).Infof("%s saveToStorage [%d,%d): %v", pages.f.fullpath(), offset, offset+size, err)
			pages.lastErr = err
			return
		}
		chunk.Mtime = mtime
		pages.collection, pages.replication = collection, replication
		pages.chunkAddLock.Lock()
		pages.f.addChunks([]*filer_pb.FileChunk{chunk})
		glog.V(3).Infof("%s saveToStorage %s [%d,%d)", pages.f.fullpath(), chunk.FileId, offset, offset+size)
		pages.chunkAddLock.Unlock()

	}

	if pages.f.wfs.concurrentWriters != nil {
		pages.f.wfs.concurrentWriters.Execute(writer)
	} else {
		go writer()
	}

}

func (pages StreamDirtyPages) Destroy() {
	pages.chunkedStream.Reset()
}
