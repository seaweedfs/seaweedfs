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

type MemoryChunkPages struct {
	f              *File
	writeWaitGroup sync.WaitGroup
	chunkAddLock   sync.Mutex
	lastErr        error
	collection     string
	replication    string
	uploadPipeline *page_writer.UploadPipeline
}

func newMemoryChunkPages(file *File, chunkSize int64) *MemoryChunkPages {

	dirtyPages := &MemoryChunkPages{
		f: file,
	}

	dirtyPages.uploadPipeline = page_writer.NewUploadPipeline(
		file.wfs.concurrentWriters, chunkSize, dirtyPages.saveChunkedFileIntevalToStorage)

	return dirtyPages
}

func (pages *MemoryChunkPages) AddPage(offset int64, data []byte) {

	glog.V(4).Infof("%v memory AddPage [%d, %d)", pages.f.fullpath(), offset, offset+int64(len(data)))
	pages.uploadPipeline.SaveDataAt(data, offset)

	return
}

func (pages *MemoryChunkPages) FlushData() error {
	pages.saveChunkedFileToStorage()
	pages.writeWaitGroup.Wait()
	if pages.lastErr != nil {
		return fmt.Errorf("flush data: %v", pages.lastErr)
	}
	return nil
}

func (pages *MemoryChunkPages) ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64) {
	return pages.uploadPipeline.MaybeReadDataAt(data, startOffset)
}

func (pages *MemoryChunkPages) GetStorageOptions() (collection, replication string) {
	return pages.collection, pages.replication
}

func (pages *MemoryChunkPages) saveChunkedFileToStorage() {

	pages.uploadPipeline.FlushAll()

}

func (pages *MemoryChunkPages) saveChunkedFileIntevalToStorage(reader io.Reader, offset int64, size int64, cleanupFn func()) {

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

func (pages MemoryChunkPages) Destroy() {
	pages.uploadPipeline.Shutdown()
}
