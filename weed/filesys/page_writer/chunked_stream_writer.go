package page_writer

import (
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"io"
	"sync"
	"sync/atomic"
)

type SaveToStorageFunc func(reader io.Reader, offset int64, size int64, cleanupFn func())

// ChunkedStreamWriter assumes the write requests will come in within chunks and in streaming mode
type ChunkedStreamWriter struct {
	activeChunks     map[LogicChunkIndex]*MemChunk
	activeChunksLock sync.Mutex
	ChunkSize        int64
	saveToStorageFn  SaveToStorageFunc
	sync.Mutex
}

type MemChunk struct {
	buf   []byte
	usage *ChunkWrittenIntervalList
}

var _ = io.WriterAt(&ChunkedStreamWriter{})

func NewChunkedStreamWriter(chunkSize int64) *ChunkedStreamWriter {
	return &ChunkedStreamWriter{
		ChunkSize:    chunkSize,
		activeChunks: make(map[LogicChunkIndex]*MemChunk),
	}
}

func (cw *ChunkedStreamWriter) SetSaveToStorageFunction(saveToStorageFn SaveToStorageFunc) {
	cw.saveToStorageFn = saveToStorageFn
}

func (cw *ChunkedStreamWriter) WriteAt(p []byte, off int64) (n int, err error) {
	cw.Lock()
	defer cw.Unlock()

	logicChunkIndex := LogicChunkIndex(off / cw.ChunkSize)
	offsetRemainder := off % cw.ChunkSize

	memChunk, found := cw.activeChunks[logicChunkIndex]
	if !found {
		memChunk = &MemChunk{
			buf:   mem.Allocate(int(cw.ChunkSize)),
			usage: newChunkWrittenIntervalList(),
		}
		cw.activeChunks[logicChunkIndex] = memChunk
	}
	n = copy(memChunk.buf[offsetRemainder:], p)
	memChunk.usage.MarkWritten(offsetRemainder, offsetRemainder+int64(n))
	if memChunk.usage.IsComplete(cw.ChunkSize) {
		if cw.saveToStorageFn != nil {
			cw.saveOneChunk(memChunk, logicChunkIndex)
			delete(cw.activeChunks, logicChunkIndex)
		}
	}

	return
}

func (cw *ChunkedStreamWriter) ReadDataAt(p []byte, off int64) (maxStop int64) {
	cw.Lock()
	defer cw.Unlock()

	logicChunkIndex := LogicChunkIndex(off / cw.ChunkSize)
	memChunkBaseOffset := int64(logicChunkIndex) * cw.ChunkSize
	memChunk, found := cw.activeChunks[logicChunkIndex]
	if !found {
		return
	}

	for t := memChunk.usage.head.next; t != memChunk.usage.tail; t = t.next {
		logicStart := max(off, int64(logicChunkIndex)*cw.ChunkSize+t.StartOffset)
		logicStop := min(off+int64(len(p)), memChunkBaseOffset+t.stopOffset)
		if logicStart < logicStop {
			copy(p[logicStart-off:logicStop-off], memChunk.buf[logicStart-memChunkBaseOffset:logicStop-memChunkBaseOffset])
			maxStop = max(maxStop, logicStop)
		}
	}
	return
}

func (cw *ChunkedStreamWriter) FlushAll() {
	cw.Lock()
	defer cw.Unlock()
	for logicChunkIndex, memChunk := range cw.activeChunks {
		if cw.saveToStorageFn != nil {
			cw.saveOneChunk(memChunk, logicChunkIndex)
			delete(cw.activeChunks, logicChunkIndex)
		}
	}
}

func (cw *ChunkedStreamWriter) saveOneChunk(memChunk *MemChunk, logicChunkIndex LogicChunkIndex) {
	var referenceCounter = int32(memChunk.usage.size())
	for t := memChunk.usage.head.next; t != memChunk.usage.tail; t = t.next {
		reader := util.NewBytesReader(memChunk.buf[t.StartOffset:t.stopOffset])
		cw.saveToStorageFn(reader, int64(logicChunkIndex)*cw.ChunkSize+t.StartOffset, t.Size(), func() {
			atomic.AddInt32(&referenceCounter, -1)
			if atomic.LoadInt32(&referenceCounter) == 0 {
				mem.Free(memChunk.buf)
			}
		})
	}
}

// Reset releases used resources
func (cw *ChunkedStreamWriter) Reset() {
	for t, memChunk := range cw.activeChunks {
		mem.Free(memChunk.buf)
		delete(cw.activeChunks, t)
	}
}
