package page_writer

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"sync"
	"sync/atomic"
)

type UploadPipeline struct {
	writableChunks     map[LogicChunkIndex]*MemChunk
	writableChunksLock sync.Mutex
	sealedChunks       map[LogicChunkIndex]*SealedChunk
	sealedChunksLock   sync.Mutex
	ChunkSize          int64
	writers            *util.LimitedConcurrentExecutor
	activeWriterCond   *sync.Cond
	activeWriterCount  int32
	saveToStorageFn    SaveToStorageFunc
}

type SealedChunk struct {
	chunk            *MemChunk
	referenceCounter int // track uploading or reading processes
}

func (sc *SealedChunk) FreeReference() {
	sc.referenceCounter--
	if sc.referenceCounter == 0 {
		mem.Free(sc.chunk.buf)
	}
}

func NewUploadPipeline(writers *util.LimitedConcurrentExecutor, chunkSize int64, saveToStorageFn SaveToStorageFunc) *UploadPipeline {
	return &UploadPipeline{
		ChunkSize:        chunkSize,
		writableChunks:   make(map[LogicChunkIndex]*MemChunk),
		sealedChunks:     make(map[LogicChunkIndex]*SealedChunk),
		writers:          writers,
		activeWriterCond: sync.NewCond(&sync.Mutex{}),
		saveToStorageFn:  saveToStorageFn,
	}
}

func (cw *UploadPipeline) SaveDataAt(p []byte, off int64) (n int) {
	cw.writableChunksLock.Lock()
	defer cw.writableChunksLock.Unlock()

	logicChunkIndex := LogicChunkIndex(off / cw.ChunkSize)
	offsetRemainder := off % cw.ChunkSize

	memChunk, found := cw.writableChunks[logicChunkIndex]
	if !found {
		memChunk = &MemChunk{
			buf:   mem.Allocate(int(cw.ChunkSize)),
			usage: newChunkWrittenIntervalList(),
		}
		cw.writableChunks[logicChunkIndex] = memChunk
	}
	n = copy(memChunk.buf[offsetRemainder:], p)
	memChunk.usage.MarkWritten(offsetRemainder, offsetRemainder+int64(n))
	cw.maybeMoveToSealed(memChunk, logicChunkIndex)

	return
}

func (cw *UploadPipeline) MaybeReadDataAt(p []byte, off int64) (maxStop int64) {
	logicChunkIndex := LogicChunkIndex(off / cw.ChunkSize)

	// read from sealed chunks first
	cw.sealedChunksLock.Lock()
	sealedChunk, found := cw.sealedChunks[logicChunkIndex]
	if found {
		sealedChunk.referenceCounter++
	}
	cw.sealedChunksLock.Unlock()
	if found {
		maxStop = readMemChunk(sealedChunk.chunk, p, off, logicChunkIndex, cw.ChunkSize)
		sealedChunk.FreeReference()
	}

	// read from writable chunks last
	cw.writableChunksLock.Lock()
	defer cw.writableChunksLock.Unlock()
	writableChunk, found := cw.writableChunks[logicChunkIndex]
	if !found {
		return
	}
	maxStop = max(maxStop, readMemChunk(writableChunk, p, off, logicChunkIndex, cw.ChunkSize))

	return
}

func (cw *UploadPipeline) FlushAll() {
	cw.writableChunksLock.Lock()
	defer cw.writableChunksLock.Unlock()

	for logicChunkIndex, memChunk := range cw.writableChunks {
		cw.moveToSealed(memChunk, logicChunkIndex)
	}

	cw.waitForCurrentWritersToComplete()
}

func (cw *UploadPipeline) waitForCurrentWritersToComplete() {
	cw.activeWriterCond.L.Lock()
	t := int32(100)
	for {
		t = atomic.LoadInt32(&cw.activeWriterCount)
		if t <= 0 {
			break
		}
		glog.V(4).Infof("activeWriterCond is %d", t)
		cw.activeWriterCond.Wait()
	}
	cw.activeWriterCond.L.Unlock()
}

func (cw *UploadPipeline) maybeMoveToSealed(memChunk *MemChunk, logicChunkIndex LogicChunkIndex) {
	if memChunk.usage.IsComplete(cw.ChunkSize) {
		cw.moveToSealed(memChunk, logicChunkIndex)
	}
}

func (cw *UploadPipeline) moveToSealed(memChunk *MemChunk, logicChunkIndex LogicChunkIndex) {
	atomic.AddInt32(&cw.activeWriterCount, 1)
	glog.V(4).Infof("activeWriterCount %d ++> %d", cw.activeWriterCount-1, cw.activeWriterCount)

	cw.sealedChunksLock.Lock()

	if oldMemChunk, found := cw.sealedChunks[logicChunkIndex]; found {
		oldMemChunk.FreeReference()
	}
	sealedChunk := &SealedChunk{
		chunk:            memChunk,
		referenceCounter: 1, // default 1 is for uploading process
	}
	cw.sealedChunks[logicChunkIndex] = sealedChunk
	delete(cw.writableChunks, logicChunkIndex)

	cw.sealedChunksLock.Unlock()

	cw.writers.Execute(func() {
		// first add to the file chunks
		cw.saveOneChunk(sealedChunk.chunk, logicChunkIndex)

		// then remove from sealed chunks
		sealedChunk.FreeReference()
		cw.sealedChunksLock.Lock()
		defer cw.sealedChunksLock.Unlock()
		delete(cw.sealedChunks, logicChunkIndex)

		atomic.AddInt32(&cw.activeWriterCount, -1)
		glog.V(4).Infof("activeWriterCount %d --> %d", cw.activeWriterCount+1, cw.activeWriterCount)
		// Lock and Unlock are not required,
		// but it may signal multiple times during one wakeup,
		// and the waiting goroutine may miss some of them!
		cw.activeWriterCond.L.Lock()
		cw.activeWriterCond.Broadcast()
		cw.activeWriterCond.L.Unlock()
	})
}

func (cw *UploadPipeline) saveOneChunk(memChunk *MemChunk, logicChunkIndex LogicChunkIndex) {
	if cw.saveToStorageFn == nil {
		return
	}
	for t := memChunk.usage.head.next; t != memChunk.usage.tail; t = t.next {
		reader := util.NewBytesReader(memChunk.buf[t.StartOffset:t.stopOffset])
		cw.saveToStorageFn(reader, int64(logicChunkIndex)*cw.ChunkSize+t.StartOffset, t.Size(), func() {
		})
	}
}

func readMemChunk(memChunk *MemChunk, p []byte, off int64, logicChunkIndex LogicChunkIndex, chunkSize int64) (maxStop int64) {
	memChunkBaseOffset := int64(logicChunkIndex) * chunkSize
	for t := memChunk.usage.head.next; t != memChunk.usage.tail; t = t.next {
		logicStart := max(off, int64(logicChunkIndex)*chunkSize+t.StartOffset)
		logicStop := min(off+int64(len(p)), memChunkBaseOffset+t.stopOffset)
		if logicStart < logicStop {
			copy(p[logicStart-off:logicStop-off], memChunk.buf[logicStart-memChunkBaseOffset:logicStop-memChunkBaseOffset])
			maxStop = max(maxStop, logicStop)
		}
	}
	return
}

func (p2 *UploadPipeline) Shutdown() {

}
