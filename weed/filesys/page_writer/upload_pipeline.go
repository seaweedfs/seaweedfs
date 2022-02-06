package page_writer

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"sync"
	"sync/atomic"
	"time"
)

type LogicChunkIndex int

type UploadPipeline struct {
	filepath             util.FullPath
	ChunkSize            int64
	writableChunks       map[LogicChunkIndex]PageChunk
	writableChunksLock   sync.Mutex
	sealedChunks         map[LogicChunkIndex]*SealedChunk
	sealedChunksLock     sync.Mutex
	uploaders            *util.LimitedConcurrentExecutor
	uploaderCount        int32
	uploaderCountCond    *sync.Cond
	saveToStorageFn      SaveToStorageFunc
	activeReadChunks     map[LogicChunkIndex]int
	activeReadChunksLock sync.Mutex
	bufferChunkLimit     int
}

type SealedChunk struct {
	chunk            PageChunk
	referenceCounter int // track uploading or reading processes
}

func (sc *SealedChunk) FreeReference(messageOnFree string) {
	sc.referenceCounter--
	if sc.referenceCounter == 0 {
		glog.V(4).Infof("Free sealed chunk: %s", messageOnFree)
		sc.chunk.FreeResource()
	}
}

func NewUploadPipeline(writers *util.LimitedConcurrentExecutor, chunkSize int64, saveToStorageFn SaveToStorageFunc, bufferChunkLimit int) *UploadPipeline {
	return &UploadPipeline{
		ChunkSize:         chunkSize,
		writableChunks:    make(map[LogicChunkIndex]PageChunk),
		sealedChunks:      make(map[LogicChunkIndex]*SealedChunk),
		uploaders:         writers,
		uploaderCountCond: sync.NewCond(&sync.Mutex{}),
		saveToStorageFn:   saveToStorageFn,
		activeReadChunks:  make(map[LogicChunkIndex]int),
		bufferChunkLimit:  bufferChunkLimit,
	}
}

func (up *UploadPipeline) SaveDataAt(p []byte, off int64) (n int) {
	up.writableChunksLock.Lock()
	defer up.writableChunksLock.Unlock()

	logicChunkIndex := LogicChunkIndex(off / up.ChunkSize)

	memChunk, found := up.writableChunks[logicChunkIndex]
	if !found {
		if len(up.writableChunks) < up.bufferChunkLimit {
			memChunk = NewMemChunk(logicChunkIndex, up.ChunkSize)
		} else {
			fullestChunkIndex, fullness := LogicChunkIndex(-1), int64(0)
			for lci, mc := range up.writableChunks {
				chunkFullness := mc.WrittenSize()
				if fullness < chunkFullness {
					fullestChunkIndex = lci
					fullness = chunkFullness
				}
			}
			up.moveToSealed(up.writableChunks[fullestChunkIndex], fullestChunkIndex)
			delete(up.writableChunks, fullestChunkIndex)
			fmt.Printf("flush chunk %d with %d bytes written", logicChunkIndex, fullness)
			memChunk = NewMemChunk(logicChunkIndex, up.ChunkSize)
		}
		up.writableChunks[logicChunkIndex] = memChunk
	}
	n = memChunk.WriteDataAt(p, off)
	up.maybeMoveToSealed(memChunk, logicChunkIndex)

	return
}

func (up *UploadPipeline) MaybeReadDataAt(p []byte, off int64) (maxStop int64) {
	logicChunkIndex := LogicChunkIndex(off / up.ChunkSize)

	// read from sealed chunks first
	up.sealedChunksLock.Lock()
	sealedChunk, found := up.sealedChunks[logicChunkIndex]
	if found {
		sealedChunk.referenceCounter++
	}
	up.sealedChunksLock.Unlock()
	if found {
		maxStop = sealedChunk.chunk.ReadDataAt(p, off)
		glog.V(4).Infof("%s read sealed memchunk [%d,%d)", up.filepath, off, maxStop)
		sealedChunk.FreeReference(fmt.Sprintf("%s finish reading chunk %d", up.filepath, logicChunkIndex))
	}

	// read from writable chunks last
	up.writableChunksLock.Lock()
	defer up.writableChunksLock.Unlock()
	writableChunk, found := up.writableChunks[logicChunkIndex]
	if !found {
		return
	}
	writableMaxStop := writableChunk.ReadDataAt(p, off)
	glog.V(4).Infof("%s read writable memchunk [%d,%d)", up.filepath, off, writableMaxStop)
	maxStop = max(maxStop, writableMaxStop)

	return
}

func (up *UploadPipeline) FlushAll() {
	up.writableChunksLock.Lock()
	defer up.writableChunksLock.Unlock()

	for logicChunkIndex, memChunk := range up.writableChunks {
		up.moveToSealed(memChunk, logicChunkIndex)
	}

	up.waitForCurrentWritersToComplete()
}

func (up *UploadPipeline) maybeMoveToSealed(memChunk PageChunk, logicChunkIndex LogicChunkIndex) {
	if memChunk.IsComplete() {
		up.moveToSealed(memChunk, logicChunkIndex)
	}
}

func (up *UploadPipeline) moveToSealed(memChunk PageChunk, logicChunkIndex LogicChunkIndex) {
	atomic.AddInt32(&up.uploaderCount, 1)
	glog.V(4).Infof("%s uploaderCount %d ++> %d", up.filepath, up.uploaderCount-1, up.uploaderCount)

	up.sealedChunksLock.Lock()

	if oldMemChunk, found := up.sealedChunks[logicChunkIndex]; found {
		oldMemChunk.FreeReference(fmt.Sprintf("%s replace chunk %d", up.filepath, logicChunkIndex))
	}
	sealedChunk := &SealedChunk{
		chunk:            memChunk,
		referenceCounter: 1, // default 1 is for uploading process
	}
	up.sealedChunks[logicChunkIndex] = sealedChunk
	delete(up.writableChunks, logicChunkIndex)

	up.sealedChunksLock.Unlock()

	up.uploaders.Execute(func() {
		// first add to the file chunks
		sealedChunk.chunk.SaveContent(up.saveToStorageFn)

		// notify waiting process
		atomic.AddInt32(&up.uploaderCount, -1)
		glog.V(4).Infof("%s uploaderCount %d --> %d", up.filepath, up.uploaderCount+1, up.uploaderCount)
		// Lock and Unlock are not required,
		// but it may signal multiple times during one wakeup,
		// and the waiting goroutine may miss some of them!
		up.uploaderCountCond.L.Lock()
		up.uploaderCountCond.Broadcast()
		up.uploaderCountCond.L.Unlock()

		// wait for readers
		for up.IsLocked(logicChunkIndex) {
			time.Sleep(59 * time.Millisecond)
		}

		// then remove from sealed chunks
		up.sealedChunksLock.Lock()
		defer up.sealedChunksLock.Unlock()
		delete(up.sealedChunks, logicChunkIndex)
		sealedChunk.FreeReference(fmt.Sprintf("%s finished uploading chunk %d", up.filepath, logicChunkIndex))

	})
}

func (up *UploadPipeline) Shutdown() {
}
