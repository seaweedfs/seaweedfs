package page_writer

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"sync"
	"sync/atomic"
)

type LogicChunkIndex int

type UploadPipeline struct {
	uploaderCount      int32
	uploaderCountCond  *sync.Cond
	filepath           util.FullPath
	ChunkSize          int64
	uploaders          *util.LimitedConcurrentExecutor
	saveToStorageFn    SaveToStorageFunc
	writableChunkLimit int
	swapFile           *SwapFile
	chunksLock         sync.Mutex
	writableChunks     map[LogicChunkIndex]PageChunk
	sealedChunks       map[LogicChunkIndex]*SealedChunk
	activeReadChunks   map[LogicChunkIndex]int
	readerCountCond    *sync.Cond
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

func NewUploadPipeline(writers *util.LimitedConcurrentExecutor, chunkSize int64, saveToStorageFn SaveToStorageFunc, bufferChunkLimit int, swapFileDir string) *UploadPipeline {
	t := &UploadPipeline{
		ChunkSize:          chunkSize,
		writableChunks:     make(map[LogicChunkIndex]PageChunk),
		sealedChunks:       make(map[LogicChunkIndex]*SealedChunk),
		uploaders:          writers,
		uploaderCountCond:  sync.NewCond(&sync.Mutex{}),
		saveToStorageFn:    saveToStorageFn,
		activeReadChunks:   make(map[LogicChunkIndex]int),
		writableChunkLimit: bufferChunkLimit,
		swapFile:           NewSwapFile(swapFileDir, chunkSize),
	}
	t.readerCountCond = sync.NewCond(&t.chunksLock)
	return t
}

func (up *UploadPipeline) SaveDataAt(p []byte, off int64, isSequential bool, tsNs int64) (n int) {

	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()

	logicChunkIndex := LogicChunkIndex(off / up.ChunkSize)

	pageChunk, found := up.writableChunks[logicChunkIndex]
	if !found {
		if len(up.writableChunks) > up.writableChunkLimit {
			// if current file chunks is over the per file buffer count limit
			candidateChunkIndex, fullness := LogicChunkIndex(-1), int64(0)
			for lci, mc := range up.writableChunks {
				chunkFullness := mc.WrittenSize()
				if fullness < chunkFullness {
					candidateChunkIndex = lci
					fullness = chunkFullness
				}
			}
			/*  // this algo generates too many chunks
			candidateChunkIndex, lowestActivityScore := LogicChunkIndex(-1), int64(math.MaxInt64)
			for wci, wc := range up.writableChunks {
				activityScore := wc.ActivityScore()
				if lowestActivityScore >= activityScore {
					if lowestActivityScore == activityScore {
						chunkFullness := wc.WrittenSize()
						if fullness < chunkFullness {
							candidateChunkIndex = lci
							fullness = chunkFullness
						}
					}
					candidateChunkIndex = wci
					lowestActivityScore = activityScore
				}
			}
			*/
			up.moveToSealed(up.writableChunks[candidateChunkIndex], candidateChunkIndex)
			// fmt.Printf("flush chunk %d with %d bytes written\n", logicChunkIndex, fullness)
		}
		// fmt.Printf("isSequential:%v len(up.writableChunks):%v memChunkCounter:%v", isSequential, len(up.writableChunks), memChunkCounter)
		if isSequential &&
			len(up.writableChunks) < up.writableChunkLimit &&
			atomic.LoadInt64(&memChunkCounter) < 4*int64(up.writableChunkLimit) {
			pageChunk = NewMemChunk(logicChunkIndex, up.ChunkSize)
			// fmt.Printf(" create mem  chunk %d\n", logicChunkIndex)
		} else {
			pageChunk = up.swapFile.NewSwapFileChunk(logicChunkIndex)
			// fmt.Printf(" create file chunk %d\n", logicChunkIndex)
		}
		up.writableChunks[logicChunkIndex] = pageChunk
	}
	//if _, foundSealed := up.sealedChunks[logicChunkIndex]; foundSealed {
	//	println("found already sealed chunk", logicChunkIndex)
	//}
	//if _, foundReading := up.activeReadChunks[logicChunkIndex]; foundReading {
	//	println("found active read chunk", logicChunkIndex)
	//}
	n = pageChunk.WriteDataAt(p, off, tsNs)
	up.maybeMoveToSealed(pageChunk, logicChunkIndex)

	return
}

func (up *UploadPipeline) MaybeReadDataAt(p []byte, off int64, tsNs int64) (maxStop int64) {
	logicChunkIndex := LogicChunkIndex(off / up.ChunkSize)

	up.chunksLock.Lock()
	defer func() {
		up.readerCountCond.Signal()
		up.chunksLock.Unlock()
	}()

	// read from sealed chunks first
	sealedChunk, found := up.sealedChunks[logicChunkIndex]
	if found {
		maxStop = sealedChunk.chunk.ReadDataAt(p, off, tsNs)
		glog.V(4).Infof("%s read sealed memchunk [%d,%d)", up.filepath, off, maxStop)
	}

	// read from writable chunks last
	writableChunk, found := up.writableChunks[logicChunkIndex]
	if !found {
		return
	}
	writableMaxStop := writableChunk.ReadDataAt(p, off, tsNs)
	glog.V(4).Infof("%s read writable memchunk [%d,%d)", up.filepath, off, writableMaxStop)
	maxStop = max(maxStop, writableMaxStop)

	return
}

func (up *UploadPipeline) FlushAll() {
	up.flushChunks()
	up.waitForCurrentWritersToComplete()
}

func (up *UploadPipeline) flushChunks() {
	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()

	for logicChunkIndex, memChunk := range up.writableChunks {
		up.moveToSealed(memChunk, logicChunkIndex)
	}
}

func (up *UploadPipeline) maybeMoveToSealed(memChunk PageChunk, logicChunkIndex LogicChunkIndex) {
	if memChunk.IsComplete() {
		up.moveToSealed(memChunk, logicChunkIndex)
	}
}

func (up *UploadPipeline) moveToSealed(memChunk PageChunk, logicChunkIndex LogicChunkIndex) {
	atomic.AddInt32(&up.uploaderCount, 1)
	glog.V(4).Infof("%s uploaderCount %d ++> %d", up.filepath, up.uploaderCount-1, up.uploaderCount)

	if oldMemChunk, found := up.sealedChunks[logicChunkIndex]; found {
		oldMemChunk.FreeReference(fmt.Sprintf("%s replace chunk %d", up.filepath, logicChunkIndex))
	}
	sealedChunk := &SealedChunk{
		chunk:            memChunk,
		referenceCounter: 1, // default 1 is for uploading process
	}
	up.sealedChunks[logicChunkIndex] = sealedChunk
	delete(up.writableChunks, logicChunkIndex)

	// unlock before submitting the uploading jobs
	up.chunksLock.Unlock()
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
		up.chunksLock.Lock()
		defer up.chunksLock.Unlock()
		for up.IsLocked(logicChunkIndex) {
			up.readerCountCond.Wait()
		}

		// then remove from sealed chunks
		delete(up.sealedChunks, logicChunkIndex)
		sealedChunk.FreeReference(fmt.Sprintf("%s finished uploading chunk %d", up.filepath, logicChunkIndex))

	})
	up.chunksLock.Lock()
}

func (up *UploadPipeline) Shutdown() {
	up.swapFile.FreeResource()

	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()
	for logicChunkIndex, sealedChunk := range up.sealedChunks {
		sealedChunk.FreeReference(fmt.Sprintf("%s uploadpipeline shutdown chunk %d", up.filepath, logicChunkIndex))
	}
}
