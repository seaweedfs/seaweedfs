package page_writer

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type LogicChunkIndex int

type UploadPipeline struct {
	uploaderCount       int32
	uploaderCountCond   *sync.Cond
	filepath            util.FullPath
	ChunkSize           int64
	uploaders           *util.LimitedConcurrentExecutor
	saveToStorageFn     SaveToStorageFunc
	writableChunkLimit  int
	concurrentWriterMax int32
	swapFile            *SwapFile
	chunksLock          sync.Mutex
	writableChunks      map[LogicChunkIndex]PageChunk
	sealedChunks        map[LogicChunkIndex]*SealedChunk
	activeReadChunks    map[LogicChunkIndex]int
	readerCountCond     *sync.Cond
	accountant          *WriteBufferAccountant
	lastWriteChunkIndex int64 // atomic: highest LogicChunkIndex written
}

type SealedChunk struct {
	chunk            PageChunk
	referenceCounter int // track uploading or reading processes
	// accountant and chunkSize are captured when the chunk is sealed so
	// FreeReference can release the global write-budget slot exactly once
	// regardless of which code path triggers the final deref (normal
	// upload completion, Shutdown, or replace-in-place in moveToSealed).
	accountant *WriteBufferAccountant
	chunkSize  int64
}

func (sc *SealedChunk) FreeReference(messageOnFree string) {
	// Early-return guard so repeated calls (Shutdown racing the async
	// uploader, or any future caller that loses track of ownership) are
	// strict no-ops rather than driving referenceCounter negative.
	if sc.referenceCounter <= 0 {
		return
	}
	sc.referenceCounter--
	if sc.referenceCounter == 0 {
		glog.V(4).Infof("Free sealed chunk: %s", messageOnFree)
		sc.chunk.FreeResource()
		sc.accountant.Release(sc.chunkSize)
	}
}

// NewUploadPipeline constructs an UploadPipeline. accountant may be nil,
// in which case no global write-buffer cap is enforced. When non-nil,
// creating a new page chunk (memory or swap) first reserves ChunkSize
// bytes against it, blocking the writer if the global cap is reached.
// The accountant is captured at construction so the pipeline's hot paths
// (SaveDataAt, moveToSealed, Shutdown) can read up.accountant without
// any synchronization.
func NewUploadPipeline(writers *util.LimitedConcurrentExecutor, chunkSize int64, saveToStorageFn SaveToStorageFunc, bufferChunkLimit int, swapFileDir string, accountant *WriteBufferAccountant) *UploadPipeline {
	t := &UploadPipeline{
		ChunkSize:           chunkSize,
		writableChunks:      make(map[LogicChunkIndex]PageChunk),
		sealedChunks:        make(map[LogicChunkIndex]*SealedChunk),
		uploaders:           writers,
		uploaderCountCond:   sync.NewCond(&sync.Mutex{}),
		saveToStorageFn:     saveToStorageFn,
		activeReadChunks:    make(map[LogicChunkIndex]int),
		writableChunkLimit:  bufferChunkLimit,
		concurrentWriterMax: int32(bufferChunkLimit),
		swapFile:            NewSwapFile(swapFileDir, chunkSize),
		accountant:          accountant,
	}
	t.readerCountCond = sync.NewCond(&t.chunksLock)
	return t
}

func (up *UploadPipeline) SaveDataAt(p []byte, off int64, isSequential bool, tsNs int64) (n int, err error) {

	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()

	logicChunkIndex := LogicChunkIndex(off / up.ChunkSize)

	// track write frontier for proactive flushing (CAS to avoid regression)
	for {
		old := atomic.LoadInt64(&up.lastWriteChunkIndex)
		if int64(logicChunkIndex) <= old {
			break
		}
		if atomic.CompareAndSwapInt64(&up.lastWriteChunkIndex, old, int64(logicChunkIndex)) {
			break
		}
	}

	pageChunk, found := up.writableChunks[logicChunkIndex]
	if !found {
		// Reserve a chunk-sized slot against the global write budget before
		// allocating. Reserve may block when volumes are full and sealed
		// chunks can't drain, so we must release chunksLock first — the
		// uploader goroutines that eventually call Release take chunksLock.
		if up.accountant != nil {
			up.chunksLock.Unlock()
			up.accountant.Reserve(up.ChunkSize)
			up.chunksLock.Lock()
			// Re-check: another writer on the same file may have created
			// the chunk while we were blocked. If so, give the slot back.
			if pageChunk, found = up.writableChunks[logicChunkIndex]; found {
				up.accountant.Release(up.ChunkSize)
			}
		}
	}
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
			if pageChunk == nil {
				up.accountant.Release(up.ChunkSize)
				return 0, fmt.Errorf("failed to create swap file chunk")
			}
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
	newCount := atomic.AddInt32(&up.uploaderCount, 1)
	glog.V(4).Infof("%s uploaderCount %d ++> %d", up.filepath, newCount-1, newCount)

	if oldMemChunk, found := up.sealedChunks[logicChunkIndex]; found {
		oldMemChunk.FreeReference(fmt.Sprintf("%s replace chunk %d", up.filepath, logicChunkIndex))
	}
	sealedChunk := &SealedChunk{
		chunk:            memChunk,
		referenceCounter: 1, // default 1 is for uploading process
		accountant:       up.accountant,
		chunkSize:        up.ChunkSize,
	}
	up.sealedChunks[logicChunkIndex] = sealedChunk
	delete(up.writableChunks, logicChunkIndex)

	// unlock before submitting the uploading jobs
	up.chunksLock.Unlock()
	up.uploaders.Execute(func() {
		// first add to the file chunks
		sealedChunk.chunk.SaveContent(up.saveToStorageFn)

		// notify waiting process
		newCount := atomic.AddInt32(&up.uploaderCount, -1)
		glog.V(4).Infof("%s uploaderCount %d --> %d", up.filepath, newCount+1, newCount)
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

// EvictOneWritableChunk force-seals the fullest writable chunk in this
// pipeline, submitting it for async upload. Called by the accountant's
// evictor when Reserve would block. Returns true if a chunk was sealed.
// The fullest-chunk heuristic matches the over-limit path in SaveDataAt:
// sealing the chunk closest to full maximizes the upload's usefulness
// and avoids thrashing on repeatedly re-creating the same half-empty
// chunk. Callers must not hold up.chunksLock.
func (up *UploadPipeline) EvictOneWritableChunk() bool {
	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()
	if len(up.writableChunks) == 0 {
		return false
	}
	var bestIndex LogicChunkIndex
	var bestBytes int64 = -1
	for lci, wc := range up.writableChunks {
		if b := wc.WrittenSize(); b > bestBytes {
			bestIndex = lci
			bestBytes = b
		}
	}
	if bestBytes < 0 {
		return false
	}
	up.moveToSealed(up.writableChunks[bestIndex], bestIndex)
	return true
}

// ProactiveFlush seals at most one idle writable chunk that is unlikely to
// receive further writes, submitting it for async upload. Returns true if a
// chunk was sealed. The caller (ChunkFlusher) invokes this periodically so
// that partially-written chunks drain continuously instead of piling up
// until fsync.
func (up *UploadPipeline) ProactiveFlush(nowNs int64, idleThresholdNs int64, maxHoldNs int64, fillRatio int64, frontierLag int, isSequential bool) bool {
	if up.concurrentWriterMax <= 0 || atomic.LoadInt32(&up.uploaderCount)*2 >= up.concurrentWriterMax {
		return false
	}

	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()

	if len(up.writableChunks) == 0 {
		return false
	}

	frontier := atomic.LoadInt64(&up.lastWriteChunkIndex)
	isSeq := isSequential

	var bestIdx LogicChunkIndex = -1
	var bestBytes int64 = -1

	for lci, chunk := range up.writableChunks {
		lastWrite := chunk.LastWriteTsNs()
		if lastWrite == 0 {
			continue
		}
		age := nowNs - lastWrite
		if age < idleThresholdNs {
			continue
		}
		written := chunk.WrittenSize()
		nearlyFull := written >= fillRatio
		behindFrontier := isSeq && int64(lci) <= frontier-int64(frontierLag)
		stale := age >= maxHoldNs

		if !nearlyFull && !behindFrontier && !stale {
			continue
		}
		if written > bestBytes {
			bestIdx = lci
			bestBytes = written
		}
	}
	if bestIdx < 0 {
		return false
	}
	glog.V(3).Infof("%s proactive flush chunk %d (%d bytes written)", up.filepath, bestIdx, bestBytes)
	up.moveToSealed(up.writableChunks[bestIdx], bestIdx)
	return true
}

func (up *UploadPipeline) Shutdown() {
	up.swapFile.FreeResource()

	up.chunksLock.Lock()
	defer up.chunksLock.Unlock()
	// Free any writable chunks that were reserved but never sealed — on the
	// Destroy() path (truncate / metadata invalidation) there is no
	// preceding FlushData(), so dirty writable chunks would otherwise leak
	// both their memory and their write-budget slots.
	for logicChunkIndex, writableChunk := range up.writableChunks {
		glog.V(4).Infof("%s uploadpipeline shutdown writable chunk %d", up.filepath, logicChunkIndex)
		writableChunk.FreeResource()
		up.accountant.Release(up.ChunkSize)
		delete(up.writableChunks, logicChunkIndex)
	}
	for logicChunkIndex, sealedChunk := range up.sealedChunks {
		// FreeReference releases the accountant slot on the refcount-zero
		// transition; a racing async uploader will call FreeReference again
		// and be a no-op, so there is no double-release.
		sealedChunk.FreeReference(fmt.Sprintf("%s uploadpipeline shutdown chunk %d", up.filepath, logicChunkIndex))
	}
}
