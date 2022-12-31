package page_writer

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"sync"
	"sync/atomic"
	"time"
)

var (
	_ = PageChunk(&MemChunk{})

	memChunkCounter int64
)

type MemChunk struct {
	sync.RWMutex
	buf                    []byte
	usage                  *ChunkWrittenIntervalList
	chunkSize              int64
	logicChunkIndex        LogicChunkIndex
	lastActiveTsNs         int64
	decayedActivenessScore int64
}

func NewMemChunk(logicChunkIndex LogicChunkIndex, chunkSize int64) *MemChunk {
	atomic.AddInt64(&memChunkCounter, 1)
	return &MemChunk{
		logicChunkIndex: logicChunkIndex,
		chunkSize:       chunkSize,
		buf:             mem.Allocate(int(chunkSize)),
		usage:           newChunkWrittenIntervalList(),
	}
}

func (mc *MemChunk) FreeResource() {
	mc.Lock()
	defer mc.Unlock()

	atomic.AddInt64(&memChunkCounter, -1)
	mem.Free(mc.buf)
}

func (mc *MemChunk) WriteDataAt(src []byte, offset int64, tsNs int64) (n int) {
	mc.Lock()
	defer mc.Unlock()

	now := time.Now().UnixNano()
	deltaTime := (now - mc.lastActiveTsNs) >> 30 // about number of seconds
	mc.lastActiveTsNs = now

	innerOffset := offset % mc.chunkSize
	n = copy(mc.buf[innerOffset:], src)
	mc.usage.MarkWritten(innerOffset, innerOffset+int64(n), tsNs)

	mc.decayedActivenessScore = mc.decayedActivenessScore>>deltaTime + 1024
	if mc.decayedActivenessScore < 0 {
		mc.decayedActivenessScore = 0
	}
	return
}

func (mc *MemChunk) ReadDataAt(p []byte, off int64, tsNs int64) (maxStop int64) {
	mc.RLock()
	defer mc.RUnlock()

	memChunkBaseOffset := int64(mc.logicChunkIndex) * mc.chunkSize
	for t := mc.usage.head.next; t != mc.usage.tail; t = t.next {
		logicStart := max(off, memChunkBaseOffset+t.StartOffset)
		logicStop := min(off+int64(len(p)), memChunkBaseOffset+t.stopOffset)
		if logicStart < logicStop {
			if t.TsNs >= tsNs {
				copy(p[logicStart-off:logicStop-off], mc.buf[logicStart-memChunkBaseOffset:logicStop-memChunkBaseOffset])
				maxStop = max(maxStop, logicStop)
			} else {
				println("read old data1", tsNs-t.TsNs, "ns")
			}
		}
	}

	now := time.Now().UnixNano()
	deltaTime := (now - mc.lastActiveTsNs) >> 30 // about number of seconds
	mc.lastActiveTsNs = now
	mc.decayedActivenessScore = mc.decayedActivenessScore>>deltaTime + 256
	if mc.decayedActivenessScore < 0 {
		mc.decayedActivenessScore = 0
	}
	return
}

func (mc *MemChunk) IsComplete() bool {
	mc.RLock()
	defer mc.RUnlock()

	return mc.usage.IsComplete(mc.chunkSize)
}

func (mc *MemChunk) ActivenessScore() int64 {
	deltaTime := (time.Now().UnixNano() - mc.lastActiveTsNs) >> 30 // about number of seconds
	return mc.decayedActivenessScore >> deltaTime
}

func (mc *MemChunk) SaveContent(saveFn SaveToStorageFunc) {
	mc.RLock()
	defer mc.RUnlock()

	if saveFn == nil {
		return
	}
	for t := mc.usage.head.next; t != mc.usage.tail; t = t.next {
		reader := util.NewBytesReader(mc.buf[t.StartOffset:t.stopOffset])
		saveFn(reader, int64(mc.logicChunkIndex)*mc.chunkSize+t.StartOffset, t.Size(), t.TsNs, func() {
		})
	}
}
