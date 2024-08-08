package page_writer

import (
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
)

var (
	_ = PageChunk(&MemChunk{})

	memChunkCounter int64
)

type MemChunk struct {
	sync.RWMutex
	buf             []byte
	usage           *ChunkWrittenIntervalList
	chunkSize       int64
	logicChunkIndex LogicChunkIndex
	activityScore   *ActivityScore
}

func NewMemChunk(logicChunkIndex LogicChunkIndex, chunkSize int64) *MemChunk {
	atomic.AddInt64(&memChunkCounter, 1)
	return &MemChunk{
		logicChunkIndex: logicChunkIndex,
		chunkSize:       chunkSize,
		buf:             mem.Allocate(int(chunkSize)),
		usage:           newChunkWrittenIntervalList(),
		activityScore:   NewActivityScore(),
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

	innerOffset := offset % mc.chunkSize
	n = copy(mc.buf[innerOffset:], src)
	mc.usage.MarkWritten(innerOffset, innerOffset+int64(n), tsNs)
	mc.activityScore.MarkWrite()

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
			copy(p[logicStart-off:logicStop-off], mc.buf[logicStart-memChunkBaseOffset:logicStop-memChunkBaseOffset])
			maxStop = max(maxStop, logicStop)

			if t.TsNs >= tsNs {
				println("read new data1", t.TsNs-tsNs, "ns")
			}
		}
	}
	mc.activityScore.MarkRead()

	return
}

func (mc *MemChunk) IsComplete() bool {
	mc.RLock()
	defer mc.RUnlock()

	return mc.usage.IsComplete(mc.chunkSize)
}

func (mc *MemChunk) ActivityScore() int64 {
	return mc.activityScore.ActivityScore()
}

func (mc *MemChunk) WrittenSize() int64 {
	mc.RLock()
	defer mc.RUnlock()

	return mc.usage.WrittenSize()
}

func (mc *MemChunk) SaveContent(saveFn SaveToStorageFunc) {
	mc.RLock()
	defer mc.RUnlock()

	if saveFn == nil {
		return
	}

	for t := mc.usage.head.next; t != mc.usage.tail; t = t.next {
		startOffset := t.StartOffset
		stopOffset := t.stopOffset
		tsNs := t.TsNs
		for t != mc.usage.tail && t.next.StartOffset == stopOffset {
			stopOffset = t.next.stopOffset
			t = t.next
			tsNs = max(tsNs, t.TsNs)
		}
		reader := util.NewBytesReader(mc.buf[startOffset:stopOffset])
		saveFn(reader, int64(mc.logicChunkIndex)*mc.chunkSize+startOffset, stopOffset-startOffset, tsNs, func() {
		})
	}
}
