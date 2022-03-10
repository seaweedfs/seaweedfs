package page_writer

import (
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"sync/atomic"
)

var (
	_ = PageChunk(&MemChunk{})

	memChunkCounter int64
)

type MemChunk struct {
	buf             []byte
	usage           *ChunkWrittenIntervalList
	chunkSize       int64
	logicChunkIndex LogicChunkIndex
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
	atomic.AddInt64(&memChunkCounter, -1)
	mem.Free(mc.buf)
}

func (mc *MemChunk) WriteDataAt(src []byte, offset int64) (n int) {
	innerOffset := offset % mc.chunkSize
	n = copy(mc.buf[innerOffset:], src)
	mc.usage.MarkWritten(innerOffset, innerOffset+int64(n))
	return
}

func (mc *MemChunk) ReadDataAt(p []byte, off int64) (maxStop int64) {
	memChunkBaseOffset := int64(mc.logicChunkIndex) * mc.chunkSize
	for t := mc.usage.head.next; t != mc.usage.tail; t = t.next {
		logicStart := max(off, int64(mc.logicChunkIndex)*mc.chunkSize+t.StartOffset)
		logicStop := min(off+int64(len(p)), memChunkBaseOffset+t.stopOffset)
		if logicStart < logicStop {
			copy(p[logicStart-off:logicStop-off], mc.buf[logicStart-memChunkBaseOffset:logicStop-memChunkBaseOffset])
			maxStop = max(maxStop, logicStop)
		}
	}
	return
}

func (mc *MemChunk) IsComplete() bool {
	return mc.usage.IsComplete(mc.chunkSize)
}

func (mc *MemChunk) WrittenSize() int64 {
	return mc.usage.WrittenSize()
}

func (mc *MemChunk) SaveContent(saveFn SaveToStorageFunc) {
	if saveFn == nil {
		return
	}
	for t := mc.usage.head.next; t != mc.usage.tail; t = t.next {
		reader := util.NewBytesReader(mc.buf[t.StartOffset:t.stopOffset])
		saveFn(reader, int64(mc.logicChunkIndex)*mc.chunkSize+t.StartOffset, t.Size(), func() {
		})
	}
}
