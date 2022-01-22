package page_writer

import (
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"io"
)

type SaveToStorageFunc func(reader io.Reader, offset int64, size int64, cleanupFn func())

type PageChunk interface {
	FreeResource()
	WriteDataAt(src []byte, offset int64) (n int)
	ReadDataAt(p []byte, off int64, logicChunkIndex LogicChunkIndex, chunkSize int64) (maxStop int64)
	IsComplete(chunkSize int64) bool
	SaveContent(saveFn SaveToStorageFunc, logicChunkIndex LogicChunkIndex, chunkSize int64)
}

var (
	_ = PageChunk(&MemChunk{})
)

type MemChunk struct {
	buf   []byte
	usage *ChunkWrittenIntervalList
}

func (mc *MemChunk) FreeResource() {
	mem.Free(mc.buf)
}

func (mc *MemChunk) WriteDataAt(src []byte, offset int64) (n int) {
	n = copy(mc.buf[offset:], src)
	mc.usage.MarkWritten(offset, offset+int64(n))
	return
}

func (mc *MemChunk) ReadDataAt(p []byte, off int64, logicChunkIndex LogicChunkIndex, chunkSize int64) (maxStop int64) {
	memChunkBaseOffset := int64(logicChunkIndex) * chunkSize
	for t := mc.usage.head.next; t != mc.usage.tail; t = t.next {
		logicStart := max(off, int64(logicChunkIndex)*chunkSize+t.StartOffset)
		logicStop := min(off+int64(len(p)), memChunkBaseOffset+t.stopOffset)
		if logicStart < logicStop {
			copy(p[logicStart-off:logicStop-off], mc.buf[logicStart-memChunkBaseOffset:logicStop-memChunkBaseOffset])
			maxStop = max(maxStop, logicStop)
		}
	}
	return
}

func (mc *MemChunk) IsComplete(chunkSize int64) bool {
	return mc.usage.IsComplete(chunkSize)
}

func (mc *MemChunk) SaveContent(saveFn SaveToStorageFunc, logicChunkIndex LogicChunkIndex, chunkSize int64) {
	if saveFn == nil {
		return
	}
	for t := mc.usage.head.next; t != mc.usage.tail; t = t.next {
		reader := util.NewBytesReader(mc.buf[t.StartOffset:t.stopOffset])
		saveFn(reader, int64(logicChunkIndex)*chunkSize+t.StartOffset, t.Size(), func() {
		})
	}
}
