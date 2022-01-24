package page_writer

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"os"
)

var (
	_ = PageChunk(&SwapFileChunk{})
)

type ActualChunkIndex int

type SwapFile struct {
	dir                     string
	file                    *os.File
	logicToActualChunkIndex map[LogicChunkIndex]ActualChunkIndex
	chunkSize               int64
}

type SwapFileChunk struct {
	swapfile         *SwapFile
	usage            *ChunkWrittenIntervalList
	logicChunkIndex  LogicChunkIndex
	actualChunkIndex ActualChunkIndex
}

func NewSwapFile(dir string, chunkSize int64) *SwapFile {
	return &SwapFile{
		dir:                     dir,
		file:                    nil,
		logicToActualChunkIndex: make(map[LogicChunkIndex]ActualChunkIndex),
		chunkSize:               chunkSize,
	}
}
func (sf *SwapFile) FreeResource() {
	if sf.file != nil {
		sf.file.Close()
		os.Remove(sf.file.Name())
	}
}

func (sf *SwapFile) NewTempFileChunk(logicChunkIndex LogicChunkIndex) (tc *SwapFileChunk) {
	if sf.file == nil {
		var err error
		sf.file, err = os.CreateTemp(sf.dir, "")
		if err != nil {
			glog.Errorf("create swap file: %v", err)
			return nil
		}
	}
	actualChunkIndex, found := sf.logicToActualChunkIndex[logicChunkIndex]
	if !found {
		actualChunkIndex = ActualChunkIndex(len(sf.logicToActualChunkIndex))
		sf.logicToActualChunkIndex[logicChunkIndex] = actualChunkIndex
	}

	return &SwapFileChunk{
		swapfile:         sf,
		usage:            newChunkWrittenIntervalList(),
		logicChunkIndex:  logicChunkIndex,
		actualChunkIndex: actualChunkIndex,
	}
}

func (sc *SwapFileChunk) FreeResource() {
}

func (sc *SwapFileChunk) WriteDataAt(src []byte, offset int64) (n int) {
	innerOffset := offset % sc.swapfile.chunkSize
	var err error
	n, err = sc.swapfile.file.WriteAt(src, int64(sc.actualChunkIndex)*sc.swapfile.chunkSize+innerOffset)
	if err == nil {
		sc.usage.MarkWritten(innerOffset, innerOffset+int64(n))
	} else {
		glog.Errorf("failed to write swap file %s: %v", sc.swapfile.file.Name(), err)
	}
	return
}

func (sc *SwapFileChunk) ReadDataAt(p []byte, off int64) (maxStop int64) {
	chunkStartOffset := int64(sc.logicChunkIndex) * sc.swapfile.chunkSize
	for t := sc.usage.head.next; t != sc.usage.tail; t = t.next {
		logicStart := max(off, chunkStartOffset+t.StartOffset)
		logicStop := min(off+int64(len(p)), chunkStartOffset+t.stopOffset)
		if logicStart < logicStop {
			actualStart := logicStart - chunkStartOffset + int64(sc.actualChunkIndex)*sc.swapfile.chunkSize
			if _, err := sc.swapfile.file.ReadAt(p[logicStart-off:logicStop-off], actualStart); err != nil {
				glog.Errorf("failed to reading swap file %s: %v", sc.swapfile.file.Name(), err)
				break
			}
			maxStop = max(maxStop, logicStop)
		}
	}
	return
}

func (sc *SwapFileChunk) IsComplete() bool {
	return sc.usage.IsComplete(sc.swapfile.chunkSize)
}

func (sc *SwapFileChunk) WrittenSize() int64 {
	return sc.usage.WrittenSize()
}

func (sc *SwapFileChunk) SaveContent(saveFn SaveToStorageFunc) {
	if saveFn == nil {
		return
	}
	for t := sc.usage.head.next; t != sc.usage.tail; t = t.next {
		data := mem.Allocate(int(t.Size()))
		sc.swapfile.file.ReadAt(data, t.StartOffset+int64(sc.actualChunkIndex)*sc.swapfile.chunkSize)
		reader := util.NewBytesReader(data)
		saveFn(reader, int64(sc.logicChunkIndex)*sc.swapfile.chunkSize+t.StartOffset, t.Size(), func() {
		})
		mem.Free(data)
	}
	sc.usage = newChunkWrittenIntervalList()
}
