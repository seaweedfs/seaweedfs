package page_writer

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"os"
)

var (
	_ = PageChunk(&TempFileChunk{})
)

type ActualChunkIndex int

type SwapFile struct {
	dir                     string
	file                    *os.File
	logicToActualChunkIndex map[LogicChunkIndex]ActualChunkIndex
	chunkSize               int64
}

type TempFileChunk struct {
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

func (sf *SwapFile) NewTempFileChunk(logicChunkIndex LogicChunkIndex) (tc *TempFileChunk) {
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

	return &TempFileChunk{
		swapfile:         sf,
		usage:            newChunkWrittenIntervalList(),
		logicChunkIndex:  logicChunkIndex,
		actualChunkIndex: actualChunkIndex,
	}
}

func (tc *TempFileChunk) FreeResource() {
}

func (tc *TempFileChunk) WriteDataAt(src []byte, offset int64) (n int) {
	innerOffset := offset % tc.swapfile.chunkSize
	var err error
	n, err = tc.swapfile.file.WriteAt(src, int64(tc.actualChunkIndex)*tc.swapfile.chunkSize+innerOffset)
	if err == nil {
		tc.usage.MarkWritten(innerOffset, innerOffset+int64(n))
	} else {
		glog.Errorf("failed to write swap file %s: %v", tc.swapfile.file.Name(), err)
	}
	return
}

func (tc *TempFileChunk) ReadDataAt(p []byte, off int64) (maxStop int64) {
	chunkStartOffset := int64(tc.logicChunkIndex) * tc.swapfile.chunkSize
	for t := tc.usage.head.next; t != tc.usage.tail; t = t.next {
		logicStart := max(off, chunkStartOffset+t.StartOffset)
		logicStop := min(off+int64(len(p)), chunkStartOffset+t.stopOffset)
		if logicStart < logicStop {
			actualStart := logicStart - chunkStartOffset + int64(tc.actualChunkIndex)*tc.swapfile.chunkSize
			if _, err := tc.swapfile.file.ReadAt(p[logicStart-off:logicStop-off], actualStart); err != nil {
				glog.Errorf("failed to reading swap file %s: %v", tc.swapfile.file.Name(), err)
				break
			}
			maxStop = max(maxStop, logicStop)
		}
	}
	return
}

func (tc *TempFileChunk) IsComplete() bool {
	return tc.usage.IsComplete(tc.swapfile.chunkSize)
}

func (tc *TempFileChunk) SaveContent(saveFn SaveToStorageFunc) {
	if saveFn == nil {
		return
	}
	for t := tc.usage.head.next; t != tc.usage.tail; t = t.next {
		data := mem.Allocate(int(t.Size()))
		tc.swapfile.file.ReadAt(data, t.StartOffset+int64(tc.actualChunkIndex)*tc.swapfile.chunkSize)
		reader := util.NewBytesReader(data)
		saveFn(reader, int64(tc.logicChunkIndex)*tc.swapfile.chunkSize+t.StartOffset, t.Size(), func() {
		})
		mem.Free(data)
	}
}
