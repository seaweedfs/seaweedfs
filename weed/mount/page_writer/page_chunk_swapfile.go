package page_writer

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"io"
	"os"
	"sync"
)

var (
	_ = PageChunk(&SwapFileChunk{})
)

type ActualChunkIndex int

type SwapFile struct {
	dir                 string
	file                *os.File
	chunkSize           int64
	chunkTrackingLock   sync.Mutex
	activeChunkCount    int
	freeActualChunkList []ActualChunkIndex
}

type SwapFileChunk struct {
	sync.RWMutex
	swapfile         *SwapFile
	usage            *ChunkWrittenIntervalList
	logicChunkIndex  LogicChunkIndex
	actualChunkIndex ActualChunkIndex
	activityScore    *ActivityScore
	//memChunk         *MemChunk
}

func NewSwapFile(dir string, chunkSize int64) *SwapFile {
	return &SwapFile{
		dir:       dir,
		file:      nil,
		chunkSize: chunkSize,
	}
}
func (sf *SwapFile) FreeResource() {
	if sf.file != nil {
		sf.file.Close()
		os.Remove(sf.file.Name())
	}
}

func (sf *SwapFile) NewSwapFileChunk(logicChunkIndex LogicChunkIndex) (tc *SwapFileChunk) {
	if sf.file == nil {
		var err error
		sf.file, err = os.CreateTemp(sf.dir, "")
		if err != nil {
			glog.Errorf("create swap file: %v", err)
			return nil
		}
	}
	sf.chunkTrackingLock.Lock()
	defer sf.chunkTrackingLock.Unlock()

	sf.activeChunkCount++

	// assign a new physical chunk
	var actualChunkIndex ActualChunkIndex
	if len(sf.freeActualChunkList) > 0 {
		actualChunkIndex = sf.freeActualChunkList[0]
		sf.freeActualChunkList = sf.freeActualChunkList[1:]
	} else {
		actualChunkIndex = ActualChunkIndex(sf.activeChunkCount)
	}

	swapFileChunk := &SwapFileChunk{
		swapfile:         sf,
		usage:            newChunkWrittenIntervalList(),
		logicChunkIndex:  logicChunkIndex,
		actualChunkIndex: actualChunkIndex,
		activityScore:    NewActivityScore(),
		// memChunk:         NewMemChunk(logicChunkIndex, sf.chunkSize),
	}

	// println(logicChunkIndex, "|", "++++", swapFileChunk.actualChunkIndex, swapFileChunk, sf)
	return swapFileChunk
}

func (sc *SwapFileChunk) FreeResource() {

	sc.Lock()
	defer sc.Unlock()

	sc.swapfile.chunkTrackingLock.Lock()
	defer sc.swapfile.chunkTrackingLock.Unlock()

	sc.swapfile.freeActualChunkList = append(sc.swapfile.freeActualChunkList, sc.actualChunkIndex)
	sc.swapfile.activeChunkCount--
	// println(sc.logicChunkIndex, "|", "----", sc.actualChunkIndex, sc, sc.swapfile)
}

func (sc *SwapFileChunk) WriteDataAt(src []byte, offset int64, tsNs int64) (n int) {
	sc.Lock()
	defer sc.Unlock()

	// println(sc.logicChunkIndex, "|", tsNs, "write at", offset, len(src), sc.actualChunkIndex)

	innerOffset := offset % sc.swapfile.chunkSize
	var err error
	n, err = sc.swapfile.file.WriteAt(src, int64(sc.actualChunkIndex)*sc.swapfile.chunkSize+innerOffset)
	sc.usage.MarkWritten(innerOffset, innerOffset+int64(n), tsNs)
	if err != nil {
		glog.Errorf("failed to write swap file %s: %v", sc.swapfile.file.Name(), err)
	}
	//sc.memChunk.WriteDataAt(src, offset, tsNs)
	sc.activityScore.MarkWrite()

	return
}

func (sc *SwapFileChunk) ReadDataAt(p []byte, off int64, tsNs int64) (maxStop int64) {
	sc.RLock()
	defer sc.RUnlock()

	// println(sc.logicChunkIndex, "|", tsNs, "read at", off, len(p), sc.actualChunkIndex)

	//memCopy := make([]byte, len(p))
	//copy(memCopy, p)

	chunkStartOffset := int64(sc.logicChunkIndex) * sc.swapfile.chunkSize
	for t := sc.usage.head.next; t != sc.usage.tail; t = t.next {
		logicStart := max(off, chunkStartOffset+t.StartOffset)
		logicStop := min(off+int64(len(p)), chunkStartOffset+t.stopOffset)
		if logicStart < logicStop {
			actualStart := logicStart - chunkStartOffset + int64(sc.actualChunkIndex)*sc.swapfile.chunkSize
			if n, err := sc.swapfile.file.ReadAt(p[logicStart-off:logicStop-off], actualStart); err != nil {
				if err == io.EOF && n == int(logicStop-logicStart) {
					err = nil
				}
				glog.Errorf("failed to reading swap file %s: %v", sc.swapfile.file.Name(), err)
				break
			}
			maxStop = max(maxStop, logicStop)

			if t.TsNs >= tsNs {
				println("read new data2", t.TsNs-tsNs, "ns")
			}
		}
	}
	//sc.memChunk.ReadDataAt(memCopy, off, tsNs)
	//if bytes.Compare(memCopy, p) != 0 {
	//	println("read wrong data from swap file", off, sc.logicChunkIndex)
	//}

	sc.activityScore.MarkRead()

	return
}

func (sc *SwapFileChunk) IsComplete() bool {
	sc.RLock()
	defer sc.RUnlock()
	return sc.usage.IsComplete(sc.swapfile.chunkSize)
}

func (sc *SwapFileChunk) ActivityScore() int64 {
	return sc.activityScore.ActivityScore()
}

func (sc *SwapFileChunk) WrittenSize() int64 {
	sc.RLock()
	defer sc.RUnlock()
	return sc.usage.WrittenSize()
}

func (sc *SwapFileChunk) SaveContent(saveFn SaveToStorageFunc) {
	sc.RLock()
	defer sc.RUnlock()

	if saveFn == nil {
		return
	}
	// println(sc.logicChunkIndex, "|", "save")
	for t := sc.usage.head.next; t != sc.usage.tail; t = t.next {
		startOffset := t.StartOffset
		stopOffset := t.stopOffset
		tsNs := t.TsNs
		for t != sc.usage.tail && t.next.StartOffset == stopOffset {
			stopOffset = t.next.stopOffset
			t = t.next
			tsNs = max(tsNs, t.TsNs)
		}

		data := mem.Allocate(int(stopOffset - startOffset))
		n, _ := sc.swapfile.file.ReadAt(data, startOffset+int64(sc.actualChunkIndex)*sc.swapfile.chunkSize)
		if n > 0 {
			reader := util.NewBytesReader(data[:n])
			saveFn(reader, int64(sc.logicChunkIndex)*sc.swapfile.chunkSize+startOffset, int64(n), tsNs, func() {
			})
		}
		mem.Free(data)
	}

}
