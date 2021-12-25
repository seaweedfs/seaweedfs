package page_writer

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"io"
	"os"
	"sync"
)

type LogicChunkIndex int
type ActualChunkIndex int

// ChunkedFileWriter assumes the write requests will come in within chunks
type ChunkedFileWriter struct {
	dir                     string
	file                    *os.File
	logicToActualChunkIndex map[LogicChunkIndex]ActualChunkIndex
	chunkUsages             []*ChunkWrittenIntervalList
	ChunkSize               int64
	sync.Mutex
}

var _ = io.WriterAt(&ChunkedFileWriter{})

func NewChunkedFileWriter(dir string, chunkSize int64) *ChunkedFileWriter {
	return &ChunkedFileWriter{
		dir:                     dir,
		file:                    nil,
		logicToActualChunkIndex: make(map[LogicChunkIndex]ActualChunkIndex),
		ChunkSize:               chunkSize,
	}
}

func (cw *ChunkedFileWriter) WriteAt(p []byte, off int64) (n int, err error) {
	cw.Lock()
	defer cw.Unlock()

	if cw.file == nil {
		cw.file, err = os.CreateTemp(cw.dir, "")
		if err != nil {
			glog.Errorf("create temp file: %v", err)
			return
		}
	}

	actualOffset, chunkUsage := cw.toActualWriteOffset(off)
	n, err = cw.file.WriteAt(p, actualOffset)
	if err == nil {
		startOffset := off % cw.ChunkSize
		chunkUsage.MarkWritten(startOffset, startOffset+int64(n))
	}
	return
}

func (cw *ChunkedFileWriter) ReadDataAt(p []byte, off int64) (maxStop int64) {
	cw.Lock()
	defer cw.Unlock()

	if cw.file == nil {
		return
	}

	logicChunkIndex := off / cw.ChunkSize
	actualChunkIndex, chunkUsage := cw.toActualReadOffset(off)
	if chunkUsage != nil {
		for t := chunkUsage.head.next; t != chunkUsage.tail; t = t.next {
			logicStart := max(off, logicChunkIndex*cw.ChunkSize+t.StartOffset)
			logicStop := min(off+int64(len(p)), logicChunkIndex*cw.ChunkSize+t.stopOffset)
			if logicStart < logicStop {
				actualStart := logicStart - logicChunkIndex*cw.ChunkSize + int64(actualChunkIndex)*cw.ChunkSize
				_, err := cw.file.ReadAt(p[logicStart-off:logicStop-off], actualStart)
				if err != nil {
					glog.Errorf("reading temp file: %v", err)
					break
				}
				maxStop = max(maxStop, logicStop)
			}
		}
	}
	return
}

func (cw *ChunkedFileWriter) toActualWriteOffset(logicOffset int64) (actualOffset int64, chunkUsage *ChunkWrittenIntervalList) {
	logicChunkIndex := LogicChunkIndex(logicOffset / cw.ChunkSize)
	offsetRemainder := logicOffset % cw.ChunkSize
	existingActualChunkIndex, found := cw.logicToActualChunkIndex[logicChunkIndex]
	if found {
		return int64(existingActualChunkIndex)*cw.ChunkSize + offsetRemainder, cw.chunkUsages[existingActualChunkIndex]
	}
	cw.logicToActualChunkIndex[logicChunkIndex] = ActualChunkIndex(len(cw.chunkUsages))
	chunkUsage = newChunkWrittenIntervalList()
	cw.chunkUsages = append(cw.chunkUsages, chunkUsage)
	return int64(len(cw.chunkUsages)-1)*cw.ChunkSize + offsetRemainder, chunkUsage
}

func (cw *ChunkedFileWriter) toActualReadOffset(logicOffset int64) (actualChunkIndex ActualChunkIndex, chunkUsage *ChunkWrittenIntervalList) {
	logicChunkIndex := LogicChunkIndex(logicOffset / cw.ChunkSize)
	existingActualChunkIndex, found := cw.logicToActualChunkIndex[logicChunkIndex]
	if found {
		return existingActualChunkIndex, cw.chunkUsages[existingActualChunkIndex]
	}
	return 0, nil
}

func (cw *ChunkedFileWriter) ProcessEachInterval(process func(file *os.File, logicChunkIndex LogicChunkIndex, interval *ChunkWrittenInterval)) {
	for logicChunkIndex, actualChunkIndex := range cw.logicToActualChunkIndex {
		chunkUsage := cw.chunkUsages[actualChunkIndex]
		for t := chunkUsage.head.next; t != chunkUsage.tail; t = t.next {
			process(cw.file, logicChunkIndex, t)
		}
	}
}

// Reset releases used resources
func (cw *ChunkedFileWriter) Reset() {
	if cw.file != nil {
		cw.file.Close()
		os.Remove(cw.file.Name())
		cw.file = nil
	}
	cw.logicToActualChunkIndex = make(map[LogicChunkIndex]ActualChunkIndex)
	cw.chunkUsages = cw.chunkUsages[:0]
}

type FileIntervalReader struct {
	f           *os.File
	startOffset int64
	stopOffset  int64
	position    int64
}

var _ = io.Reader(&FileIntervalReader{})

func NewFileIntervalReader(cw *ChunkedFileWriter, logicChunkIndex LogicChunkIndex, interval *ChunkWrittenInterval) *FileIntervalReader {
	actualChunkIndex, found := cw.logicToActualChunkIndex[logicChunkIndex]
	if !found {
		// this should never happen
		return nil
	}
	return &FileIntervalReader{
		f:           cw.file,
		startOffset: int64(actualChunkIndex)*cw.ChunkSize + interval.StartOffset,
		stopOffset:  int64(actualChunkIndex)*cw.ChunkSize + interval.stopOffset,
		position:    0,
	}
}

func (fr *FileIntervalReader) Read(p []byte) (n int, err error) {
	readSize := minInt(len(p), int(fr.stopOffset-fr.startOffset-fr.position))
	n, err = fr.f.ReadAt(p[:readSize], fr.startOffset+fr.position)
	if err == nil || err == io.EOF {
		fr.position += int64(n)
		if fr.stopOffset-fr.startOffset-fr.position == 0 {
			// return a tiny bit faster
			err = io.EOF
			return
		}
	}
	return
}
