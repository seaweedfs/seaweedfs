package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/page_writer"
)

type PageWriter struct {
	fh            *FileHandle
	collection    string
	replication   string
	chunkSize     int64
	writerPattern *WriterPattern

	randomWriter page_writer.DirtyPages
}

func newPageWriter(fh *FileHandle, chunkSize int64) *PageWriter {
	pw := &PageWriter{
		fh:            fh,
		chunkSize:     chunkSize,
		writerPattern: NewWriterPattern(chunkSize),
		randomWriter:  newMemoryChunkPages(fh, chunkSize),
	}
	return pw
}

// AddPage returns how many leading bytes of data it buffered; on error those
// bytes stay buffered.
func (pw *PageWriter) AddPage(offset int64, data []byte, isSequential bool, tsNs int64) (buffered int64, err error) {

	glog.V(4).Infof("%v AddPage [%d, %d)", pw.fh.fh, offset, offset+int64(len(data)))

	chunkIndex := offset / pw.chunkSize
	for i := chunkIndex; len(data) > 0; i++ {
		writeSize := min(int64(len(data)), (i+1)*pw.chunkSize-offset)
		n, err := pw.addToOneChunk(i, offset, data[:writeSize], isSequential, tsNs)
		buffered += int64(n)
		if err != nil {
			return buffered, err
		}
		offset += writeSize
		data = data[writeSize:]
	}
	return buffered, nil
}

func (pw *PageWriter) addToOneChunk(chunkIndex, offset int64, data []byte, isSequential bool, tsNs int64) (int, error) {
	return pw.randomWriter.AddPage(offset, data, isSequential, tsNs)
}

func (pw *PageWriter) FlushData() error {
	return pw.randomWriter.FlushData()
}

func (pw *PageWriter) HasWrites() bool {
	return pw.randomWriter.HasWrites()
}

func (pw *PageWriter) HasPendingUploads() bool {
	return pw.randomWriter.HasPendingUploads()
}

func (pw *PageWriter) ReadDirtyDataAt(data []byte, offset int64, tsNs int64) (maxStop int64) {
	glog.V(4).Infof("ReadDirtyDataAt %v [%d, %d)", pw.fh.inode, offset, offset+int64(len(data)))

	chunkIndex := offset / pw.chunkSize
	for i := chunkIndex; len(data) > 0; i++ {
		readSize := min(int64(len(data)), (i+1)*pw.chunkSize-offset)

		// keep the furthest stop across chunks; a clean chunk later in the
		// range returns 0 and must not hide dirty data read from earlier ones
		maxStop = max(maxStop, pw.randomWriter.ReadDirtyDataAt(data[:readSize], offset, tsNs))

		offset += readSize
		data = data[readSize:]
	}

	return
}

func (pw *PageWriter) LockForRead(startOffset, stopOffset int64) {
	pw.randomWriter.LockForRead(startOffset, stopOffset)
}

func (pw *PageWriter) UnlockForRead(startOffset, stopOffset int64) {
	pw.randomWriter.UnlockForRead(startOffset, stopOffset)
}

func (pw *PageWriter) Destroy() {
	pw.randomWriter.Destroy()
}

func (pw *PageWriter) EvictOneWritableChunk() bool {
	return pw.randomWriter.EvictOneWritableChunk()
}

func (pw *PageWriter) ProactiveFlush(nowNs, idleThresholdNs, maxHoldNs, fillRatio int64, frontierLag int, isSequential bool) bool {
	return pw.randomWriter.ProactiveFlush(nowNs, idleThresholdNs, maxHoldNs, fillRatio, frontierLag, isSequential)
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
