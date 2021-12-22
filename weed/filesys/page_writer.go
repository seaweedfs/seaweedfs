package filesys

import (
	"github.com/chrislusf/seaweedfs/weed/filesys/page_writer"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

type PageWriter struct {
	f             *File
	collection    string
	replication   string
	chunkSize     int64
	writerPattern *WriterPattern

	randomWriter page_writer.DirtyPages
	streamWriter page_writer.DirtyPages
}

var (
	_ = page_writer.DirtyPages(&PageWriter{})
)

func newPageWriter(file *File, chunkSize int64) *PageWriter {
	pw := &PageWriter{
		f:             file,
		chunkSize:     chunkSize,
		randomWriter:  newTempFileDirtyPages(file),
		streamWriter:  newContinuousDirtyPages(file),
		writerPattern: NewWriterPattern(file.Name, chunkSize),
	}
	return pw
}

func (pw *PageWriter) AddPage(offset int64, data []byte) {

	glog.V(4).Infof("AddPage %v [%d, %d) streaming:%v", pw.f.fullpath(), offset, offset+int64(len(data)), pw.writerPattern.IsStreamingMode())

	pw.writerPattern.MonitorWriteAt(offset, len(data))

	chunkIndex := offset / pw.chunkSize
	for i := chunkIndex; len(data) > 0; i++ {
		writeSize := min(int64(len(data)), (i+1)*pw.chunkSize-offset)
		pw.addToOneChunk(i, offset, data[:writeSize])
		offset += writeSize
		data = data[writeSize:]
	}
}

func (pw *PageWriter) addToOneChunk(chunkIndex, offset int64, data []byte) {
	if chunkIndex > 0 {
		if pw.writerPattern.IsStreamingMode() {
			pw.streamWriter.AddPage(offset, data)
			return
		}
	}
	pw.randomWriter.AddPage(offset, data)
}

func (pw *PageWriter) FlushData() error {
	if err := pw.streamWriter.FlushData(); err != nil {
		return err
	}
	return pw.randomWriter.FlushData()
}

func (pw *PageWriter) ReadDirtyDataAt(data []byte, startOffset int64) (maxStop int64) {
	glog.V(4).Infof("ReadDirtyDataAt %v [%d, %d)", pw.f.fullpath(), startOffset, startOffset+int64(len(data)))
	m1 := pw.streamWriter.ReadDirtyDataAt(data, startOffset)
	m2 := pw.randomWriter.ReadDirtyDataAt(data, startOffset)
	return max(m1, m2)
}

func (pw *PageWriter) GetStorageOptions() (collection, replication string) {
	if pw.writerPattern.IsStreamingMode() {
		return pw.streamWriter.GetStorageOptions()
	}
	return pw.randomWriter.GetStorageOptions()
}
