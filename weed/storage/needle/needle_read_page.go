package needle

import (
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ReadNeedleData uses a needle without n.Data to read the content
// volumeOffset: the offset within the volume
// needleOffset: the offset within the needle Data
func (n *Needle) ReadNeedleData(r backend.BackendStorageFile, volumeOffset int64, data []byte, needleOffset int64) (count int, err error) {

	sizeToRead := min(int64(len(data)), int64(n.DataSize)-needleOffset)
	if sizeToRead <= 0 {
		return 0, io.EOF
	}
	startOffset := volumeOffset + NeedleHeaderSize + DataSizeSize + needleOffset

	count, err = r.ReadAt(data[:sizeToRead], startOffset)
	if err == io.EOF && int64(count) == sizeToRead {
		err = nil
	}
	if err != nil {
		fileSize, _, _ := r.GetStat()
		glog.Errorf("%s read %d %d size %d at offset %d fileSize %d: %v", r.Name(), n.Id, needleOffset, sizeToRead, volumeOffset, fileSize, err)
	}
	return

}

// ReadNeedleMeta fills all metadata except the n.Data
func (n *Needle) ReadNeedleMeta(r backend.BackendStorageFile, offset int64, size Size, version Version) error {
	return n.ReadFromFile(r, offset, size, version, NeedleReadOptions{
		ReadHeader: true,
		ReadMeta:   true,
		ReadData:   false,
	})
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
