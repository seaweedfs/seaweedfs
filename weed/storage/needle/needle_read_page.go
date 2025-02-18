package needle

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
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
func (n *Needle) ReadNeedleMeta(r backend.BackendStorageFile, offset int64, size Size, version Version) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %+v", r)
		}
	}()

	bytes := make([]byte, NeedleHeaderSize+DataSizeSize)

	count, err := r.ReadAt(bytes, offset)
	if err == io.EOF && count == NeedleHeaderSize+DataSizeSize {
		err = nil
	}
	if count != NeedleHeaderSize+DataSizeSize || err != nil {
		return err
	}
	n.ParseNeedleHeader(bytes)
	if n.Size != size {
		if OffsetSize == 4 && offset < int64(MaxPossibleVolumeSize) {
			return ErrorSizeMismatch
		}
	}
	n.DataSize = util.BytesToUint32(bytes[NeedleHeaderSize : NeedleHeaderSize+DataSizeSize])
	startOffset := offset + NeedleHeaderSize
	if size.IsValid() {
		startOffset = offset + NeedleHeaderSize + DataSizeSize + int64(n.DataSize)
	}
	dataSize := GetActualSize(size, version)
	stopOffset := offset + dataSize
	metaSize := stopOffset - startOffset
	metaSlice := make([]byte, int(metaSize))

	count, err = r.ReadAt(metaSlice, startOffset)
	if err != nil && int64(count) == metaSize {
		err = nil
	}
	if err != nil {
		return err
	}

	var index int
	if size.IsValid() {
		index, err = n.readNeedleDataVersion2NonData(metaSlice)
	}

	n.Checksum = CRC(util.BytesToUint32(metaSlice[index : index+NeedleChecksumSize]))
	if version == Version3 {
		n.AppendAtNs = util.BytesToUint64(metaSlice[index+NeedleChecksumSize : index+NeedleChecksumSize+TimestampSize])
	}
	return err

}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
