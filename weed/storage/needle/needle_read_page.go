package needle

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
)

// ReadNeedleDataInto uses a needle without n.Data to read the content into an io.Writer
func (n *Needle) ReadNeedleDataInto(r backend.BackendStorageFile, volumeOffset int64, buf []byte, writer io.Writer, needleOffset int64, size int64) (err error) {
	crc := CRC(0)
	for x := needleOffset; x < needleOffset+size; x += int64(len(buf)) {
		count, err := n.ReadNeedleData(r, volumeOffset, buf, x)
		if count > 0 {
			crc = crc.Update(buf[0:count])
			if _, err = writer.Write(buf[0:count]); err != nil {
				return fmt.Errorf("ReadNeedleData write: %v", err)
			}
		}
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return fmt.Errorf("ReadNeedleData: %v", err)
		}
		if count <= 0 {
			break
		}
	}
	if needleOffset == 0 && size == int64(n.DataSize) && (n.Checksum != crc && uint32(n.Checksum) != crc.Value()) {
		// the crc.Value() function is to be deprecated. this double checking is for backward compatible.
		return fmt.Errorf("ReadNeedleData checksum %v expected %v", crc, n.Checksum)
	}
	return nil
}

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
	if err != nil {
		fileSize, _, _ := r.GetStat()
		glog.Errorf("%s read %d %d size %d at offset %d fileSize %d: %v", r.Name(), n.Id, needleOffset, sizeToRead, volumeOffset, fileSize, err)
	}
	return

}

// ReadNeedleMeta fills all metadata except the n.Data
func (n *Needle) ReadNeedleMeta(r backend.BackendStorageFile, offset int64, size Size, version Version) (err error) {

	bytes := make([]byte, NeedleHeaderSize+DataSizeSize)

	count, err := r.ReadAt(bytes, offset)
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

	startOffset := offset + NeedleHeaderSize + DataSizeSize + int64(n.DataSize)
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
	index, err = n.readNeedleDataVersion2NonData(metaSlice)

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
