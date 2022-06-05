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
func (n *Needle) ReadNeedleDataInto(r backend.BackendStorageFile, offset int64, buf []byte, writer io.Writer, expectedChecksumValue uint32) (err error) {
	crc := CRC(0)
	for x := 0; ; x += len(buf) {
		count, err := n.ReadNeedleData(r, offset, buf, int64(x))
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("ReadNeedleData: %v", err)
		}
		if count > 0 {
			crc = crc.Update(buf[0:count])
			if _, err = writer.Write(buf[0:count]); err != nil {
				return fmt.Errorf("ReadNeedleData write: %v", err)
			}
		} else {
			break
		}
	}
	if expectedChecksumValue != crc.Value() {
		return fmt.Errorf("ReadNeedleData checksum %v expected %v", crc.Value(), expectedChecksumValue)
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
func (n *Needle) ReadNeedleMeta(r backend.BackendStorageFile, offset int64, size Size, version Version) (checksumValue uint32, err error) {

	bytes := make([]byte, NeedleHeaderSize+DataSizeSize)

	count, err := r.ReadAt(bytes, offset)
	if count != NeedleHeaderSize+DataSizeSize || err != nil {
		return 0, err
	}
	n.ParseNeedleHeader(bytes)
	n.DataSize = util.BytesToUint32(bytes[NeedleHeaderSize : NeedleHeaderSize+DataSizeSize])

	startOffset := offset + NeedleHeaderSize + DataSizeSize + int64(n.DataSize)
	dataSize := GetActualSize(size, version)
	stopOffset := offset + dataSize
	metaSize := stopOffset - startOffset
	fmt.Printf("offset %d dataSize %d\n", offset, dataSize)
	fmt.Printf("read needle meta [%d,%d) size %d\n", startOffset, stopOffset, metaSize)
	metaSlice := make([]byte, int(metaSize))

	count, err = r.ReadAt(metaSlice, startOffset)
	if err != nil && int64(count) == metaSize {
		err = nil
	}
	if err != nil {
		return 0, err
	}

	var index int
	index, err = n.readNeedleDataVersion2NonData(metaSlice)

	checksumValue = util.BytesToUint32(metaSlice[index : index+NeedleChecksumSize])
	if version == Version3 {
		n.AppendAtNs = util.BytesToUint64(metaSlice[index+NeedleChecksumSize : index+NeedleChecksumSize+TimestampSize])
	}

	return checksumValue, err

}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
