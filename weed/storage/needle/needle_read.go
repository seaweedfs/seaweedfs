package needle

import (
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
)

const (
	FlagIsCompressed        = 0x01
	FlagHasName             = 0x02
	FlagHasMime             = 0x04
	FlagHasLastModifiedDate = 0x08
	FlagHasTtl              = 0x10
	FlagHasPairs            = 0x20
	FlagIsChunkManifest     = 0x80
	LastModifiedBytesLength = 5
	TtlBytesLength          = 2
)

var ErrorSizeMismatch = errors.New("size mismatch")
var ErrorSizeInvalid = errors.New("size invalid")

func (n *Needle) DiskSize(version Version) int64 {
	return GetActualSize(n.Size, version)
}

func ReadNeedleBlob(r backend.BackendStorageFile, offset int64, size Size, version Version) (dataSlice []byte, err error) {

	dataSize := GetActualSize(size, version)
	dataSlice = make([]byte, int(dataSize))

	var n int
	n, err = r.ReadAt(dataSlice, offset)
	if err != nil && int64(n) == dataSize {
		err = nil
	}
	if err != nil {
		fileSize, _, _ := r.GetStat()
		glog.Errorf("%s read %d dataSize %d offset %d fileSize %d: %v", r.Name(), n, dataSize, offset, fileSize, err)
	}
	return dataSlice, err

}

// ReadBytes hydrates the needle from the bytes buffer, with only n.Id is set.
func (n *Needle) ReadBytes(bytes []byte, offset int64, size Size, version Version) (err error) {
	n.ParseNeedleHeader(bytes)
	if n.Size != size {
		// cookie is not always passed in for this API. Use size to do preliminary checking.
		if OffsetSize == 4 && offset < int64(MaxPossibleVolumeSize) {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorSizeMismatchOffsetSize).Inc()
			glog.Errorf("entry not found1: offset %d found id %x size %d, expected size %d", offset, n.Id, n.Size, size)
			return ErrorSizeMismatch
		}
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorSizeMismatch).Inc()
		return fmt.Errorf("entry not found: offset %d found id %x size %d, expected size %d", offset, n.Id, n.Size, size)
	}
	switch version {
	case Version1:
		n.Data = bytes[NeedleHeaderSize : NeedleHeaderSize+size]
	case Version2, Version3:
		err = n.readNeedleDataVersion2(bytes[NeedleHeaderSize : NeedleHeaderSize+int(n.Size)])
	}
	if err != nil && err != io.EOF {
		return err
	}
	if size > 0 {
		checksum := util.BytesToUint32(bytes[NeedleHeaderSize+size : NeedleHeaderSize+size+NeedleChecksumSize])
		newChecksum := NewCRC(n.Data)
		if checksum != newChecksum.Value() && checksum != uint32(newChecksum) {
			// the crc.Value() function is to be deprecated. this double checking is for backward compatibility
			// with seaweed version using crc.Value() instead of uint32(crc), which appears in commit 056c480eb
			// and switch appeared in version 3.09.
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorCRC).Inc()
			return errors.New("CRC error! Data On Disk Corrupted")
		}
		n.Checksum = newChecksum
	}
	if version == Version3 {
		tsOffset := NeedleHeaderSize + size + NeedleChecksumSize
		n.AppendAtNs = util.BytesToUint64(bytes[tsOffset : tsOffset+TimestampSize])
	}
	return nil
}

// ReadData hydrates the needle from the file, with only n.Id is set.
func (n *Needle) ReadData(r backend.BackendStorageFile, offset int64, size Size, version Version) (err error) {
	bytes, err := ReadNeedleBlob(r, offset, size, version)
	if err != nil {
		return err
	}

	err = n.ReadBytes(bytes, offset, size, version)
	if err == ErrorSizeMismatch && OffsetSize == 4 {
		offset = offset + int64(MaxPossibleVolumeSize)
		bytes, err = ReadNeedleBlob(r, offset, size, version)
		if err != nil {
			return err
		}
		err = n.ReadBytes(bytes, offset, size, version)
	}
	return err
}

func (n *Needle) ParseNeedleHeader(bytes []byte) {
	n.Cookie = BytesToCookie(bytes[0:CookieSize])
	n.Id = BytesToNeedleId(bytes[CookieSize : CookieSize+NeedleIdSize])
	n.Size = BytesToSize(bytes[CookieSize+NeedleIdSize : NeedleHeaderSize])
}

func (n *Needle) readNeedleDataVersion2(bytes []byte) (err error) {
	index, lenBytes := 0, len(bytes)
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
		if int(n.DataSize)+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return fmt.Errorf("index out of range %d", 1)
		}
		n.Data = bytes[index : index+int(n.DataSize)]
		index = index + int(n.DataSize)
	}
	_, err = n.readNeedleDataVersion2NonData(bytes[index:])
	return
}
func (n *Needle) readNeedleDataVersion2NonData(bytes []byte) (index int, err error) {
	lenBytes := len(bytes)
	if index < lenBytes {
		n.Flags = bytes[index]
		index = index + 1
	}
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		if int(n.NameSize)+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 2)
		}
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	if index < lenBytes && n.HasMime() {
		n.MimeSize = uint8(bytes[index])
		index = index + 1
		if int(n.MimeSize)+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 3)
		}
		n.Mime = bytes[index : index+int(n.MimeSize)]
		index = index + int(n.MimeSize)
	}
	if index < lenBytes && n.HasLastModifiedDate() {
		if LastModifiedBytesLength+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 4)
		}
		n.LastModified = util.BytesToUint64(bytes[index : index+LastModifiedBytesLength])
		index = index + LastModifiedBytesLength
	}
	if index < lenBytes && n.HasTtl() {
		if TtlBytesLength+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 5)
		}
		n.Ttl = LoadTTLFromBytes(bytes[index : index+TtlBytesLength])
		index = index + TtlBytesLength
	}
	if index < lenBytes && n.HasPairs() {
		if 2+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 6)
		}
		n.PairsSize = util.BytesToUint16(bytes[index : index+2])
		index += 2
		if int(n.PairsSize)+index > lenBytes {
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorIndexOutOfRange).Inc()
			return index, fmt.Errorf("index out of range %d", 7)
		}
		end := index + int(n.PairsSize)
		n.Pairs = bytes[index:end]
		index = end
	}
	return index, nil
}

func ReadNeedleHeader(r backend.BackendStorageFile, version Version, offset int64) (n *Needle, bytes []byte, bodyLength int64, err error) {
	n = new(Needle)
	if version == Version1 || version == Version2 || version == Version3 {
		bytes = make([]byte, NeedleHeaderSize)

		var count int
		count, err = r.ReadAt(bytes, offset)
		if err == io.EOF && count == NeedleHeaderSize {
			err = nil
		}
		if count <= 0 || err != nil {
			return nil, bytes, 0, err
		}

		n.ParseNeedleHeader(bytes)
		bodyLength = NeedleBodyLength(n.Size, version)
	}

	return
}

func PaddingLength(needleSize Size, version Version) Size {
	if version == Version3 {
		// this is same value as version2, but just listed here for clarity
		return NeedlePaddingSize - ((NeedleHeaderSize + needleSize + NeedleChecksumSize + TimestampSize) % NeedlePaddingSize)
	}
	return NeedlePaddingSize - ((NeedleHeaderSize + needleSize + NeedleChecksumSize) % NeedlePaddingSize)
}

func NeedleBodyLength(needleSize Size, version Version) int64 {
	if version == Version3 {
		return int64(needleSize) + NeedleChecksumSize + TimestampSize + int64(PaddingLength(needleSize, version))
	}
	return int64(needleSize) + NeedleChecksumSize + int64(PaddingLength(needleSize, version))
}

// n should be a needle already read the header
// the input stream will read until next file entry
func (n *Needle) ReadNeedleBody(r backend.BackendStorageFile, version Version, offset int64, bodyLength int64) (bytes []byte, err error) {

	if bodyLength <= 0 {
		return nil, nil
	}
	bytes = make([]byte, bodyLength)
	readCount, err := r.ReadAt(bytes, offset)
	if err == io.EOF && int64(readCount) == bodyLength {
		err = nil
	}
	if err != nil {
		glog.Errorf("%s read %d bodyLength %d offset %d: %v", r.Name(), readCount, bodyLength, offset, err)
		return
	}

	err = n.ReadNeedleBodyBytes(bytes, version)

	return
}

func (n *Needle) ReadNeedleBodyBytes(needleBody []byte, version Version) (err error) {

	if len(needleBody) <= 0 {
		return nil
	}
	switch version {
	case Version1:
		n.Data = needleBody[:n.Size]
		n.Checksum = NewCRC(n.Data)
	case Version2, Version3:
		err = n.readNeedleDataVersion2(needleBody[0:n.Size])
		n.Checksum = NewCRC(n.Data)

		if version == Version3 {
			tsOffset := n.Size + NeedleChecksumSize
			n.AppendAtNs = util.BytesToUint64(needleBody[tsOffset : tsOffset+TimestampSize])
		}
	default:
		err = fmt.Errorf("unsupported version %d!", version)
	}
	return
}

func (n *Needle) IsCompressed() bool {
	return n.Flags&FlagIsCompressed > 0
}
func (n *Needle) SetIsCompressed() {
	n.Flags = n.Flags | FlagIsCompressed
}
func (n *Needle) HasName() bool {
	return n.Flags&FlagHasName > 0
}
func (n *Needle) SetHasName() {
	n.Flags = n.Flags | FlagHasName
}
func (n *Needle) HasMime() bool {
	return n.Flags&FlagHasMime > 0
}
func (n *Needle) SetHasMime() {
	n.Flags = n.Flags | FlagHasMime
}
func (n *Needle) HasLastModifiedDate() bool {
	return n.Flags&FlagHasLastModifiedDate > 0
}
func (n *Needle) SetHasLastModifiedDate() {
	n.Flags = n.Flags | FlagHasLastModifiedDate
}
func (n *Needle) HasTtl() bool {
	return n.Flags&FlagHasTtl > 0
}
func (n *Needle) SetHasTtl() {
	n.Flags = n.Flags | FlagHasTtl
}

func (n *Needle) IsChunkedManifest() bool {
	return n.Flags&FlagIsChunkManifest > 0
}

func (n *Needle) SetIsChunkManifest() {
	n.Flags = n.Flags | FlagIsChunkManifest
}

func (n *Needle) HasPairs() bool {
	return n.Flags&FlagHasPairs != 0
}

func (n *Needle) SetHasPairs() {
	n.Flags = n.Flags | FlagHasPairs
}

func GetActualSize(size Size, version Version) int64 {
	return NeedleHeaderSize + NeedleBodyLength(size, version)
}
