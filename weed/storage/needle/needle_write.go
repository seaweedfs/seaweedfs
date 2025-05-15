package needle

import (
	"bytes"
	"fmt"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
)

func (n *Needle) prepareWriteBuffer(version Version, writeBytes *bytes.Buffer) (Size, int64, error) {
	writeBytes.Reset()
	switch version {
	case Version1:
		header := make([]byte, NeedleHeaderSize)
		CookieToBytes(header[0:CookieSize], n.Cookie)
		NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], n.Id)
		n.Size = Size(len(n.Data))
		SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], n.Size)
		size := n.Size
		actualSize := NeedleHeaderSize + int64(n.Size)
		writeBytes.Write(header)
		writeBytes.Write(n.Data)
		padding := PaddingLength(n.Size, version)
		util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))
		writeBytes.Write(header[0 : NeedleChecksumSize+padding])
		return size, actualSize, nil
	case Version2, Version3:
		header := make([]byte, NeedleHeaderSize+TimestampSize) // adding timestamp to reuse it and avoid extra allocation
		CookieToBytes(header[0:CookieSize], n.Cookie)
		NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], n.Id)
		if len(n.Name) >= math.MaxUint8 {
			n.NameSize = math.MaxUint8
		} else {
			n.NameSize = uint8(len(n.Name))
		}
		n.DataSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Mime))
		if n.DataSize > 0 {
			n.Size = 4 + Size(n.DataSize) + 1
			if n.HasName() {
				n.Size = n.Size + 1 + Size(n.NameSize)
			}
			if n.HasMime() {
				n.Size = n.Size + 1 + Size(n.MimeSize)
			}
			if n.HasLastModifiedDate() {
				n.Size = n.Size + LastModifiedBytesLength
			}
			if n.HasTtl() {
				n.Size = n.Size + TtlBytesLength
			}
			if n.HasPairs() {
				n.Size += 2 + Size(n.PairsSize)
			}
		} else {
			n.Size = 0
		}
		SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], n.Size)
		writeBytes.Write(header[0:NeedleHeaderSize])
		if n.DataSize > 0 {
			util.Uint32toBytes(header[0:4], n.DataSize)
			writeBytes.Write(header[0:4])
			writeBytes.Write(n.Data)
			util.Uint8toBytes(header[0:1], n.Flags)
			writeBytes.Write(header[0:1])
			if n.HasName() {
				util.Uint8toBytes(header[0:1], n.NameSize)
				writeBytes.Write(header[0:1])
				writeBytes.Write(n.Name[:n.NameSize])
			}
			if n.HasMime() {
				util.Uint8toBytes(header[0:1], n.MimeSize)
				writeBytes.Write(header[0:1])
				writeBytes.Write(n.Mime)
			}
			if n.HasLastModifiedDate() {
				util.Uint64toBytes(header[0:8], n.LastModified)
				writeBytes.Write(header[8-LastModifiedBytesLength : 8])
			}
			if n.HasTtl() && n.Ttl != nil {
				n.Ttl.ToBytes(header[0:TtlBytesLength])
				writeBytes.Write(header[0:TtlBytesLength])
			}
			if n.HasPairs() {
				util.Uint16toBytes(header[0:2], n.PairsSize)
				writeBytes.Write(header[0:2])
				writeBytes.Write(n.Pairs)
			}
		}
		padding := PaddingLength(n.Size, version)
		util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))
		if version == Version2 {
			writeBytes.Write(header[0 : NeedleChecksumSize+padding])
		} else {
			// version3
			util.Uint64toBytes(header[NeedleChecksumSize:NeedleChecksumSize+TimestampSize], n.AppendAtNs)
			writeBytes.Write(header[0 : NeedleChecksumSize+TimestampSize+padding])
		}

		return Size(n.DataSize), GetActualSize(n.Size, version), nil
	}

	return 0, 0, fmt.Errorf("Unsupported Version! (%d)", version)
}

func (n *Needle) Append(w backend.BackendStorageFile, version Version) (offset uint64, size Size, actualSize int64, err error) {

	if end, _, e := w.GetStat(); e == nil {
		defer func(w backend.BackendStorageFile, off int64) {
			if err != nil {
				if te := w.Truncate(end); te != nil {
					glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", w.Name(), end, te)
				}
			}
		}(w, end)
		offset = uint64(end)
	} else {
		err = fmt.Errorf("Cannot Read Current Volume Position: %w", e)
		return
	}
	if offset >= MaxPossibleVolumeSize && len(n.Data) != 0 {
		err = fmt.Errorf("Volume Size %d Exceeded %d", offset, MaxPossibleVolumeSize)
		return
	}

	bytesBuffer := buffer_pool.SyncPoolGetBuffer()
	defer buffer_pool.SyncPoolPutBuffer(bytesBuffer)

	size, actualSize, err = n.prepareWriteBuffer(version, bytesBuffer)

	if err == nil {
		_, err = w.WriteAt(bytesBuffer.Bytes(), int64(offset))
		if err != nil {
			err = fmt.Errorf("failed to write %d bytes to %s at offset %d: %w", actualSize, w.Name(), offset, err)
		}
	}

	return offset, size, actualSize, err
}

func WriteNeedleBlob(w backend.BackendStorageFile, dataSlice []byte, size Size, appendAtNs uint64, version Version) (offset uint64, err error) {

	if end, _, e := w.GetStat(); e == nil {
		defer func(w backend.BackendStorageFile, off int64) {
			if err != nil {
				if te := w.Truncate(end); te != nil {
					glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", w.Name(), end, te)
				}
			}
		}(w, end)
		offset = uint64(end)
	} else {
		err = fmt.Errorf("Cannot Read Current Volume Position: %v", e)
		return
	}

	if version == Version3 {
		tsOffset := NeedleHeaderSize + size + NeedleChecksumSize
		util.Uint64toBytes(dataSlice[tsOffset:tsOffset+TimestampSize], appendAtNs)
	}

	if err == nil {
		_, err = w.WriteAt(dataSlice, int64(offset))
	}

	return

}
