package needle

import (
	"bytes"
	"math"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func writeNeedleV3(n *Needle, offset uint64, bytesBuffer *bytes.Buffer) (size Size, actualSize int64, err error) {
	bytesBuffer.Reset()
	header := make([]byte, NeedleHeaderSize+TimestampSize)
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
	bytesBuffer.Write(header[0:NeedleHeaderSize])
	if n.DataSize > 0 {
		util.Uint32toBytes(header[0:4], n.DataSize)
		bytesBuffer.Write(header[0:4])
		bytesBuffer.Write(n.Data)
		util.Uint8toBytes(header[0:1], n.Flags)
		bytesBuffer.Write(header[0:1])
		if n.HasName() {
			util.Uint8toBytes(header[0:1], n.NameSize)
			bytesBuffer.Write(header[0:1])
			bytesBuffer.Write(n.Name[:n.NameSize])
		}
		if n.HasMime() {
			util.Uint8toBytes(header[0:1], n.MimeSize)
			bytesBuffer.Write(header[0:1])
			bytesBuffer.Write(n.Mime)
		}
		if n.HasLastModifiedDate() {
			util.Uint64toBytes(header[0:8], n.LastModified)
			bytesBuffer.Write(header[8-LastModifiedBytesLength : 8])
		}
		if n.HasTtl() && n.Ttl != nil {
			n.Ttl.ToBytes(header[0:TtlBytesLength])
			bytesBuffer.Write(header[0:TtlBytesLength])
		}
		if n.HasPairs() {
			util.Uint16toBytes(header[0:2], n.PairsSize)
			bytesBuffer.Write(header[0:2])
			bytesBuffer.Write(n.Pairs)
		}
	}
	padding := PaddingLength(n.Size, Version3)
	util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))
	util.Uint64toBytes(header[NeedleChecksumSize:NeedleChecksumSize+TimestampSize], n.AppendAtNs)
	bytesBuffer.Write(header[0 : NeedleChecksumSize+TimestampSize+padding])

	size = Size(n.DataSize)
	actualSize = GetActualSize(n.Size, Version3)
	return size, actualSize, nil
}
