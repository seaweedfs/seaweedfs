package needle

import (
	"bytes"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func writeNeedleV1(n *Needle, offset uint64, bytesBuffer *bytes.Buffer) (size Size, actualSize int64, err error) {
	bytesBuffer.Reset()
	header := make([]byte, NeedleHeaderSize)
	CookieToBytes(header[0:CookieSize], n.Cookie)
	NeedleIdToBytes(header[CookieSize:CookieSize+NeedleIdSize], n.Id)
	n.Size = Size(len(n.Data))
	SizeToBytes(header[CookieSize+NeedleIdSize:CookieSize+NeedleIdSize+SizeSize], n.Size)
	size = n.Size
	actualSize = NeedleHeaderSize + int64(n.Size)
	bytesBuffer.Write(header)
	bytesBuffer.Write(n.Data)
	padding := PaddingLength(n.Size, Version1)
	util.Uint32toBytes(header[0:NeedleChecksumSize], uint32(n.Checksum))
	bytesBuffer.Write(header[0 : NeedleChecksumSize+padding])

	return size, actualSize, nil
}
