package needle

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
)

func writeNeedleV1(w backend.BackendStorageFile, n *Needle) (offset uint64, size Size, actualSize int64, err error) {
	if end, _, e := w.GetStat(); e == nil {
		defer func(w backend.BackendStorageFile, off int64) {
			if err != nil {
				if te := w.Truncate(end); te != nil {
					// handle error
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

	_, err = w.WriteAt(bytesBuffer.Bytes(), int64(offset))
	if err != nil {
		err = fmt.Errorf("failed to write %d bytes to %s at offset %d: %w", actualSize, w.Name(), offset, err)
	}

	return offset, size, actualSize, err
}
