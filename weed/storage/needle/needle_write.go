package needle

import (
	"bytes"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/buffer_pool"
)

func (n *Needle) Append(w backend.BackendStorageFile, version Version) (offset uint64, size Size, actualSize int64, err error) {
	end, _, e := w.GetStat()
	if e != nil {
		err = fmt.Errorf("Cannot Read Current Volume Position: %w", e)
		return
	}
	offset = uint64(end)
	if offset >= MaxPossibleVolumeSize && len(n.Data) != 0 {
		err = fmt.Errorf("Volume Size %d Exceeded %d", offset, MaxPossibleVolumeSize)
		return
	}
	bytesBuffer := buffer_pool.SyncPoolGetBuffer()
	defer func() {
		if err != nil {
			if te := w.Truncate(end); te != nil {
				// handle error or log
			}
		}
		buffer_pool.SyncPoolPutBuffer(bytesBuffer)
	}()

	size, actualSize, err = writeNeedleByVersion(version, n, offset, bytesBuffer)
	if err != nil {
		return
	}

	_, err = w.WriteAt(bytesBuffer.Bytes(), int64(offset))
	if err != nil {
		err = fmt.Errorf("failed to write %d bytes to %s at offset %d: %w", actualSize, w.Name(), offset, err)
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
		// compute byte offset as int to compare and slice correctly
		tsOffset := int(NeedleHeaderSize) + int(size) + NeedleChecksumSize
		// Ensure dataSlice has enough capacity for the timestamp
		if tsOffset+TimestampSize > len(dataSlice) {
			err = fmt.Errorf("needle blob buffer too small: need %d bytes, have %d", tsOffset+TimestampSize, len(dataSlice))
			return
		}
		util.Uint64toBytes(dataSlice[tsOffset:tsOffset+TimestampSize], appendAtNs)
	}

	if err == nil {
		_, err = w.WriteAt(dataSlice, int64(offset))
	}

	return

}

// prepareNeedleWrite encapsulates the common beginning logic for all versioned writeNeedle functions.
func prepareNeedleWrite(w backend.BackendStorageFile, n *Needle) (offset uint64, bytesBuffer *bytes.Buffer, cleanup func(err error), err error) {
	end, _, e := w.GetStat()
	if e != nil {
		err = fmt.Errorf("Cannot Read Current Volume Position: %w", e)
		return
	}
	offset = uint64(end)
	if offset >= MaxPossibleVolumeSize && len(n.Data) != 0 {
		err = fmt.Errorf("Volume Size %d Exceeded %d", offset, MaxPossibleVolumeSize)
		return
	}
	bytesBuffer = buffer_pool.SyncPoolGetBuffer()
	cleanup = func(err error) {
		if err != nil {
			if te := w.Truncate(end); te != nil {
				// handle error or log
			}
		}
		buffer_pool.SyncPoolPutBuffer(bytesBuffer)
	}
	return
}
