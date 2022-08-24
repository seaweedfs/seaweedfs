package idx

import (
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// walks through the index file, calls fn function with each key, offset, size
// stops with the error returned by the fn function
func WalkIndexFile(r io.ReaderAt, startFrom uint64, fn func(key types.NeedleId, offset types.Offset, size types.Size) error) error {
	readerOffset := int64(startFrom * types.NeedleMapEntrySize)
	bytes := make([]byte, types.NeedleMapEntrySize*RowsToRead)
	count, e := r.ReadAt(bytes, readerOffset)
	if count == 0 && e == io.EOF {
		return nil
	}
	glog.V(3).Infof("readerOffset %d count %d err: %v", readerOffset, count, e)
	readerOffset += int64(count)
	var (
		key    types.NeedleId
		offset types.Offset
		size   types.Size
		i      int
	)

	for count > 0 && e == nil || e == io.EOF {
		for i = 0; i+types.NeedleMapEntrySize <= count; i += types.NeedleMapEntrySize {
			key, offset, size = IdxFileEntry(bytes[i : i+types.NeedleMapEntrySize])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		if e == io.EOF {
			return nil
		}
		count, e = r.ReadAt(bytes, readerOffset)
		glog.V(3).Infof("readerOffset %d count %d err: %v", readerOffset, count, e)
		readerOffset += int64(count)
	}
	return e
}

func IdxFileEntry(bytes []byte) (key types.NeedleId, offset types.Offset, size types.Size) {
	key = types.BytesToNeedleId(bytes[:types.NeedleIdSize])
	offset = types.BytesToOffset(bytes[types.NeedleIdSize : types.NeedleIdSize+types.OffsetSize])
	size = types.BytesToSize(bytes[types.NeedleIdSize+types.OffsetSize : types.NeedleIdSize+types.OffsetSize+types.SizeSize])
	return
}

const (
	RowsToRead = 1024
)
