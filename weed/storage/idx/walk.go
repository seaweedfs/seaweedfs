package idx

import (
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

// walks through the index file, calls fn function with each key, offset, size
// stops with the error returned by the fn function
func WalkIndexFile(r *os.File, fn func(key types.NeedleId, offset types.Offset, size uint32) error) error {
	var readerOffset int64
	bytes := make([]byte, types.NeedleMapEntrySize*RowsToRead)
	count, e := r.ReadAt(bytes, readerOffset)
	glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
	readerOffset += int64(count)
	var (
		key    types.NeedleId
		offset types.Offset
		size   uint32
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
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		readerOffset += int64(count)
	}
	return e
}

func IdxFileEntry(bytes []byte) (key types.NeedleId, offset types.Offset, size uint32) {
	key = types.BytesToNeedleId(bytes[:types.NeedleIdSize])
	offset = types.BytesToOffset(bytes[types.NeedleIdSize : types.NeedleIdSize+types.OffsetSize])
	size = util.BytesToUint32(bytes[types.NeedleIdSize+types.OffsetSize : types.NeedleIdSize+types.OffsetSize+types.SizeSize])
	return
}

const (
	RowsToRead = 1024
)
