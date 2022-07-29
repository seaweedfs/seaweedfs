package needle_map

import (
	"github.com/google/btree"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type NeedleValue struct {
	Key    NeedleId
	Offset Offset `comment:"Volume offset"` //since aligned to 8 bytes, range is 4G*8=32G
	Size   Size   `comment:"Size of the data portion"`
}

func (this NeedleValue) Less(than btree.Item) bool {
	that := than.(NeedleValue)
	return this.Key < that.Key
}

func (nv NeedleValue) ToBytes() []byte {
	return ToBytes(nv.Key, nv.Offset, nv.Size)
}

func ToBytes(key NeedleId, offset Offset, size Size) []byte {
	bytes := make([]byte, NeedleIdSize+OffsetSize+SizeSize)
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)
	OffsetToBytes(bytes[NeedleIdSize:NeedleIdSize+OffsetSize], offset)
	util.Uint32toBytes(bytes[NeedleIdSize+OffsetSize:NeedleIdSize+OffsetSize+SizeSize], uint32(size))
	return bytes
}
