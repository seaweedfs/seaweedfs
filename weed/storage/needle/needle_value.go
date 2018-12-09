package needle

import (
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/google/btree"
)

type NeedleValue struct {
	Key    NeedleId
	Offset Offset `comment:"Volume offset"` //since aligned to 8 bytes, range is 4G*8=32G
	Size   uint32 `comment:"Size of the data portion"`
}

func (this NeedleValue) Less(than btree.Item) bool {
	that := than.(NeedleValue)
	return this.Key < that.Key
}
