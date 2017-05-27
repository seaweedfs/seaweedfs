package needle

import (
	"strconv"

	"github.com/google/btree"
)

const (
	batch = 100000
)

type NeedleValue struct {
	Key    Key
	Offset uint32 `comment:"Volume offset"` //since aligned to 8 bytes, range is 4G*8=32G
	Size   uint32 `comment:"Size of the data portion"`
}

func (this NeedleValue) Less(than btree.Item) bool {
	that := than.(NeedleValue)
	return this.Key < that.Key
}

type Key uint64

func (k Key) String() string {
	return strconv.FormatUint(uint64(k), 10)
}
