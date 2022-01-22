package mem

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"sync"
	"sync/atomic"
)

var pools []*sync.Pool

const (
	min_size = 1024
)

func bitCount(size int) (count int) {
	for ; size > min_size; count++ {
		size = (size + 1) >> 1
	}
	return
}

func init() {
	// 1KB ~ 256MB
	pools = make([]*sync.Pool, bitCount(1024*1024*256))
	for i := 0; i < len(pools); i++ {
		slotSize := 1024 << i
		pools[i] = &sync.Pool{
			New: func() interface{} {
				buffer := make([]byte, slotSize)
				return &buffer
			},
		}
	}
}

func getSlotPool(size int) *sync.Pool {
	index := bitCount(size)
	return pools[index]
}

var total int64

func Allocate(size int) []byte {
	newVal := atomic.AddInt64(&total, 1)
	glog.V(4).Infof("++> %d", newVal)
	slab := *getSlotPool(size).Get().(*[]byte)
	return slab[:size]
}

func Free(buf []byte) {
	newVal := atomic.AddInt64(&total, -1)
	glog.V(4).Infof("--> %d", newVal)
	getSlotPool(cap(buf)).Put(&buf)
}
