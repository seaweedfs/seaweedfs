package mem

import "sync"

var pools []*sync.Pool

const (
	min_size = 1024
)

func bitCount(size int) (count int) {
	for ; size > min_size; count++ {
		size = size >> 1
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

func Allocate(size int) []byte {
	slab := *getSlotPool(size).Get().(*[]byte)
	return slab[:size]
}

func Free(buf []byte) {
	getSlotPool(cap(buf)).Put(&buf)
}
