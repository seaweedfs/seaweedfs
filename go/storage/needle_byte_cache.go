package storage

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/hashicorp/golang-lru"

	"github.com/chrislusf/seaweedfs/go/util"
)

var (
	bytesCache *lru.Cache
	bytesPool  *util.BytesPool
)

/*
There are one level of caching, and one level of pooling.

In pooling, all []byte are fetched and returned to the pool bytesPool.

In caching, the string~[]byte mapping is cached, to
*/
func init() {
	bytesPool = util.NewBytesPool()
	bytesCache, _ = lru.NewWithEvict(1, func(key interface{}, value interface{}) {
		value.(*Block).decreaseReference()
	})
}

type Block struct {
	Bytes    []byte
	refCount int32
}

func (block *Block) decreaseReference() {
	if atomic.AddInt32(&block.refCount, -1) == 0 {
		bytesPool.Put(block.Bytes)
	}
}
func (block *Block) increaseReference() {
	atomic.AddInt32(&block.refCount, 1)
}

// get bytes from the LRU cache of []byte first, then from the bytes pool
// when []byte in LRU cache is evicted, it will be put back to the bytes pool
func getBytesForFileBlock(r *os.File, offset int64, readSize int) (block *Block, isNew bool) {
	// check cache, return if found
	cacheKey := fmt.Sprintf("%d:%d:%d", r.Fd(), offset>>3, readSize)
	if obj, found := bytesCache.Get(cacheKey); found {
		block = obj.(*Block)
		block.increaseReference()
		return block, false
	}

	// get the []byte from pool
	b := bytesPool.Get(readSize)
	// refCount = 2, one by the bytesCache, one by the actual needle object
	block = &Block{Bytes: b, refCount: 2}
	bytesCache.Add(cacheKey, block)
	return block, true
}

func (n *Needle) ReleaseMemory() {
	n.rawBlock.decreaseReference()
}
func ReleaseBytes(b []byte) {
	bytesPool.Put(b)
}
