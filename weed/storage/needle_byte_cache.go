package storage

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/hashicorp/golang-lru"

	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

var (
	bytesCache *lru.Cache
	bytesPool  *util.BytesPool
)

/*
There are one level of caching, and one level of pooling.

In pooling, all []byte are fetched and returned to the pool bytesPool.

In caching, the string~[]byte mapping is cached
*/
func init() {
	bytesPool = util.NewBytesPool()
	bytesCache, _ = lru.NewWithEvict(50, func(key interface{}, value interface{}) {
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
func getBytesForFileBlock(r *os.File, offset int64, readSize int) (dataSlice []byte, block *Block, err error) {
	//Skip the cache if we are looking for a block that is too big to fit in the cache (defaulting to 10MB)
	cacheable := readSize <= (1024*1024*10)
	if !cacheable {
		glog.V(4).Infoln("Block too big to keep in cache. Size:", readSize)
	}
	cacheKey := string("")
	if cacheable {
	// check cache, return if found
		cacheKey = fmt.Sprintf("%d:%d:%d", r.Fd(), offset >> 3, readSize)
	if obj, found := bytesCache.Get(cacheKey); found {
		glog.V(4).Infoln("Found block in cache. Size:", readSize)
		block = obj.(*Block)
		block.increaseReference()
		dataSlice = block.Bytes[0:readSize]
		return dataSlice, block, nil
		}
	}

	// get the []byte from pool
	b := bytesPool.Get(readSize)
	// refCount = 2, one by the bytesCache, one by the actual needle object
	refCount := int32(1)
	if cacheable {
		refCount = 2
	}
	block = &Block{Bytes: b, refCount: refCount}
	dataSlice = block.Bytes[0:readSize]
	_, err = r.ReadAt(dataSlice, offset)
	if cacheable {
	bytesCache.Add(cacheKey, block)
	}
	return dataSlice, block, err
}

func (n *Needle) ReleaseMemory() {
	if n.rawBlock != nil {
		n.rawBlock.decreaseReference()
	}
}
func ReleaseBytes(b []byte) {
	bytesPool.Put(b)
}
