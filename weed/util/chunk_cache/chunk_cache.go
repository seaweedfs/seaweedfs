package chunk_cache

import (
	"errors"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

var ErrorOutOfBounds = errors.New("attempt to read out of bounds")

type ChunkCache interface {
	GetChunk(fileId string, minSize uint64) (data []byte)
	GetChunkSlice(fileId string, offset, length uint64) []byte
	SetChunk(fileId string, data []byte)
}

// a global cache for recently accessed file chunks
type TieredChunkCache struct {
	memCache   *ChunkCacheInMemory
	diskCaches []*OnDiskCacheLayer
	sync.RWMutex
	onDiskCacheSizeLimit0 uint64
	onDiskCacheSizeLimit1 uint64
	onDiskCacheSizeLimit2 uint64
}

var _ ChunkCache = &TieredChunkCache{}

func NewTieredChunkCache(maxEntries int64, dir string, diskSizeInUnit int64, unitSize int64) *TieredChunkCache {

	c := &TieredChunkCache{
		memCache: NewChunkCacheInMemory(maxEntries),
	}
	c.diskCaches = make([]*OnDiskCacheLayer, 3)
	c.onDiskCacheSizeLimit0 = uint64(unitSize)
	c.onDiskCacheSizeLimit1 = 4 * c.onDiskCacheSizeLimit0
	c.onDiskCacheSizeLimit2 = 2 * c.onDiskCacheSizeLimit1
	c.diskCaches[0] = NewOnDiskCacheLayer(dir, "c0_2", diskSizeInUnit*unitSize/8, 2)
	c.diskCaches[1] = NewOnDiskCacheLayer(dir, "c1_3", diskSizeInUnit*unitSize/4+diskSizeInUnit*unitSize/8, 3)
	c.diskCaches[2] = NewOnDiskCacheLayer(dir, "c2_2", diskSizeInUnit*unitSize/2, 2)

	return c
}

func (c *TieredChunkCache) GetChunk(fileId string, minSize uint64) (data []byte) {
	if c == nil {
		return
	}

	c.RLock()
	defer c.RUnlock()

	return c.doGetChunk(fileId, minSize)
}

func (c *TieredChunkCache) doGetChunk(fileId string, minSize uint64) (data []byte) {

	if minSize <= c.onDiskCacheSizeLimit0 {
		data = c.memCache.GetChunk(fileId)
		if len(data) >= int(minSize) {
			return data
		}
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return nil
	}

	if minSize <= c.onDiskCacheSizeLimit0 {
		data = c.diskCaches[0].getChunk(fid.Key)
		if len(data) >= int(minSize) {
			return data
		}
	}
	if minSize <= c.onDiskCacheSizeLimit1 {
		data = c.diskCaches[1].getChunk(fid.Key)
		if len(data) >= int(minSize) {
			return data
		}
	}
	{
		data = c.diskCaches[2].getChunk(fid.Key)
		if len(data) >= int(minSize) {
			return data
		}
	}

	return nil

}

func (c *TieredChunkCache) GetChunkSlice(fileId string, offset, length uint64) []byte {
	if c == nil {
		return nil
	}

	c.RLock()
	defer c.RUnlock()

	return c.doGetChunkSlice(fileId, offset, length)
}

func (c *TieredChunkCache) doGetChunkSlice(fileId string, offset, length uint64) (data []byte) {

	minSize := offset + length
	if minSize <= c.onDiskCacheSizeLimit0 {
		data, err := c.memCache.getChunkSlice(fileId, offset, length)
		if err != nil {
			glog.Errorf("failed to read from memcache: %s", err)
		}
		if len(data) >= int(minSize) {
			return data
		}
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return nil
	}

	if minSize <= c.onDiskCacheSizeLimit0 {
		data = c.diskCaches[0].getChunkSlice(fid.Key, offset, length)
		if len(data) >= int(minSize) {
			return data
		}
	}
	if minSize <= c.onDiskCacheSizeLimit1 {
		data = c.diskCaches[1].getChunkSlice(fid.Key, offset, length)
		if len(data) >= int(minSize) {
			return data
		}
	}
	{
		data = c.diskCaches[2].getChunkSlice(fid.Key, offset, length)
		if len(data) >= int(minSize) {
			return data
		}
	}

	return nil
}

func (c *TieredChunkCache) SetChunk(fileId string, data []byte) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()

	glog.V(4).Infof("SetChunk %s size %d\n", fileId, len(data))

	c.doSetChunk(fileId, data)
}

func (c *TieredChunkCache) doSetChunk(fileId string, data []byte) {

	if len(data) <= int(c.onDiskCacheSizeLimit0) {
		c.memCache.SetChunk(fileId, data)
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return
	}

	if len(data) <= int(c.onDiskCacheSizeLimit0) {
		c.diskCaches[0].setChunk(fid.Key, data)
	} else if len(data) <= int(c.onDiskCacheSizeLimit1) {
		c.diskCaches[1].setChunk(fid.Key, data)
	} else {
		c.diskCaches[2].setChunk(fid.Key, data)
	}

}

func (c *TieredChunkCache) Shutdown() {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	for _, diskCache := range c.diskCaches {
		diskCache.shutdown()
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
