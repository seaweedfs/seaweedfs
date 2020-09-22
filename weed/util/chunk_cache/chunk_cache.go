package chunk_cache

import (
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

const (
	memCacheSizeLimit     = 1024 * 1024
	onDiskCacheSizeLimit0 = memCacheSizeLimit
	onDiskCacheSizeLimit1 = 4 * memCacheSizeLimit
)

type ChunkCache interface {
	GetChunk(fileId string, minSize uint64) (data []byte)
	SetChunk(fileId string, data []byte)
}

// a global cache for recently accessed file chunks
type TieredChunkCache struct {
	memCache   *ChunkCacheInMemory
	diskCaches []*OnDiskCacheLayer
	sync.RWMutex
}

func NewTieredChunkCache(maxEntries int64, dir string, diskSizeMB int64) *TieredChunkCache {

	c := &TieredChunkCache{
		memCache: NewChunkCacheInMemory(maxEntries),
	}
	c.diskCaches = make([]*OnDiskCacheLayer, 3)
	c.diskCaches[0] = NewOnDiskCacheLayer(dir, "c0_1", diskSizeMB/4, 4)
	c.diskCaches[1] = NewOnDiskCacheLayer(dir, "c1_4", diskSizeMB/4, 4)
	c.diskCaches[2] = NewOnDiskCacheLayer(dir, "cache", diskSizeMB/2, 4)

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

	if minSize < memCacheSizeLimit {
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

	if minSize < onDiskCacheSizeLimit0 {
		data = c.diskCaches[0].getChunk(fid.Key)
		if len(data) >= int(minSize) {
			return data
		}
	}
	if minSize < onDiskCacheSizeLimit1 {
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

	if len(data) < memCacheSizeLimit {
		c.memCache.SetChunk(fileId, data)
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return
	}

	if len(data) < onDiskCacheSizeLimit0 {
		c.diskCaches[0].setChunk(fid.Key, data)
	} else if len(data) < onDiskCacheSizeLimit1 {
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
