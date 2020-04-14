package chunk_cache

import (
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

const (
	memCacheSizeLimit = 1024 * 1024
)

// a global cache for recently accessed file chunks
type ChunkCache struct {
	memCache  *ChunkCacheInMemory
	diskCache *OnDiskCacheLayer
	sync.RWMutex
}

func NewChunkCache(maxEntries int64, dir string, diskSizeMB int64, segmentCount int) *ChunkCache {

	volumeCount, volumeSize := int(diskSizeMB/30000), int64(30000)
	if volumeCount < segmentCount {
		volumeCount, volumeSize = segmentCount, diskSizeMB/int64(segmentCount)
	}

	c := &ChunkCache{
		memCache:  NewChunkCacheInMemory(maxEntries),
		diskCache: NewOnDiskCacheLayer(dir, "cache", volumeCount, volumeSize),
	}

	return c
}

func (c *ChunkCache) GetChunk(fileId string, chunkSize uint64) (data []byte) {
	if c == nil {
		return
	}

	c.RLock()
	defer c.RUnlock()

	return c.doGetChunk(fileId, chunkSize)
}

func (c *ChunkCache) doGetChunk(fileId string, chunkSize uint64) (data []byte) {

	if chunkSize < memCacheSizeLimit {
		if data = c.memCache.GetChunk(fileId); data != nil {
			return data
		}
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return nil
	}

	return c.diskCache.getChunk(fid.Key)

}

func (c *ChunkCache) SetChunk(fileId string, data []byte) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()

	c.doSetChunk(fileId, data)
}

func (c *ChunkCache) doSetChunk(fileId string, data []byte) {

	if len(data) < memCacheSizeLimit {
		c.memCache.SetChunk(fileId, data)
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return
	}

	c.diskCache.setChunk(fid.Key, data)

}

func (c *ChunkCache) Shutdown() {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.diskCache.shutdown()
}
