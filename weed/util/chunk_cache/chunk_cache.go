package chunk_cache

import (
	"fmt"
	"path"
	"sort"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

// a global cache for recently accessed file chunks
type ChunkCache struct {
	memCache   *ChunkCacheInMemory
	diskCaches []*ChunkCacheVolume
	sync.RWMutex
}

func NewChunkCache(maxEntries int64, dir string, diskSizeMB int64, segmentCount int) *ChunkCache {
	c := &ChunkCache{
		memCache: NewChunkCacheInMemory(maxEntries),
	}

	volumeCount, volumeSize := int(diskSizeMB/30000), int64(30000)
	if volumeCount < segmentCount {
		volumeCount, volumeSize = segmentCount, diskSizeMB/int64(segmentCount)
	}

	for i := 0; i < volumeCount; i++ {
		fileName := path.Join(dir, fmt.Sprintf("cache_%d", i))
		diskCache, err := LoadOrCreateChunkCacheVolume(fileName, volumeSize*1024*1024)
		if err != nil {
			glog.Errorf("failed to add cache %s : %v", fileName, err)
		} else {
			c.diskCaches = append(c.diskCaches, diskCache)
		}
	}

	// keep newest cache to the front
	sort.Slice(c.diskCaches, func(i, j int) bool {
		return c.diskCaches[i].lastModTime.After(c.diskCaches[j].lastModTime)
	})

	return c
}

func (c *ChunkCache) GetChunk(fileId string) (data []byte) {
	if c == nil {
		return
	}

	c.RLock()
	defer c.RUnlock()

	return c.doGetChunk(fileId)
}

func (c *ChunkCache) doGetChunk(fileId string) (data []byte) {
	if data = c.memCache.GetChunk(fileId); data != nil {
		return data
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return nil
	}
	for _, diskCache := range c.diskCaches {
		data, err = diskCache.GetNeedle(fid.Key)
		if err == storage.ErrorNotFound {
			continue
		}
		if err != nil {
			glog.Errorf("failed to read cache file %s id %s", diskCache.fileName, fileId)
			continue
		}
		if len(data) != 0 {
			return
		}
	}
	return nil
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

	c.memCache.SetChunk(fileId, data)

	if len(c.diskCaches) == 0 {
		return
	}

	if c.diskCaches[0].fileSize+int64(len(data)) > c.diskCaches[0].sizeLimit {
		t, resetErr := c.diskCaches[len(c.diskCaches)-1].Reset()
		if resetErr != nil {
			glog.Errorf("failed to reset cache file %s", c.diskCaches[len(c.diskCaches)-1].fileName)
			return
		}
		for i := len(c.diskCaches) - 1; i > 0; i-- {
			c.diskCaches[i] = c.diskCaches[i-1]
		}
		c.diskCaches[0] = t
	}

	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		glog.Errorf("failed to parse file id %s", fileId)
		return
	}
	c.diskCaches[0].WriteNeedle(fid.Key, data)

}

func (c *ChunkCache) Shutdown() {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	for _, diskCache := range c.diskCaches {
		diskCache.Shutdown()
	}
}
