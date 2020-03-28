package pb_cache

import (
	"time"

	"github.com/karlseguin/ccache"
)

// a global cache for recently accessed file chunks
type ChunkCache struct {
	cache *ccache.Cache
}

func NewChunkCache() *ChunkCache {
	return &ChunkCache{
		cache: ccache.New(ccache.Configure().MaxSize(1000).ItemsToPrune(100)),
	}
}

func (c *ChunkCache) GetChunk(fileId string) []byte {
	item := c.cache.Get(fileId)
	if item == nil {
		return nil
	}
	data := item.Value().([]byte)
	item.Extend(time.Hour)
	return data
}

func (c *ChunkCache) SetChunk(fileId string, data []byte) {
	c.cache.Set(fileId, data, time.Hour)
}
