package chunk_cache

import (
	"time"

	"github.com/karlseguin/ccache"
)

// a global cache for recently accessed file chunks
type ChunkCache struct {
	cache *ccache.Cache
}

func NewChunkCache(maxEntries int64) *ChunkCache {
	pruneCount := maxEntries >> 3
	if pruneCount <= 0 {
		pruneCount = 500
	}
	return &ChunkCache{
		cache: ccache.New(ccache.Configure().MaxSize(maxEntries).ItemsToPrune(uint32(pruneCount))),
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
