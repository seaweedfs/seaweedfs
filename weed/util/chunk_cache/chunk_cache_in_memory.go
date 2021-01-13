package chunk_cache

import (
	"time"

	"github.com/karlseguin/ccache/v2"
)

// a global cache for recently accessed file chunks
type ChunkCacheInMemory struct {
	cache *ccache.Cache
}

func NewChunkCacheInMemory(maxEntries int64) *ChunkCacheInMemory {
	pruneCount := maxEntries >> 3
	if pruneCount <= 0 {
		pruneCount = 500
	}
	return &ChunkCacheInMemory{
		cache: ccache.New(ccache.Configure().MaxSize(maxEntries).ItemsToPrune(uint32(pruneCount))),
	}
}

func (c *ChunkCacheInMemory) GetChunk(fileId string) []byte {
	item := c.cache.Get(fileId)
	if item == nil {
		return nil
	}
	data := item.Value().([]byte)
	item.Extend(time.Hour)
	return data
}

func (c *ChunkCacheInMemory) SetChunk(fileId string, data []byte) {
	c.cache.Set(fileId, data, time.Hour)
}
