package chunk_cache

import (
	"github.com/karlseguin/ccache/v2"
	"time"
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

func (c *ChunkCacheInMemory) getChunkSlice(fileId string, offset, length uint64) ([]byte, error) {
	item := c.cache.Get(fileId)
	if item == nil {
		return nil, nil
	}
	data := item.Value().([]byte)
	item.Extend(time.Hour)
	wanted := min(int(length), len(data)-int(offset))
	if wanted < 0 {
		return nil, ErrorOutOfBounds
	}
	return data[offset : int(offset)+wanted], nil
}

func (c *ChunkCacheInMemory) readChunkAt(buffer []byte, fileId string, offset uint64) (int, error) {
	item := c.cache.Get(fileId)
	if item == nil {
		return 0, nil
	}
	data := item.Value().([]byte)
	item.Extend(time.Hour)
	wanted := min(len(buffer), len(data)-int(offset))
	if wanted < 0 {
		return 0, ErrorOutOfBounds
	}
	n := copy(buffer, data[offset:int(offset)+wanted])
	return n, nil
}

func (c *ChunkCacheInMemory) SetChunk(fileId string, data []byte) {
	localCopy := make([]byte, len(data))
	copy(localCopy, data)
	c.cache.Set(fileId, localCopy, time.Hour)
}
