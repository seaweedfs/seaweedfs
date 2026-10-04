package gateway

import (
	"container/list"
	"sync"
)

type result struct {
	data              []byte
	contentType, etag string
}

type cacheEntry struct {
	key   string
	value *result
	size  int64
}

// cache 是按字节淘汰的 LRU，另设条数上限以限制小对象的元数据开销。
type cache struct {
	mu             sync.Mutex
	entries        map[string]*list.Element
	order          *list.List
	capacity, used int64
}

// newCache 创建可丢弃的进程内缓存，不创建任何存储对象。
func newCache(capacity int64) *cache {
	return &cache{entries: make(map[string]*list.Element), order: list.New(), capacity: capacity}
}

// get 返回不可变结果，读取时更新最近使用顺序。
func (c *cache) get(key string) *result {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.order.MoveToFront(e)
		return e.Value.(*cacheEntry).value
	}
	return nil
}

// put 按实际数据及索引大小计费，超过容量的单张图片不入缓存。
func (c *cache) put(key string, value *result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	size := int64(len(key) + len(value.data) + len(value.contentType) + len(value.etag) + 128)
	if c.capacity == 0 || size > c.capacity {
		return
	}
	if old := c.entries[key]; old != nil {
		c.used -= old.Value.(*cacheEntry).size
		c.order.Remove(old)
		delete(c.entries, key)
	}
	for c.used+size > c.capacity || len(c.entries) >= 1024 {
		old := c.order.Back()
		entry := old.Value.(*cacheEntry)
		delete(c.entries, entry.key)
		c.used -= entry.size
		c.order.Remove(old)
	}
	c.entries[key] = c.order.PushFront(&cacheEntry{key: key, value: value, size: size})
	c.used += size
}
