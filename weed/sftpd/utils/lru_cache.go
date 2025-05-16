package utils

import (
	"container/list"
)

type CacheEntry struct {
	key   int64
	value []byte
}

type LruCache struct {
	capacity int
	ll       *list.List
	cache    map[int64]*list.Element
}

func NewLRUCache(capacity int) *LruCache {
	return &LruCache{
		capacity: capacity,
		ll:       list.New(),
		cache:    make(map[int64]*list.Element),
	}
}

func (c *LruCache) Get(key int64) ([]byte, bool) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		return ele.Value.(*CacheEntry).value, true
	}
	return nil, false
}

func (c *LruCache) Put(key int64, value []byte) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		ele.Value.(*CacheEntry).value = value
		return
	}

	if c.ll.Len() >= c.capacity {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			delete(c.cache, oldest.Value.(*CacheEntry).key)
		}
	}

	entry := &CacheEntry{key, value}
	ele := c.ll.PushFront(entry)
	c.cache[key] = ele
}
