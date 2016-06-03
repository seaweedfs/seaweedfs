package util

import (
	"sync"
)

// A mostly for read map, which can thread-safely
// initialize the map entries.
type ConcurrentReadMap struct {
	sync.RWMutex

	items map[string]interface{}
}

func NewConcurrentReadMap() *ConcurrentReadMap {
	return &ConcurrentReadMap{items: make(map[string]interface{})}
}

func (m *ConcurrentReadMap) initMapEntry(key string, newEntry func() interface{}) (value interface{}) {
	m.Lock()
	defer m.Unlock()
	if value, ok := m.items[key]; ok {
		return value
	}
	value = newEntry()
	m.items[key] = value
	return value
}

func (m *ConcurrentReadMap) Get(key string, newEntry func() interface{}) interface{} {
	m.RLock()
	if value, ok := m.items[key]; ok {
		m.RUnlock()
		return value
	}
	m.RUnlock()
	return m.initMapEntry(key, newEntry)
}

func (m *ConcurrentReadMap) Find(key string) (interface{}, bool) {
	m.RLock()
	value, ok := m.items[key]
	m.RUnlock()
	return value, ok
}

func (m *ConcurrentReadMap) Items() (itemsCopy []interface{}) {
	m.RLock()
	for _, i := range m.items {
		itemsCopy = append(itemsCopy, i)
	}
	m.RUnlock()
	return itemsCopy
}

func (m *ConcurrentReadMap) Delete(key string) {
	m.Lock()
	delete(m.items, key)
	m.Unlock()
}
