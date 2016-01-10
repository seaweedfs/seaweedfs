package util

import (
	"sync"
)

// A mostly for read map, which can thread-safely
// initialize the map entries.
type ConcurrentReadMap struct {
	rwmutex sync.RWMutex
	Items   map[string]interface{}
}

func NewConcurrentReadMap() *ConcurrentReadMap {
	return &ConcurrentReadMap{Items: make(map[string]interface{})}
}

func (m *ConcurrentReadMap) initMapEntry(key string, newEntry func() interface{}) (value interface{}) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()
	if value, ok := m.Items[key]; ok {
		return value
	}
	value = newEntry()
	m.Items[key] = value
	return value
}

func (m *ConcurrentReadMap) Get(key string, newEntry func() interface{}) interface{} {
	m.rwmutex.RLock()
	value, ok := m.Items[key]
	m.rwmutex.RUnlock()
	if ok {
		return value
	}
	return m.initMapEntry(key, newEntry)
}
