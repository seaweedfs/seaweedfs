package util

import (
	"sync"
)

// A mostly for read map, which can thread-safely
// initialize the map entries.
type ConcurrentReadMap struct {
	rmutex sync.RWMutex
	mutex  sync.Mutex
	Items  map[string]interface{}
}

func NewConcurrentReadMap() *ConcurrentReadMap {
	return &ConcurrentReadMap{Items: make(map[string]interface{})}
}

func (m *ConcurrentReadMap) initMapEntry(key string, newEntry func() interface{}) (value interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if value, ok := m.Items[key]; ok {
		return value
	}
	value = newEntry()
	m.Items[key] = value
	return value
}

func (m *ConcurrentReadMap) Get(key string, newEntry func() interface{}) interface{} {
	m.rmutex.RLock()
	if value, ok := m.Items[key]; ok {
		m.rmutex.RUnlock()
		return value
	}
	m.rmutex.RUnlock()
	return m.initMapEntry(key, newEntry)
}
