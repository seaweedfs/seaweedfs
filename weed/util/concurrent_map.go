package util

import (
	"errors"
	"sync"
)

// A mostly for read map, which can thread-safely
// initialize the map entries.
type ConcurrentMap struct {
	mutex sync.RWMutex
	items map[string]interface{}
}

func NewConcurrentMap() *ConcurrentMap {
	return &ConcurrentMap{items: make(map[string]interface{})}
}

func (m *ConcurrentMap) initMapEntry(key string, newEntry func() interface{}) (value interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if value, ok := m.items[key]; ok {
		return value
	}
	value = newEntry()
	m.items[key] = value
	return value
}

func (m *ConcurrentMap) GetOrNew(key string, newEntry func() interface{}) interface{} {
	m.mutex.RLock()
	value, ok := m.items[key]
	m.mutex.RUnlock()
	if ok {
		return value
	}
	return m.initMapEntry(key, newEntry)
}

func (m *ConcurrentMap) Get(key string) (item interface{}, ok bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	item, ok = m.items[key]
	return
}

func (m *ConcurrentMap) Has(key string) bool {
	m.mutex.RLock()
	_, ok := m.items[key]
	m.mutex.RUnlock()
	return ok
}

func (m *ConcurrentMap) Delete(key string) {
	m.mutex.Lock()
	delete(m.items, key)
	m.mutex.Unlock()
}

func (m *ConcurrentMap) Size() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.items)
}

// Wipes all items from the map
func (m *ConcurrentMap) Flush() int {
	m.mutex.Lock()
	size := len(m.items)
	m.items = make(map[string]interface{})
	m.mutex.Unlock()
	return size
}

var ErrBreakWalk = errors.New("Break walk.")

// break walk when walker fuc return an error
type MapWalker func(k string, v interface{}) (e error)

// MUST NOT add or delete item in walker
func (m *ConcurrentMap) Walk(mw MapWalker) (e error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for k, v := range m.items {
		if e = mw(k, v); e != nil {
			return e
		}
	}
	return
}

func (m *ConcurrentMap) Keys() (keys []string) {
	m.mutex.RLock()
	keys = make([]string, 0, len(m.items))
	for key := range m.items {
		keys = append(keys, key)
	}
	m.mutex.RUnlock()
	return
}

// Item is a pair of key and value
type Item struct {
	Key   string
	Value interface{}
}

// Return a channel from which each item (key:value pair) in the map can be read
// You can't break the iterator
func (m *ConcurrentMap) IterItems() <-chan Item {
	ch := make(chan Item)
	go func() {
		m.mutex.RLock()
		for key, value := range m.items {
			ch <- Item{key, value}
		}
		m.mutex.RUnlock()
		close(ch)
	}()
	return ch
}
