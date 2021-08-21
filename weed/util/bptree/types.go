package bptree

import (
	"errors"
	"fmt"
)

type Equatable interface {
	Equals(b Equatable) bool
}

type Sortable interface {
	Equatable
	Less(b Sortable) bool
}

type Hashable interface {
	Sortable
	Hash() int
}

var BpTreeError = fmt.Errorf

func NegativeSize() error {
	return errors.New("negative size")
}

type Iterator func() (item ItemValue, next Iterator)
type KIterator func() (key ItemKey, next KIterator)
type KVIterator func() (key ItemKey, value ItemValue, next KVIterator)
type KVIterable interface {
	Iterate() KVIterator
}

type MapOperable interface {
	Has(key ItemKey) bool
	Put(key ItemKey, value ItemValue) (err error)
	Get(key ItemKey) (value ItemValue, err error)
	Remove(key ItemKey) (value ItemValue, err error)
}

type WhereFunc func(value ItemValue) bool

func MakeValuesIterator(obj KVIterable) Iterator {
	kv_iterator := obj.Iterate()
	var v_iterator Iterator
	v_iterator = func() (value ItemValue, next Iterator) {
		_, value, kv_iterator = kv_iterator()
		if kv_iterator == nil {
			return nil, nil
		}
		return value, v_iterator
	}
	return v_iterator
}

func MakeItemsIterator(obj KVIterable) (kit KIterator) {
	kv_iterator := obj.Iterate()
	kit = func() (item ItemKey, next KIterator) {
		var key ItemKey
		var value ItemValue
		key, value, kv_iterator = kv_iterator()
		if kv_iterator == nil {
			return nil, nil
		}
		return &MapEntry{key, value}, kit
	}
	return kit
}

type MapEntry struct {
	Key   ItemKey
	Value ItemValue
}

func (m *MapEntry) Equals(other Equatable) bool {
	if o, ok := other.(*MapEntry); ok {
		return m.Key.Equals(o.Key)
	} else {
		return m.Key.Equals(other)
	}
}

func (m *MapEntry) Less(other Sortable) bool {
	if o, ok := other.(*MapEntry); ok {
		return m.Key.Less(o.Key)
	} else {
		return m.Key.Less(other)
	}
}

func (m *MapEntry) Hash() int {
	return m.Key.Hash()
}

func (m *MapEntry) String() string {
	return fmt.Sprintf("<MapEntry %v: %v>", m.Key, m.Value)
}
