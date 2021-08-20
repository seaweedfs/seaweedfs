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

type Iterator func() (item interface{}, next Iterator)
type KIterator func() (key Hashable, next KIterator)
type KVIterator func() (key Hashable, value interface{}, next KVIterator)
type KVIterable interface {
	Iterate() KVIterator
}

type MapOperable interface {
	Has(key Hashable) bool
	Put(key Hashable, value interface{}) (err error)
	Get(key Hashable) (value interface{}, err error)
	Remove(key Hashable) (value interface{}, err error)
}

type WhereFunc func(value interface{}) bool

func MakeValuesIterator(obj KVIterable) Iterator {
	kv_iterator := obj.Iterate()
	var v_iterator Iterator
	v_iterator = func() (value interface{}, next Iterator) {
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
	kit = func() (item Hashable, next KIterator) {
		var key Hashable
		var value interface{}
		key, value, kv_iterator = kv_iterator()
		if kv_iterator == nil {
			return nil, nil
		}
		return &MapEntry{key, value}, kit
	}
	return kit
}

type MapEntry struct {
	Key   Hashable
	Value interface{}
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
