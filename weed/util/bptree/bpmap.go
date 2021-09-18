package bptree

import (
	"fmt"
)

/* A BpMap is a B+Tree with support for duplicate keys disabled. This makes it
 * behave like a regular Map rather than a MultiMap.
 */
type BpMap BpTree

func NewBpMap(node_size int) *BpMap {
	return &BpMap{
		root: NewLeaf(node_size),
	}
}

func (self *BpMap) Has(key ItemKey) bool {
	return (*BpTree)(self).Has(key)
}

func (self *BpMap) Put(key ItemKey, value ItemValue) (err error) {
	new_root, err := self.getRoot().put(key, value)
	if err != nil {
		return err
	}
	self.setRoot(new_root)
	return nil
}

func (self *BpMap) Get(key ItemKey) (value ItemValue, err error) {
	j, l := self.getRoot().get_start(key)
	if l.keys[j].Equals(key) {
		return l.values[j], nil
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

func (self *BpMap) Remove(key ItemKey) (value ItemValue, err error) {
	value, err = self.Get(key)
	if err != nil {
		return nil, err
	}
	ns := self.getRoot().Capacity()
	new_root, err := self.getRoot().remove(key, func(value ItemValue) bool { return true })
	if err != nil {
		return nil, err
	}
	if new_root == nil {
		new_root = NewLeaf(ns)
		err = new_root.persist()
		self.setRoot(new_root)
	} else {
		self.setRoot(new_root)
	}
	return value, nil
}

func (self *BpMap) Keys() (ki KIterator) {
	return (*BpTree)(self).Keys()
}

func (self *BpMap) Values() (vi Iterator) {
	return (*BpTree)(self).Values()
}

func (self *BpMap) Iterate() (kvi KVIterator) {
	return (*BpTree)(self).Iterate()
}
