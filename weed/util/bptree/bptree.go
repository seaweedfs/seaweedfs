package bptree

// started by copying from https://sourcegraph.com/github.com/timtadh/data-structures@master/-/tree/tree/bptree

/* A BpTree is a B+Tree with support for duplicate keys. This makes it behave as
 * a MultiMap. Additionally you can use the Range operator to select k/v in a
 * range. If from > to it will iterate backwards.
 */
type BpTree struct {
	root *BpNode
}

type loc_iterator func() (i int, leaf *BpNode, li loc_iterator)

func NewBpTree(node_size int, nodeStore NodeStore) *BpTree {
	return &BpTree{
		root: NewLeaf(node_size, nodeStore),
	}
}

func (self *BpTree) Has(key ItemKey) bool {
	if len(self.getRoot().keys) == 0 {
		return false
	}
	j, l := self.getRoot().get_start(key)
	return l.keys[j].Equals(key)
}

func (self *BpTree) Count(key ItemKey) int {
	if len(self.root.keys) == 0 {
		return 0
	}
	j, l := self.root.get_start(key)
	count := 0
	end := false
	for !end && l.keys[j].Equals(key) {
		count++
		j, l, end = next_location(j, l)
	}
	return count
}

func (self *BpTree) Add(key ItemKey, value ItemValue) (err error) {
	new_root, err := self.getRoot().put(key, value)
	if err != nil {
		return err
	}
	self.setRoot(new_root)
	return nil
}

func (self *BpTree) Replace(key ItemKey, where WhereFunc, value ItemValue) (err error) {
	li := self.getRoot().forward(key, key)
	for i, leaf, next := li(); next != nil; i, leaf, next = next() {
		if where(leaf.values[i]) {
			leaf.values[i] = value
			if persistErr := leaf.persist(); persistErr != nil && err == nil {
				err = persistErr
				break
			}
		}
	}
	return err
}

func (self *BpTree) Find(key ItemKey) (kvi KVIterator) {
	return self.Range(key, key)
}

func (self *BpTree) Range(from, to ItemKey) (kvi KVIterator) {
	var li loc_iterator
	if !to.Less(from) {
		li = self.getRoot().forward(from, to)
	} else {
		li = self.getRoot().backward(from, to)
	}
	kvi = func() (key ItemKey, value ItemValue, next KVIterator) {
		var i int
		var leaf *BpNode
		i, leaf, li = li()
		if li == nil {
			return nil, nil, nil
		}
		return leaf.keys[i], leaf.values[i], kvi
	}
	return kvi
}

func (self *BpTree) RemoveWhere(key ItemKey, where WhereFunc) (err error) {
	ns := self.getRoot().Capacity()
	new_root, err := self.getRoot().remove(key, where)
	if err != nil {
		return err
	}
	if new_root == nil {
		new_root = NewLeaf(ns, self.root.nodeStore)
		err = new_root.persist()
		self.setRoot(new_root)
	} else {
		self.setRoot(new_root)
	}
	return err
}

func (self *BpTree) Keys() (ki KIterator) {
	li := self.getRoot().all()
	var prev Equatable
	ki = func() (key ItemKey, next KIterator) {
		var i int
		var leaf *BpNode
		i, leaf, li = li()
		if li == nil {
			return nil, nil
		}
		if leaf.keys[i].Equals(prev) {
			return ki()
		}
		prev = leaf.keys[i]
		return leaf.keys[i], ki
	}
	return ki
}

func (self *BpTree) Values() (vi Iterator) {
	return MakeValuesIterator(self)
}

func (self *BpTree) Items() (vi KIterator) {
	return MakeItemsIterator(self)
}

func (self *BpTree) Iterate() (kvi KVIterator) {
	li := self.getRoot().all()
	kvi = func() (key ItemKey, value ItemValue, next KVIterator) {
		var i int
		var leaf *BpNode
		i, leaf, li = li()
		if li == nil {
			return nil, nil, nil
		}
		return leaf.keys[i], leaf.values[i], kvi
	}
	return kvi
}

func (self *BpTree) Backward() (kvi KVIterator) {
	li := self.getRoot().all_backward()
	kvi = func() (key ItemKey, value ItemValue, next KVIterator) {
		var i int
		var leaf *BpNode
		i, leaf, li = li()
		if li == nil {
			return nil, nil, nil
		}
		return leaf.keys[i], leaf.values[i], kvi
	}
	return kvi
}
