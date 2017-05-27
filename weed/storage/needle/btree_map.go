package needle

import (
	"github.com/google/btree"
)

//This map assumes mostly inserting increasing keys
type BtreeMap struct {
	tree *btree.BTree
}

func NewBtreeMap() *BtreeMap {
	return &BtreeMap{
		tree: btree.New(32),
	}
}

func (cm *BtreeMap) Set(key Key, offset, size uint32) (oldOffset, oldSize uint32) {
	found := cm.tree.ReplaceOrInsert(NeedleValue{key, offset, size})
	if found != nil {
		old := found.(NeedleValue)
		return old.Offset, old.Size
	}
	return
}

func (cm *BtreeMap) Delete(key Key) (oldSize uint32) {
	found := cm.tree.Delete(NeedleValue{key, 0, 0})
	if found != nil {
		old := found.(NeedleValue)
		return old.Size
	}
	return
}
func (cm *BtreeMap) Get(key Key) (*NeedleValue, bool) {
	found := cm.tree.Get(NeedleValue{key, 0, 0})
	if found != nil {
		old := found.(NeedleValue)
		return &old, true
	}
	return nil, false
}

// Visit visits all entries or stop if any error when visiting
func (cm *BtreeMap) Visit(visit func(NeedleValue) error) (ret error) {
	cm.tree.Ascend(func(item btree.Item) bool {
		needle := item.(NeedleValue)
		ret = visit(needle)
		return ret == nil
	})
	return ret
}
