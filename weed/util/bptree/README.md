This adapts one b+ tree implementation
https://sourcegraph.com/github.com/timtadh/data-structures@master/-/tree/tree/bptree
to persist changes to on disk.

# When a node needs to persist itself?

* A node changed its key or value
    * When an item is added.
    * When an item is updated.
    * When an item is deleted.

* When a node is split.
    * 2 new nodes are created (they shoud persist themselves). 
    * Parent node need to point to the new nodes. 

* When a node is merged.
    * delete one node
    * persist the merged node


In general, if one node is returned from a function, the node should have already been persisted.
The parent node may need to delete the old node.

BpTree
  Add(key ItemKey, value ItemValue)
    new_root = self.getRoot().put(key,value)
      a, b, err := self.insert(key, value)
        self.internal_insert(key, value)
          self.internal_split(q.keys[0], q)
            persist(a,b)
          self.persist() // child add q node
          self.maybePersist(child == p)
        self.leaf_insert(key, value)
          self.persist() // if dedup
          self.leaf_split(key, value)
            self.pure_leaf_split(key, value)
              persist(a,b)
              a.persist()
              persist(a,b)
            self.put_kv(key, value)
      new_root.persist()
    self.setRoot(new_root)
      oldroot.destroy()
    // maybe persist BpTree new root

  Replace(key ItemKey, where WhereFunc, value ItemValue)
    leaf.persist()
  RemoveWhere(key ItemKey, where WhereFunc)
    self.getRoot().remove(key, where)
      self.internal_remove(key, nil, where)
        child.leaf_remove(key, nil, where)
        child.leaf_remove(key, sibling.keys[0], where)
          l.destroy() // when the node is empty
          a.maybePersist(hasChange)
        self.destroy() // when no keys left
        self.persist() // when some keys are left
      self.leaf_remove(key, self.keys[len(self.keys)-1], where)
    new_root.persist() // when new root is added
    // maybe persist BpTree new root
  