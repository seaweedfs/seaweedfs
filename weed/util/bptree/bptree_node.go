package bptree

type BpNode struct {
	keys     []Hashable
	values   []interface{}
	pointers []*BpNode
	next     *BpNode
	prev     *BpNode
}

func NewInternal(size int) *BpNode {
	if size < 0 {
		panic(NegativeSize())
	}
	return &BpNode{
		keys:     make([]Hashable, 0, size),
		pointers: make([]*BpNode, 0, size),
	}
}

func NewLeaf(size int) *BpNode {
	if size < 0 {
		panic(NegativeSize())
	}
	return &BpNode{
		keys:   make([]Hashable, 0, size),
		values: make([]interface{}, 0, size),
	}
}

func (self *BpNode) Full() bool {
	return len(self.keys) == cap(self.keys)
}

func (self *BpNode) Pure() bool {
	if len(self.keys) == 0 {
		return true
	}
	k0 := self.keys[0]
	for _, k := range self.keys {
		if !k0.Equals(k) {
			return false
		}
	}
	return true
}

func (self *BpNode) Internal() bool {
	return cap(self.pointers) > 0
}

func (self *BpNode) NodeSize() int {
	return cap(self.keys)
}

func (self *BpNode) Height() int {
	if !self.Internal() {
		return 1
	} else if len(self.pointers) == 0 {
		panic(BpTreeError("Internal node has no pointers but asked for height"))
	}
	return self.pointers[0].Height() + 1
}

func (self *BpNode) count(key Hashable) int {
	i, _ := self.find(key)
	count := 0
	for ; i < len(self.keys); i++ {
		if self.keys[i].Equals(key) {
			count++
		} else {
			break
		}
	}
	return count
}

func (self *BpNode) has(key Hashable) bool {
	_, has := self.find(key)
	return has
}

func (self *BpNode) left_most_leaf() *BpNode {
	if self.Internal() {
		return self.pointers[0].left_most_leaf()
	}
	return self
}

func (self *BpNode) right_most_leaf() *BpNode {
	if self.Internal() {
		return self.pointers[len(self.pointers)-1].right_most_leaf()
	}
	return self
}

/* returns the index and leaf-block of the first key greater than or equal to
 * the search key. (unless the search key is greater than all the keys in the
 * tree, in that case it will be the last key in the tree)
 */
func (self *BpNode) get_start(key Hashable) (i int, leaf *BpNode) {
	if self.Internal() {
		return self.internal_get_start(key)
	} else {
		return self.leaf_get_start(key)
	}
}

func next_location(i int, leaf *BpNode) (int, *BpNode, bool) {
	j := i + 1
	for j >= len(leaf.keys) && leaf.next != nil {
		j = 0
		leaf = leaf.next
	}
	if j >= len(leaf.keys) {
		return -1, nil, true
	}
	return j, leaf, false
}

func prev_location(i int, leaf *BpNode) (int, *BpNode, bool) {
	j := i - 1
	for j < 0 && leaf.prev != nil {
		leaf = leaf.prev
		j = len(leaf.keys) - 1
	}
	if j < 0 {
		return -1, nil, true
	}
	return j, leaf, false
}

/* returns the index and leaf-block of the last key equal to the search key or
 * the first key greater than the search key. (unless the search key is greater
 * than all the keys in the tree, in that case it will be the last key in the
 * tree)
 */
func (self *BpNode) get_end(key Hashable) (i int, leaf *BpNode) {
	end := false
	i, leaf = self.get_start(key)
	pi, pleaf := i, leaf
	for !end && leaf.keys[i].Equals(key) {
		pi, pleaf = i, leaf
		i, leaf, end = next_location(i, leaf)
	}
	return pi, pleaf
}

func (self *BpNode) internal_get_start(key Hashable) (i int, leaf *BpNode) {
	if !self.Internal() {
		panic(BpTreeError("Expected a internal node"))
	}
	i, has := self.find(key)
	if !has && i > 0 {
		// if it doesn't have it and the index > 0 then we have the next block
		// so we have to subtract one from the index.
		i--
	}
	child := self.pointers[i]
	return child.get_start(key)
}

func (self *BpNode) leaf_get_start(key Hashable) (i int, leaf *BpNode) {
	i, has := self.find(key)
	if i >= len(self.keys) && i > 0 {
		i = len(self.keys) - 1
	}
	if !has && (len(self.keys) == 0 || self.keys[i].Less(key)) && self.next != nil {
		return self.next.leaf_get_start(key)
	}
	return i, self
}

/* This puts the k/v pair into the B+Tree rooted at this node and returns the
 * (possibly) new root of the tree.
 */
func (self *BpNode) put(key Hashable, value interface{}) (root *BpNode, err error) {
	a, b, err := self.insert(key, value)
	if err != nil {
		return nil, err
	} else if b == nil {
		return a, nil
	}
	// else we have root split
	root = NewInternal(self.NodeSize())
	root.put_kp(a.keys[0], a)
	root.put_kp(b.keys[0], b)
	return root, nil
}

// right is only set on split
// left is always set. When split is false left is the pointer to block
//                     When split is true left is the pointer to the new left
//                     block
func (self *BpNode) insert(key Hashable, value interface{}) (a, b *BpNode, err error) {
	if self.Internal() {
		return self.internal_insert(key, value)
	} else { // leaf node
		return self.leaf_insert(key, value)
	}
}

/* - first find the child to insert into
 * - do the child insert
 * - if there was a split:
 *    - if the block is full, split this block
 *    - else insert the new key/pointer into this block
 */
func (self *BpNode) internal_insert(key Hashable, value interface{}) (a, b *BpNode, err error) {
	if !self.Internal() {
		return nil, nil, BpTreeError("Expected a internal node")
	}
	i, has := self.find(key)
	if !has && i > 0 {
		// if it doesn't have it and the index > 0 then we have the next block
		// so we have to subtract one from the index.
		i--
	}
	child := self.pointers[i]
	p, q, err := child.insert(key, value)
	if err != nil {
		return nil, nil, err
	}
	self.keys[i] = p.keys[0]
	self.pointers[i] = p
	if q != nil {
		// we had a split
		if self.Full() {
			return self.internal_split(q.keys[0], q)
		} else {
			if err := self.put_kp(q.keys[0], q); err != nil {
				return nil, nil, err
			}
			return self, nil, nil
		}
	}
	return self, nil, nil
}

/* On split
 * - first assert that the key to be inserted is not already in the block.
 * - Make a new block
 * - balance the two blocks.
 * - insert the new key/pointer combo into the correct block
 */
func (self *BpNode) internal_split(key Hashable, ptr *BpNode) (a, b *BpNode, err error) {
	if !self.Internal() {
		return nil, nil, BpTreeError("Expected a internal node")
	}
	if self.has(key) {
		return nil, nil, BpTreeError("Tried to split an internal block on duplicate key")
	}
	a = self
	b = NewInternal(self.NodeSize())
	balance_nodes(a, b)
	if key.Less(b.keys[0]) {
		if err := a.put_kp(key, ptr); err != nil {
			return nil, nil, err
		}
	} else {
		if err := b.put_kp(key, ptr); err != nil {
			return nil, nil, err
		}
	}
	return a, b, nil
}

/* if the leaf is full then it will defer to a leaf_split
 *    (but in one case that will not actually split in the case of a insert into
 *    a pure block with a matching key)
 * else this leaf will get a new entry.
 */
func (self *BpNode) leaf_insert(key Hashable, value interface{}) (a, b *BpNode, err error) {
	if self.Internal() {
		return nil, nil, BpTreeError("Expected a leaf node")
	}
	if self.Full() {
		return self.leaf_split(key, value)
	} else {
		if err := self.put_kv(key, value); err != nil {
			return nil, nil, err
		}
		return self, nil, nil
	}
}

/* on leaf split if the block is pure then it will defer to pure_leaf_split
 * else
 *    - a new block will be made and inserted after this one
 *    - the two blocks will be balanced with balanced_nodes
 *    - if the key is less than b.keys[0] it will go in a else b
 */
func (self *BpNode) leaf_split(key Hashable, value interface{}) (a, b *BpNode, err error) {
	if self.Internal() {
		return nil, nil, BpTreeError("Expected a leaf node")
	}
	if self.Pure() {
		return self.pure_leaf_split(key, value)
	}
	a = self
	b = NewLeaf(self.NodeSize())
	insert_linked_list_node(b, a, a.next)
	balance_nodes(a, b)
	if key.Less(b.keys[0]) {
		if err := a.put_kv(key, value); err != nil {
			return nil, nil, err
		}
	} else {
		if err := b.put_kv(key, value); err != nil {
			return nil, nil, err
		}
	}
	return a, b, nil
}

/* a pure leaf split has two cases:
 *  1) the inserted key is less than the current pure block.
 *     - a new block should be created before the current block
 *     - the key should be put in it
 *  2) the inserted key is greater than or equal to the pure block.
 *     - the end of run of pure blocks should be found
 *     - if the key is equal to pure block and the last block is not full insert
 *       the new kv
 *     - else split by making a new block after the last block in the run
 *       and putting the new key there.
 *     - always return the current block as "a" and the new block as "b"
 */
func (self *BpNode) pure_leaf_split(key Hashable, value interface{}) (a, b *BpNode, err error) {
	if self.Internal() || !self.Pure() {
		return nil, nil, BpTreeError("Expected a pure leaf node")
	}
	if key.Less(self.keys[0]) {
		a = NewLeaf(self.NodeSize())
		b = self
		if err := a.put_kv(key, value); err != nil {
			return nil, nil, err
		}
		insert_linked_list_node(a, b.prev, b)
		return a, b, nil
	} else {
		a = self
		e := self.find_end_of_pure_run()
		if e.keys[0].Equals(key) && !e.Full() {
			if err := e.put_kv(key, value); err != nil {
				return nil, nil, err
			}
			return a, nil, nil
		} else {
			b = NewLeaf(self.NodeSize())
			if err := b.put_kv(key, value); err != nil {
				return nil, nil, err
			}
			insert_linked_list_node(b, e, e.next)
			if e.keys[0].Equals(key) {
				return a, nil, nil
			}
			return a, b, nil
		}
	}
}

func (self *BpNode) put_kp(key Hashable, ptr *BpNode) error {
	if self.Full() {
		return BpTreeError("Block is full.")
	}
	if !self.Internal() {
		return BpTreeError("Expected a internal node")
	}
	i, has := self.find(key)
	if has {
		return BpTreeError("Tried to insert a duplicate key into an internal node")
	} else if i < 0 {
		panic(BpTreeError("find returned a negative int"))
	} else if i >= cap(self.keys) {
		panic(BpTreeError("find returned a int > than cap(keys)"))
	}
	if err := self.put_key_at(i, key); err != nil {
		return err
	}
	if err := self.put_pointer_at(i, ptr); err != nil {
		return err
	}
	return nil
}

func (self *BpNode) put_kv(key Hashable, value interface{}) error {
	if self.Full() {
		return BpTreeError("Block is full.")
	}
	if self.Internal() {
		return BpTreeError("Expected a leaf node")
	}
	i, _ := self.find(key)
	if i < 0 {
		panic(BpTreeError("find returned a negative int"))
	} else if i >= cap(self.keys) {
		panic(BpTreeError("find returned a int > than cap(keys)"))
	}
	if err := self.put_key_at(i, key); err != nil {
		return err
	}
	if err := self.put_value_at(i, value); err != nil {
		return err
	}
	return nil
}

func (self *BpNode) put_key_at(i int, key Hashable) error {
	if self.Full() {
		return BpTreeError("Block is full.")
	}
	self.keys = self.keys[:len(self.keys)+1]
	for j := len(self.keys) - 1; j > i; j-- {
		self.keys[j] = self.keys[j-1]
	}
	self.keys[i] = key
	return nil
}

func (self *BpNode) put_value_at(i int, value interface{}) error {
	if len(self.values) == cap(self.values) {
		return BpTreeError("Block is full.")
	}
	if self.Internal() {
		return BpTreeError("Expected a leaf node")
	}
	self.values = self.values[:len(self.values)+1]
	for j := len(self.values) - 1; j > i; j-- {
		self.values[j] = self.values[j-1]
	}
	self.values[i] = value
	return nil
}

func (self *BpNode) put_pointer_at(i int, pointer *BpNode) error {
	if len(self.pointers) == cap(self.pointers) {
		return BpTreeError("Block is full.")
	}
	if !self.Internal() {
		return BpTreeError("Expected a internal node")
	}
	self.pointers = self.pointers[:len(self.pointers)+1]
	for j := len(self.pointers) - 1; j > i; j-- {
		self.pointers[j] = self.pointers[j-1]
	}
	self.pointers[i] = pointer
	return nil
}

func (self *BpNode) remove(key Hashable, where WhereFunc) (a *BpNode, err error) {
	if self.Internal() {
		return self.internal_remove(key, nil, where)
	} else {
		return self.leaf_remove(key, self.keys[len(self.keys)-1], where)
	}
}

func (self *BpNode) internal_remove(key Hashable, sibling *BpNode, where WhereFunc) (a *BpNode, err error) {
	if !self.Internal() {
		panic(BpTreeError("Expected a internal node"))
	}
	i, has := self.find(key)
	if !has && i > 0 {
		// if it doesn't have it and the index > 0 then we have the next block
		// so we have to subtract one from the index.
		i--
	}
	if i+1 < len(self.keys) {
		sibling = self.pointers[i+1]
	} else if sibling != nil {
		sibling = sibling.left_most_leaf()
	}
	child := self.pointers[i]
	if child.Internal() {
		child, err = child.internal_remove(key, sibling, where)
	} else {
		if sibling == nil {
			child, err = child.leaf_remove(key, nil, where)
		} else {
			child, err = child.leaf_remove(key, sibling.keys[0], where)
		}
	}
	if err != nil {
		return nil, err
	}
	if child == nil {
		if err := self.remove_key_at(i); err != nil {
			return nil, err
		}
		if err := self.remove_ptr_at(i); err != nil {
			return nil, err
		}
	} else {
		self.keys[i] = child.keys[0]
		self.pointers[i] = child
	}
	if len(self.keys) == 0 {
		return nil, nil
	}
	return self, nil
}

func (self *BpNode) leaf_remove(key, stop Hashable, where WhereFunc) (a *BpNode, err error) {
	if self.Internal() {
		return nil, BpTreeError("Expected a leaf node")
	}
	a = self
	for j, l, next := self.forward(key, key)(); next != nil; j, l, next = next() {
		if where(l.values[j]) {
			if err := l.remove_key_at(j); err != nil {
				return nil, err
			}
			if err := l.remove_value_at(j); err != nil {
				return nil, err
			}
		}
		if len(l.keys) == 0 {
			remove_linked_list_node(l)
			if l.next == nil {
				a = nil
			} else if stop == nil {
				a = nil
			} else if !l.next.keys[0].Equals(stop) {
				a = l.next
			} else {
				a = nil
			}
		}
	}
	return a, nil
}

func (self *BpNode) remove_key_at(i int) error {
	if i >= len(self.keys) || i < 0 {
		return BpTreeError("i, %v, is out of bounds, %v, %v %v.", i, len(self.keys), len(self.values), self)
	}
	for j := i; j < len(self.keys)-1; j++ {
		self.keys[j] = self.keys[j+1]
	}
	self.keys = self.keys[:len(self.keys)-1]
	return nil
}

func (self *BpNode) remove_value_at(i int) error {
	if i >= len(self.values) || i < 0 {
		return BpTreeError("i, %v, is out of bounds, %v.", i, len(self.values))
	}
	for j := i; j < len(self.values)-1; j++ {
		self.values[j] = self.values[j+1]
	}
	self.values = self.values[:len(self.values)-1]
	return nil
}

func (self *BpNode) remove_ptr_at(i int) error {
	if i >= len(self.pointers) || i < 0 {
		return BpTreeError("i, %v, is out of bounds, %v.", i, len(self.pointers))
	}
	for j := i; j < len(self.pointers)-1; j++ {
		self.pointers[j] = self.pointers[j+1]
	}
	self.pointers = self.pointers[:len(self.pointers)-1]
	return nil
}

func (self *BpNode) find(key Hashable) (int, bool) {
	var l int = 0
	var r int = len(self.keys) - 1
	var m int
	for l <= r {
		m = ((r - l) >> 1) + l
		if key.Less(self.keys[m]) {
			r = m - 1
		} else if key.Equals(self.keys[m]) {
			for j := m; j >= 0; j-- {
				if j == 0 || !key.Equals(self.keys[j-1]) {
					return j, true
				}
			}
		} else {
			l = m + 1
		}
	}
	return l, false
}

func (self *BpNode) find_end_of_pure_run() *BpNode {
	k := self.keys[0]
	p := self
	n := self.next
	for n != nil && n.Pure() && k.Equals(n.keys[0]) {
		p = n
		n = n.next
	}
	return p
}

func (self *BpNode) all() (li loc_iterator) {
	j := -1
	l := self.left_most_leaf()
	end := false
	j, l, end = next_location(j, l)
	li = func() (i int, leaf *BpNode, next loc_iterator) {
		if end {
			return -1, nil, nil
		}
		i = j
		leaf = l
		j, l, end = next_location(j, l)
		return i, leaf, li
	}
	return li
}

func (self *BpNode) all_backward() (li loc_iterator) {
	l := self.right_most_leaf()
	j := len(l.keys)
	end := false
	j, l, end = prev_location(j, l)
	li = func() (i int, leaf *BpNode, next loc_iterator) {
		if end {
			return -1, nil, nil
		}
		i = j
		leaf = l
		j, l, end = prev_location(j, l)
		return i, leaf, li
	}
	return li
}

func (self *BpNode) forward(from, to Hashable) (li loc_iterator) {
	j, l := self.get_start(from)
	end := false
	j--
	li = func() (i int, leaf *BpNode, next loc_iterator) {
		j, l, end = next_location(j, l)
		if end || to.Less(l.keys[j]) {
			return -1, nil, nil
		}
		return j, l, li
	}
	return li
}

func (self *BpNode) backward(from, to Hashable) (li loc_iterator) {
	j, l := self.get_end(from)
	end := false
	li = func() (i int, leaf *BpNode, next loc_iterator) {
		if end || l.keys[j].Less(to) {
			return -1, nil, nil
		}
		i = j
		leaf = l
		j, l, end = prev_location(i, l)
		return i, leaf, li
	}
	return li
}

func insert_linked_list_node(n, prev, next *BpNode) {
	if (prev != nil && prev.next != next) || (next != nil && next.prev != prev) {
		panic(BpTreeError("prev and next not hooked up"))
	}
	n.prev = prev
	n.next = next
	if prev != nil {
		prev.next = n
	}
	if next != nil {
		next.prev = n
	}
}

func remove_linked_list_node(n *BpNode) {
	if n.prev != nil {
		n.prev.next = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	}
}

/* a must be full and b must be empty else there will be a panic
 */
func balance_nodes(a, b *BpNode) {
	if len(b.keys) != 0 {
		panic(BpTreeError("b was not empty"))
	}
	if !a.Full() {
		panic(BpTreeError("a was not full", a))
	}
	if cap(a.keys) != cap(b.keys) {
		panic(BpTreeError("cap(a.keys) != cap(b.keys)"))
	}
	if cap(a.values) != cap(b.values) {
		panic(BpTreeError("cap(a.values) != cap(b.values)"))
	}
	if cap(a.pointers) != cap(b.pointers) {
		panic(BpTreeError("cap(a.pointers) != cap(b.pointers)"))
	}
	m := len(a.keys) / 2
	for m < len(a.keys) && a.keys[m-1].Equals(a.keys[m]) {
		m++
	}
	if m == len(a.keys) {
		m--
		for m > 0 && a.keys[m-1].Equals(a.keys[m]) {
			m--
		}
	}
	var lim int = len(a.keys) - m
	b.keys = b.keys[:lim]
	if cap(a.values) > 0 {
		if cap(a.values) != cap(a.keys) {
			panic(BpTreeError("cap(a.values) != cap(a.keys)"))
		}
		b.values = b.values[:lim]
	}
	if cap(a.pointers) > 0 {
		if cap(a.pointers) != cap(a.keys) {
			panic(BpTreeError("cap(a.pointers) != cap(a.keys)"))
		}
		b.pointers = b.pointers[:lim]
	}
	for i := 0; i < lim; i++ {
		j := m + i
		b.keys[i] = a.keys[j]
		if cap(a.values) > 0 {
			b.values[i] = a.values[j]
		}
		if cap(a.pointers) > 0 {
			b.pointers[i] = a.pointers[j]
		}
	}
	a.keys = a.keys[:m]
	if cap(a.values) > 0 {
		a.values = a.values[:m]
	}
	if cap(a.pointers) > 0 {
		a.pointers = a.pointers[:m]
	}
}
