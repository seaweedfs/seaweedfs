package bptree

var (
	protoNodeId = int64(0)
)
func GetProtoNodeId() int64 {
	protoNodeId++
	return protoNodeId
}

func (self *BpMap) getRoot() *BpNode {
	return self.root
}
func (self *BpMap) setRoot(root *BpNode) {
	self.root = root
}

func (self *BpTree) getRoot() *BpNode {
	return self.root
}
func (self *BpTree) setRoot(root *BpNode) {
	self.root = root
}

func (self *BpNode) getNext() *BpNode {
	return self.next
}
func (self *BpNode) setNext(next *BpNode) {
	self.next = next
}
func (self *BpNode) getPrev() *BpNode {
	return self.prev
}
func (self *BpNode) setPrev(prev *BpNode) {
	self.prev = prev
}
func (self *BpNode) getNode(x int)(*BpNode) {
	return self.pointers[x]
}

func (self *BpNode) maybePersist(shouldPersist bool) error {
	if !shouldPersist {
		return nil
	}
	return self.persist()
}
func (self *BpNode) persist() error {
	if PersistFn != nil {
		return PersistFn(self)
	}
	return nil
}
func (self *BpNode) destroy() error {
	if DestroyFn != nil {
		return DestroyFn(self)
	}
	return nil
}

func persist(a, b *BpNode) error {
	if a != nil {
		if err := a.persist(); err != nil {
			return err
		}
	}
	if b != nil {
		if err := b.persist(); err != nil {
			return err
		}
	}
	return nil
}