package bptree

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
