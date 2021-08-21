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
