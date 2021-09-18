package bptree

func (protoNode *ProtoNode) ToBpTree() *BpTree {
	node := protoNode.ToBpNode()
	return &BpTree{root: node}
}

func (protoNode *ProtoNode) ToBpNode() *BpNode {
	return nil
}