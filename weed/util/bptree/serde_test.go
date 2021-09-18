package bptree

import (
	"fmt"
	"testing"
)

type nodeStoreMapImpl struct {
	m map[int64]*ProtoNode
}

func (n *nodeStoreMapImpl) PersistFunc(node *BpNode) error {
	println("saving node", node.protoNodeId)
	n.m[node.protoNodeId] = node.protoNode
	return nil
}
func (n *nodeStoreMapImpl) DestroyFunc(node *BpNode) error {
	println("delete node", node.protoNodeId)
	delete(n.m, node.protoNodeId)
	return nil
}

func TestSerDe(t *testing.T) {

	nodeStore := &nodeStoreMapImpl{
		m: make(map[int64]*ProtoNode),
	}

	tree := NewBpTree(3, nodeStore)

	for i:=0;i<32;i++{
		println("add", i)
		tree.Add(String(fmt.Sprintf("%02d", i)), nil)
	}

	for i:=5;i<9;i++{
		println("----------", i)
		tree.RemoveWhere(String(fmt.Sprintf("%02d", i)), func(value ItemValue) bool {
			return true
		})
		printTree(tree.root, "")
	}



}