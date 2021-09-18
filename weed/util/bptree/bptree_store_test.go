package bptree

import (
	"fmt"
	"testing"
)

type nodeStorePrintlnImpl struct {
}

func (n *nodeStorePrintlnImpl) PersistFunc(node *BpNode) error {
	println("saving node", node.protoNodeId)
	return nil
}
func (n *nodeStorePrintlnImpl) DestroyFunc(node *BpNode) error {
	println("delete node", node.protoNodeId)
	return nil
}

func TestAddRemove(t *testing.T) {

	tree := NewBpTree(3, &nodeStorePrintlnImpl{})
	for i:=0;i<9;i++{
		println("++++++++++", i)
		tree.Add(String(fmt.Sprintf("%02d", i)), nil)
		printTree(tree.root, "")
	}

	if !tree.Has(String("08")) {
		t.Errorf("lookup error")
	}
	for i:=5;i<9;i++{
		println("----------", i)
		tree.RemoveWhere(String(fmt.Sprintf("%02d", i)), func(value ItemValue) bool {
			return true
		})
		printTree(tree.root, "")
	}
	if tree.Has(String("08")) {
		t.Errorf("remove error")
	}
}

func printTree(node *BpNode, prefix string) {
	fmt.Printf("%sNode %d\n", prefix, node.protoNodeId)
	prefix += " "
	for i:=0;i<len(node.keys);i++{
		fmt.Printf("%skey %v\n", prefix, node.keys[i])
		if i < len(node.pointers) && node.pointers[i] != nil {
			printTree(node.pointers[i], prefix+" ")
		}
	}
}