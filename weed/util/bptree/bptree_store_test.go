package bptree

import (
	"fmt"
	"testing"
)

func TestAddRemove(t *testing.T) {
	tree := NewBpTree(5)
	PersistFn = func(node *BpNode) error {
		println("saving", node.protoNodeId)
		return nil
	}
	DestroyFn = func(node *BpNode) error {
		println("delete", node.protoNodeId)
		return nil
	}
	for i:=0;i<32;i++{
		println("++++++++++", i)
		tree.Add(String(fmt.Sprintf("%02d", i)), nil)
		printTree(tree.root, "")
	}

	if !tree.Has(String("30")) {
		t.Errorf("lookup error")
	}
	tree.RemoveWhere(String("30"), func(value ItemValue) bool {
		return true
	})
	if tree.Has(String("30")) {
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