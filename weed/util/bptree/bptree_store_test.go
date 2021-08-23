package bptree

import (
	"fmt"
	"testing"
)

func TestAddRemove(t *testing.T) {
	tree := NewBpTree(32)
	PersistFn = func(node *BpNode) error {
		println("saving", node.protoNodeId)
		return nil
	}
	DestroyFn = func(node *BpNode) error {
		println("delete", node.protoNodeId)
		return nil
	}
	for i:=0;i<1024;i++{
		println("++++++++++", i)
		tree.Add(String(fmt.Sprintf("%02d", i)), String(fmt.Sprintf("%02d", i)))
		printTree(tree.root, "")
	}
}

func printTree(node *BpNode, prefix string) {
	fmt.Printf("%sNode %d\n", prefix, node.protoNodeId)
	prefix += "  "
	for i:=0;i<len(node.keys);i++{
		fmt.Printf("%skey %s\n", prefix, node.keys[i])
		if i < len(node.pointers) && node.pointers[i] != nil {
			printTree(node.pointers[i], prefix+"  ")
		}
	}
}