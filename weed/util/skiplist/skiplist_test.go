package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

const (
	maxN = 10000
)

func TestInsertAndFind(t *testing.T) {

	k0 := []byte("0")
	var list *SkipList

	var listPointer *SkipList
	listPointer.Insert(k0)
	if _, ok := listPointer.Find(k0); ok {
		t.Fail()
	}

	list = New()
	if _, ok := list.Find(k0); ok {
		t.Fail()
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	// Test at the beginning of the list.
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(maxN-i))
		list.Insert(key)
	}
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(maxN-i))
		if _, ok := list.Find(key); !ok {
			t.Fail()
		}
	}


	list = New()
	// Test at the end of the list.
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(i))
		list.Insert(key)
	}
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(i))
		if _, ok := list.Find(key); !ok {
			t.Fail()
		}
	}

	list = New()
	// Test at random positions in the list.
	rList := rand.Perm(maxN)
	for _, e := range rList {
		key := []byte(strconv.Itoa(e))
		println("insert", e)
		list.Insert(key)
	}
	for _, e := range rList {
		key := []byte(strconv.Itoa(e))
		println("find", e)
		if _, ok := list.Find(key); !ok {
			t.Fail()
		}
	}
	println("print list")
	list.println()

}

func Element(x int) []byte {
	return []byte(strconv.Itoa(x))
}

func TestDelete(t *testing.T) {

	k0 := []byte("0")

	var list *SkipList

	// Delete on empty list
	list.Delete(k0)

	list = New()

	list.Delete(k0)
	if !list.IsEmpty() {
		t.Fail()
	}

	list.Insert(k0)
	list.Delete(k0)
	if !list.IsEmpty() {
		t.Fail()
	}

	// Delete elements at the beginning of the list.
	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}
	for i := 0; i < maxN; i++ {
		list.Delete(Element(i))
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	list = New()
	// Delete elements at the end of the list.
	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}
	for i := 0; i < maxN; i++ {
		list.Delete(Element(maxN - i - 1))
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	list = New()
	// Delete elements at random positions in the list.
	rList := rand.Perm(maxN)
	for _, e := range rList {
		list.Insert(Element(e))
	}
	for _, e := range rList {
		list.Delete(Element(e))
	}
	if !list.IsEmpty() {
		t.Fail()
	}
}

func TestNext(t *testing.T) {
	list := New()

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}

	smallest := list.GetSmallestNode()
	largest := list.GetLargestNode()

	lastNode := smallest
	node := lastNode
	for node != largest {
		node = list.Next(node)
		// Must always be incrementing here!
		if bytes.Compare(node.Values[0], lastNode.Values[0]) <= 0 {
			t.Fail()
		}
		// Next.Prev must always point to itself!
		if list.Next(list.Prev(node)) != node {
			t.Fail()
		}
		lastNode = node
	}

	if list.Next(largest) != smallest {
		t.Fail()
	}
}

func TestPrev(t *testing.T) {
	list := New()

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}

	smallest := list.GetSmallestNode()
	largest := list.GetLargestNode()

	lastNode := largest
	node := lastNode
	for node != smallest {
		node = list.Prev(node)
		// Must always be incrementing here!
		if bytes.Compare(node.Values[0], lastNode.Values[0]) >= 0 {
			t.Fail()
		}
		// Next.Prev must always point to itself!
		if list.Prev(list.Next(node)) != node {
			t.Fail()
		}
		lastNode = node
	}

	if list.Prev(smallest) != largest {
		t.Fail()
	}
}

func TestGetNodeCount(t *testing.T) {
	list := New()

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}

	if list.GetNodeCount() != maxN {
		t.Fail()
	}
}

func TestFindGreaterOrEqual(t *testing.T) {

	maxNumber := maxN * 100

	var list *SkipList
	var listPointer *SkipList

	// Test on empty list.
	if _, ok := listPointer.FindGreaterOrEqual(Element(0)); ok {
		t.Fail()
	}

	list = New()

	for i := 0; i < maxN; i++ {
		list.Insert(Element(rand.Intn(maxNumber)))
	}

	for i := 0; i < maxN; i++ {
		key := Element(rand.Intn(maxNumber))
		if v, ok := list.FindGreaterOrEqual(key); ok {
			// if f is v should be bigger than the element before
			if bytes.Compare(v.Prev.Key, key) >= 0 {
				fmt.Printf("PrevV: %s\n    key: %s\n\n", string(v.Prev.Key), string(key))
				t.Fail()
			}
			// v should be bigger or equal to f
			// If we compare directly, we get an equal key with a difference on the 10th decimal point, which fails.
			if bytes.Compare(v.Values[0], key) < 0 {
				fmt.Printf("v: %s\n    key: %s\n\n", string(v.Values[0]), string(key))
				t.Fail()
			}
		} else {
			lastV := list.GetLargestNode().GetValue()
			// It is OK, to fail, as long as f is bigger than the last element.
			if bytes.Compare(key, lastV) <= 0 {
				fmt.Printf("lastV: %s\n    key: %s\n\n", string(lastV), string(key))
				t.Fail()
			}
		}
	}

}