package skiplist

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"
)

const (
	maxN = 10000
)

var (
	memStore = newMemStore()
)

func TestReverseInsert(t *testing.T) {
	list := NewSeed(100, memStore)

	list.InsertByKey([]byte("zzz"), 0, []byte("zzz"))
	list.DeleteByKey([]byte("zzz"))

	list.InsertByKey([]byte("aaa"), 0, []byte("aaa"))

	if list.IsEmpty() {
		t.Fail()
	}

}

func TestInsertAndFind(t *testing.T) {

	k0 := []byte("0")
	var list *SkipList

	var listPointer *SkipList
	listPointer.InsertByKey(k0, 0, k0)
	if _, _, ok, _ := listPointer.Find(k0); ok {
		t.Fail()
	}

	list = New(memStore)
	if _, _, ok, _ := list.Find(k0); ok {
		t.Fail()
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	// Test at the beginning of the list.
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(maxN - i))
		list.InsertByKey(key, 0, key)
	}
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(maxN - i))
		if _, _, ok, _ := list.Find(key); !ok {
			t.Fail()
		}
	}

	list = New(memStore)
	// Test at the end of the list.
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(i))
		list.InsertByKey(key, 0, key)
	}
	for i := 0; i < maxN; i++ {
		key := []byte(strconv.Itoa(i))
		if _, _, ok, _ := list.Find(key); !ok {
			t.Fail()
		}
	}

	list = New(memStore)
	// Test at random positions in the list.
	rList := rand.Perm(maxN)
	for _, e := range rList {
		key := []byte(strconv.Itoa(e))
		// println("insert", e)
		list.InsertByKey(key, 0, key)
	}
	for _, e := range rList {
		key := []byte(strconv.Itoa(e))
		// println("find", e)
		if _, _, ok, _ := list.Find(key); !ok {
			t.Fail()
		}
	}
	// println("print list")
	// list.println()

}

func Element(x int) []byte {
	return []byte(strconv.Itoa(x))
}

func TestDelete(t *testing.T) {

	k0 := []byte("0")

	var list *SkipList

	// Delete on empty list
	list.DeleteByKey(k0)

	list = New(memStore)

	list.DeleteByKey(k0)
	if !list.IsEmpty() {
		t.Fail()
	}

	list.InsertByKey(k0, 0, k0)
	list.DeleteByKey(k0)
	if !list.IsEmpty() {
		t.Fail()
	}

	// Delete elements at the beginning of the list.
	for i := 0; i < maxN; i++ {
		list.InsertByKey(Element(i), 0, Element(i))
	}
	for i := 0; i < maxN; i++ {
		list.DeleteByKey(Element(i))
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	list = New(memStore)
	// Delete elements at the end of the list.
	for i := 0; i < maxN; i++ {
		list.InsertByKey(Element(i), 0, Element(i))
	}
	for i := 0; i < maxN; i++ {
		list.DeleteByKey(Element(maxN - i - 1))
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	list = New(memStore)
	// Delete elements at random positions in the list.
	rList := rand.Perm(maxN)
	for _, e := range rList {
		list.InsertByKey(Element(e), 0, Element(e))
	}
	for _, e := range rList {
		list.DeleteByKey(Element(e))
	}
	if !list.IsEmpty() {
		t.Fail()
	}
}

func TestNext(t *testing.T) {
	list := New(memStore)

	for i := 0; i < maxN; i++ {
		list.InsertByKey(Element(i), 0, Element(i))
	}

	smallest, _ := list.GetSmallestNode()
	largest, _ := list.GetLargestNode()

	lastNode := smallest
	node := lastNode
	for node != largest {
		node, _ = list.Next(node)
		// Must always be incrementing here!
		if bytes.Compare(node.Key, lastNode.Key) <= 0 {
			t.Fail()
		}
		// Next.Prev must always point to itself!
		prevNode, _ := list.Prev(node)
		nextNode, _ := list.Next(prevNode)
		if nextNode != node {
			t.Fail()
		}
		lastNode = node
	}

	if nextNode, _ := list.Next(largest); nextNode != smallest {
		t.Fail()
	}
}

func TestPrev(t *testing.T) {
	list := New(memStore)

	for i := 0; i < maxN; i++ {
		list.InsertByKey(Element(i), 0, Element(i))
	}

	smallest, _ := list.GetSmallestNode()
	largest, _ := list.GetLargestNode()

	lastNode := largest
	node := lastNode
	for node != smallest {
		node, _ = list.Prev(node)
		// Must always be incrementing here!
		if bytes.Compare(node.Key, lastNode.Key) >= 0 {
			t.Fail()
		}
		// Next.Prev must always point to itself!
		nextNode, _ := list.Next(node)
		prevNode, _ := list.Prev(nextNode)
		if prevNode != node {
			t.Fail()
		}
		lastNode = node
	}

	if prevNode, _ := list.Prev(smallest); prevNode != largest {
		t.Fail()
	}
}

func TestFindGreaterOrEqual(t *testing.T) {

	maxNumber := maxN * 100

	var list *SkipList
	var listPointer *SkipList

	// Test on empty list.
	if _, _, ok, _ := listPointer.FindGreaterOrEqual(Element(0)); ok {
		t.Errorf("found element 0 in an empty list")
	}

	list = New(memStore)

	for i := 0; i < maxN; i++ {
		list.InsertByKey(Element(rand.Intn(maxNumber)), 0, Element(i))
	}

	for i := 0; i < maxN; i++ {
		key := Element(rand.Intn(maxNumber))
		if _, v, ok, _ := list.FindGreaterOrEqual(key); ok {
			// if f is v should be bigger than the element before
			if v.Prev != nil && bytes.Compare(key, v.Prev.Key) < 0 {
				t.Errorf("PrevV: %s\n    key: %s\n\n", string(v.Prev.Key), string(key))
			}
			// v should be bigger or equal to f
			// If we compare directly, we get an equal key with a difference on the 10th decimal point, which fails.
			if bytes.Compare(v.Key, key) < 0 {
				t.Errorf("v: %s\n    key: %s\n\n", string(v.Key), string(key))
			}
		} else {
			lastNode, _ := list.GetLargestNode()
			lastV := lastNode.GetValue()
			// It is OK, to fail, as long as f is bigger than the last element.
			if bytes.Compare(key, lastV) <= 0 {
				t.Errorf("lastV: %s\n    key: %s\n\n", string(lastV), string(key))
			}
		}
	}

}

func TestChangeValue(t *testing.T) {
	list := New(memStore)

	for i := 0; i < maxN; i++ {
		list.InsertByKey(Element(i), 0, []byte("value"))
	}

	for i := 0; i < maxN; i++ {
		// The key only looks at the int so the string doesn't matter here!
		_, f1, ok, _ := list.Find(Element(i))
		if !ok {
			t.Fail()
		}
		err := list.ChangeValue(f1, []byte("different value"))
		if err != nil {
			t.Fail()
		}
		_, f2, ok, _ := list.Find(Element(i))
		if !ok {
			t.Fail()
		}
		if bytes.Compare(f2.GetValue(), []byte("different value")) != 0 {
			t.Fail()
		}
	}
}
