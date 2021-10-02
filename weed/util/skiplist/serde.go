package skiplist

import "bytes"

func compareElement(a *SkipListElement, key []byte) int {
	if len(a.Values) == 0 {
		return -1
	}
	if bytes.Compare(a.Values[0], key) < 0 {
		return -1
	}
	if bytes.Compare(a.Values[len(a.Values)-1], key) > 0 {
		return 1
	}
	return 0
}

var (
	memStore = make(map[int64]*SkipListElement)
)

func (node *SkipListElement) Reference() *SkipListElementReference {
	if node == nil {
		return nil
	}
	return &SkipListElementReference{
		ElementPointer: node.Id,
		Key:            node.Values[0],
	}
}
func (node *SkipListElement) Save() {
	if node == nil {
		return
	}
	memStore[node.Id] = node
	//println("++ node", node.Id, string(node.Values[0]))
}

func (node *SkipListElement) DeleteSelf() {
	if node == nil {
		return
	}
	delete(memStore, node.Id)
	//println("++ node", node.Id, string(node.Values[0]))
}

func (ref *SkipListElementReference) Load() *SkipListElement {
	if ref == nil {
		return nil
	}
	//println("~ node", ref.ElementPointer, string(ref.Key))
	return memStore[ref.ElementPointer]
}

