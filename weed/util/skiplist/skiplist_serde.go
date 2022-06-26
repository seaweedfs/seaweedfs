package skiplist

import "bytes"

func compareElement(a *SkipListElement, key []byte) int {
	if len(a.Key) == 0 {
		return -1
	}
	return bytes.Compare(a.Key, key)
}

func (node *SkipListElement) Reference() *SkipListElementReference {
	if node == nil {
		return nil
	}
	return &SkipListElementReference{
		ElementPointer: node.Id,
		Key:            node.Key,
	}
}

func (t *SkipList) SaveElement(element *SkipListElement) error {
	if element == nil {
		return nil
	}
	return t.ListStore.SaveElement(element.Id, element)
}

func (t *SkipList) DeleteElement(element *SkipListElement) error {
	if element == nil {
		return nil
	}
	return t.ListStore.DeleteElement(element.Id)
}

func (t *SkipList) LoadElement(ref *SkipListElementReference) (*SkipListElement, error) {
	if ref.IsNil() {
		return nil, nil
	}
	return t.ListStore.LoadElement(ref.ElementPointer)
}

func (ref *SkipListElementReference) IsNil() bool {
	if ref == nil {
		return true
	}
	if len(ref.Key) == 0 {
		return true
	}
	return false
}
