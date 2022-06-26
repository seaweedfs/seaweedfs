package skiplist

type ListStore interface {
	SaveElement(id int64, element *SkipListElement) error
	DeleteElement(id int64) error
	LoadElement(id int64) (*SkipListElement, error)
}

type MemStore struct {
	m map[int64]*SkipListElement
}

func newMemStore() *MemStore {
	return &MemStore{
		m: make(map[int64]*SkipListElement),
	}
}

func (m *MemStore) SaveElement(id int64, element *SkipListElement) error {
	m.m[id] = element
	return nil
}

func (m *MemStore) DeleteElement(id int64) error {
	delete(m.m, id)
	return nil
}

func (m *MemStore) LoadElement(id int64) (*SkipListElement, error) {
	element := m.m[id]
	return element, nil
}
