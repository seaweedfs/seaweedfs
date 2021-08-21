package tree_store

import "errors"

var (
	NotFound = errors.New("not found")
)

type MemoryTreeStore struct {
	m map[int64][]byte
}

func NewMemoryTreeStore() *MemoryTreeStore{
	return &MemoryTreeStore{
		m: make(map[int64][]byte),
	}
}

func (m *MemoryTreeStore) Put(k int64, v []byte) error {
	m.m[k] = v
	return nil
}

func (m *MemoryTreeStore) Get(k int64) ([]byte, error) {
	if v, found := m.m[k]; found {
		return v, nil
	}
	return nil, NotFound
}
