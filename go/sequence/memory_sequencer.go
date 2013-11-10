package sequence

import ()

// just for testing
type MemorySequencer struct {
	counter uint64
}

func NewMemorySequencer() (m *MemorySequencer) {
	m = &MemorySequencer{counter: 1}
	return
}

func (m *MemorySequencer) NextFileId(count int) (uint64, int) {
	ret := m.counter
	m.counter += uint64(count)
	return ret, count
}
