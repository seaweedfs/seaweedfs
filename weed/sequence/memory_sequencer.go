package sequence

import (
	"sync"
)

// default Sequencer
type MemorySequencer struct {
	counter      uint64
	sequenceLock sync.Mutex
}

func NewMemorySequencer() (m *MemorySequencer) {
	m = &MemorySequencer{counter: 1}
	return
}

func (m *MemorySequencer) NextFileId(count uint64) uint64 {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	ret := m.counter
	m.counter += count
	return ret
}

func (m *MemorySequencer) SetMax(seenValue uint64) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	if m.counter <= seenValue {
		m.counter = seenValue + 1
	}
}
