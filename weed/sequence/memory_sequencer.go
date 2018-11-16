package sequence

import (
	"sync"
	"time"
)

/**
Prevent duplicate numbering caused by heartbeat failure
*/
const (
	LenTimeStamp = 32
	LenSequence  = 32 // Num of Sequence Bits

	MaxSequence  = 1<<LenSequence - 1
	MaxTimeStamp = 1<<LenTimeStamp - 1
)

// just for testing
type MemorySequencer struct {
	counter      uint64
	sequenceLock sync.Mutex
}

func NewMemorySequencer() (m *MemorySequencer) {
	nowStamp := uint64(time.Now().UnixNano() / 1e6)
	m = &MemorySequencer{
		counter:      (nowStamp&MaxTimeStamp)<<LenSequence | 1,
		sequenceLock: sync.Mutex{}}
	return
}

func (m *MemorySequencer) NextFileId(count uint64) (uint64, uint64) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	ret := m.counter
	m.counter += uint64(count)
	return ret, count
}

func (m *MemorySequencer) SetMax(seenValue uint64) {
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	if m.counter <= seenValue {
		m.counter = seenValue + 1
	}
}

func (m *MemorySequencer) Peek() uint64 {
	return m.counter
}
