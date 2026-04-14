package page_writer

import (
	"sync"
)

// WriteBufferAccountant enforces a global byte budget across all
// UploadPipeline instances. Callers Reserve chunk-sized slots before
// allocating a page chunk (in memory or on swap) and Release them when
// the chunk is freed. Reserve blocks when the cap would be exceeded,
// providing natural backpressure to the FUSE write path when volume
// uploads stall (e.g. all assigned volumes are full) instead of letting
// the swap file grow without bound.
//
// A nil receiver is treated as "unlimited" for backward compatibility.
type WriteBufferAccountant struct {
	mu   sync.Mutex
	cond *sync.Cond
	cap  int64 // 0 means unlimited
	used int64
}

func NewWriteBufferAccountant(capBytes int64) *WriteBufferAccountant {
	a := &WriteBufferAccountant{cap: capBytes}
	a.cond = sync.NewCond(&a.mu)
	return a
}

// Reserve blocks until n bytes can be accounted for under the cap.
// It must not be called while holding any UploadPipeline lock, or the
// uploader goroutines that eventually call Release will deadlock.
func (a *WriteBufferAccountant) Reserve(n int64) {
	if a == nil || a.cap <= 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.used+n > a.cap && a.used > 0 {
		a.cond.Wait()
	}
	a.used += n
}

func (a *WriteBufferAccountant) Release(n int64) {
	if a == nil || a.cap <= 0 {
		return
	}
	a.mu.Lock()
	a.used -= n
	if a.used < 0 {
		a.used = 0
	}
	a.cond.Broadcast()
	a.mu.Unlock()
}

// Used returns the currently reserved byte count (for tests/metrics).
func (a *WriteBufferAccountant) Used() int64 {
	if a == nil || a.cap <= 0 {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used
}
