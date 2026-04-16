package page_writer

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// softThresholdRatio is the fraction of cap at which soft throttling begins.
	// At this level, writes sleep briefly to let uploaders drain.
	softThresholdRatio = 0.8
	// hardThresholdRatio is the fraction of cap at which hard throttling kicks in.
	// Writes sleep longer to aggressively slow intake.
	hardThresholdRatio = 0.95
	// softThrottleDelay is the sleep duration when usage exceeds the soft threshold.
	softThrottleDelay = 10 * time.Millisecond
	// hardThrottleDelay is the sleep duration when usage exceeds the hard threshold.
	hardThrottleDelay = 50 * time.Millisecond
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
	mu            sync.Mutex
	cond          *sync.Cond
	cap           int64 // 0 means unlimited
	used          int64
	softThreshold int64 // pre-computed: cap * softThresholdRatio
	hardThreshold int64 // pre-computed: cap * hardThresholdRatio
	evictor       func(needBytes int64) bool
	evicting      bool

	softThrottleCount atomic.Int64
	hardThrottleCount atomic.Int64
}

func NewWriteBufferAccountant(capBytes int64) *WriteBufferAccountant {
	a := &WriteBufferAccountant{
		cap:           capBytes,
		softThreshold: int64(float64(capBytes) * softThresholdRatio),
		hardThreshold: int64(float64(capBytes) * hardThresholdRatio),
	}
	a.cond = sync.NewCond(&a.mu)
	return a
}

// SetEvictor registers a callback that Reserve invokes when the cap would
// otherwise block. The evictor is expected to force-seal at least one
// writable chunk in some UploadPipeline, which turns a pinned-forever
// writable chunk into a sealed chunk that the async uploader drains and
// Releases. Without this hook, workloads that hold many files open for
// write with less-than-chunkSize data in each (e.g. fio 4k randwrite with
// nrfiles * chunkSize > cap) deadlock permanently, because writable chunks
// only seal on close or when they fill.
//
// The evictor must not call Reserve on the same accountant or re-enter
// Reserve transitively — it would deadlock on accountant.mu.
func (a *WriteBufferAccountant) SetEvictor(fn func(needBytes int64) bool) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.evictor = fn
	a.mu.Unlock()
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

	// Graduated backpressure: slow writers before hitting the hard cap.
	if a.used > a.hardThreshold {
		a.hardThrottleCount.Add(1)
		a.mu.Unlock()
		time.Sleep(hardThrottleDelay)
		a.mu.Lock()
	} else if a.used > a.softThreshold {
		a.softThrottleCount.Add(1)
		a.mu.Unlock()
		time.Sleep(softThrottleDelay)
		a.mu.Lock()
	}

	for a.used+n > a.cap && a.used > 0 {
		// Before blocking, try to force-seal a writable chunk somewhere so
		// its async upload path will eventually Release a slot. Single-flight
		// on `evicting` so a stampede of blocked reservers doesn't iterate
		// the fhMap concurrently.
		if a.evictor != nil && !a.evicting {
			a.runEvictorLocked(n)
			// A concurrent Release may have brought used back under the
			// cap during the evict window — re-check before waiting so we
			// do not block on a broadcast that has already fired.
			if a.used+n <= a.cap || a.used == 0 {
				break
			}
		}
		a.cond.Wait()
	}
	a.used += n
}

// runEvictorLocked is called with a.mu held and !a.evicting. It drops the
// lock around the evictor invocation so uploader goroutines (which call
// Release under the same lock) can make progress, and uses defer to
// guarantee the `evicting` flag and the lock are restored even if the
// evictor panics.
func (a *WriteBufferAccountant) runEvictorLocked(n int64) {
	evictor := a.evictor
	a.evicting = true
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.evicting = false
		a.cond.Broadcast()
	}()
	evictor(n)
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

// SoftThrottleCount returns the number of times soft throttling was triggered.
func (a *WriteBufferAccountant) SoftThrottleCount() int64 {
	if a == nil {
		return 0
	}
	return a.softThrottleCount.Load()
}

// HardThrottleCount returns the number of times hard throttling was triggered.
func (a *WriteBufferAccountant) HardThrottleCount() int64 {
	if a == nil {
		return 0
	}
	return a.hardThrottleCount.Load()
}
