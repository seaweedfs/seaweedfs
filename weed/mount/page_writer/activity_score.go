package page_writer

import (
	"sync/atomic"
	"time"
)

type ActivityScore struct {
	lastActiveTsNs         int64 // atomic
	decayedActivenessScore int64 // atomic
}

func NewActivityScore() *ActivityScore {
	return &ActivityScore{}
}

func (as *ActivityScore) MarkRead() {
	now := time.Now().UnixNano()
	last := atomic.SwapInt64(&as.lastActiveTsNs, now)
	deltaTime := (now - last) >> 30 // about number of seconds
	for {
		old := atomic.LoadInt64(&as.decayedActivenessScore)
		score := old>>deltaTime + 256
		if score < 0 {
			score = 0
		}
		if atomic.CompareAndSwapInt64(&as.decayedActivenessScore, old, score) {
			break
		}
	}
}

func (as *ActivityScore) MarkWrite() {
	now := time.Now().UnixNano()
	last := atomic.SwapInt64(&as.lastActiveTsNs, now)
	deltaTime := (now - last) >> 30 // about number of seconds
	for {
		old := atomic.LoadInt64(&as.decayedActivenessScore)
		score := old>>deltaTime + 1024
		if score < 0 {
			score = 0
		}
		if atomic.CompareAndSwapInt64(&as.decayedActivenessScore, old, score) {
			break
		}
	}
}

func (as *ActivityScore) ActivityScore() int64 {
	deltaTime := (time.Now().UnixNano() - atomic.LoadInt64(&as.lastActiveTsNs)) >> 30
	return atomic.LoadInt64(&as.decayedActivenessScore) >> deltaTime
}
