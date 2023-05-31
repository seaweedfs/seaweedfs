package page_writer

import "time"

type ActivityScore struct {
	lastActiveTsNs         int64
	decayedActivenessScore int64
}

func NewActivityScore() *ActivityScore {
	return &ActivityScore{}
}

func (as ActivityScore) MarkRead() {
	now := time.Now().UnixNano()
	deltaTime := (now - as.lastActiveTsNs) >> 30 // about number of seconds
	as.lastActiveTsNs = now

	as.decayedActivenessScore = as.decayedActivenessScore>>deltaTime + 256
	if as.decayedActivenessScore < 0 {
		as.decayedActivenessScore = 0
	}
}

func (as ActivityScore) MarkWrite() {
	now := time.Now().UnixNano()
	deltaTime := (now - as.lastActiveTsNs) >> 30 // about number of seconds
	as.lastActiveTsNs = now

	as.decayedActivenessScore = as.decayedActivenessScore>>deltaTime + 1024
	if as.decayedActivenessScore < 0 {
		as.decayedActivenessScore = 0
	}
}

func (as ActivityScore) ActivityScore() int64 {
	deltaTime := (time.Now().UnixNano() - as.lastActiveTsNs) >> 30 // about number of seconds
	return as.decayedActivenessScore >> deltaTime
}
