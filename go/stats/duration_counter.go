package stats

import (
	"sync"
	"time"
)

type TimedValue struct {
	t   time.Time
	val int64
}

func NewTimedValue(t time.Time, val int64) *TimedValue {
	return &TimedValue{t: t, val: val}
}

type RoundRobinCounter struct {
	lastIndex int
	values    []int64
	counts    []int64
	mutex     sync.RWMutex
}

func NewRoundRobinCounter(slots int) *RoundRobinCounter {
	return &RoundRobinCounter{lastIndex: -1, values: make([]int64, slots), counts: make([]int64, slots)}
}
func (rrc *RoundRobinCounter) Add(index int, val int64) {
	rrc.mutex.Lock()
	defer rrc.mutex.Unlock()
	if index >= len(rrc.values) {
		return
	}
	for rrc.lastIndex != index {
		rrc.lastIndex = (rrc.lastIndex + 1) % len(rrc.values)
		rrc.values[rrc.lastIndex] = 0
		rrc.counts[rrc.lastIndex] = 0
	}
	rrc.values[index] += val
	rrc.counts[index]++
}
func (rrc *RoundRobinCounter) Max() (max int64) {
	rrc.mutex.RLock()
	defer rrc.mutex.RUnlock()
	for _, val := range rrc.values {
		if max < val {
			max = val
		}
	}
	return
}
func (rrc *RoundRobinCounter) Count() (cnt int64) {
	rrc.mutex.RLock()
	defer rrc.mutex.RUnlock()
	for _, c := range rrc.counts {
		cnt += c
	}
	return
}
func (rrc *RoundRobinCounter) Sum() (sum int64) {
	rrc.mutex.RLock()
	defer rrc.mutex.RUnlock()
	for _, val := range rrc.values {
		sum += val
	}
	return
}

func (rrc *RoundRobinCounter) ToList() (ret []int64) {
	rrc.mutex.RLock()
	defer rrc.mutex.RUnlock()
	index := rrc.lastIndex
	step := len(rrc.values)
	for step > 0 {
		step--
		index++
		if index >= len(rrc.values) {
			index = 0
		}
		ret = append(ret, rrc.values[index])
	}
	return
}

type DurationCounter struct {
	MinuteCounter *RoundRobinCounter
	HourCounter   *RoundRobinCounter
	DayCounter    *RoundRobinCounter
	WeekCounter   *RoundRobinCounter
}

func NewDurationCounter() *DurationCounter {
	return &DurationCounter{
		MinuteCounter: NewRoundRobinCounter(60),
		HourCounter:   NewRoundRobinCounter(60),
		DayCounter:    NewRoundRobinCounter(24),
		WeekCounter:   NewRoundRobinCounter(7),
	}
}

// Add is for cumulative counts
func (sc *DurationCounter) Add(tv *TimedValue) {
	sc.MinuteCounter.Add(tv.t.Second(), tv.val)
	sc.HourCounter.Add(tv.t.Minute(), tv.val)
	sc.DayCounter.Add(tv.t.Hour(), tv.val)
	sc.WeekCounter.Add(int(tv.t.Weekday()), tv.val)
}
