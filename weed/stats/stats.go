package stats

import (
	"time"
)

type ServerStats struct {
	Requests       *DurationCounter
	Connections    *DurationCounter
	AssignRequests *DurationCounter
	ReadRequests   *DurationCounter
	WriteRequests  *DurationCounter
	DeleteRequests *DurationCounter
	BytesIn        *DurationCounter
	BytesOut       *DurationCounter
}

type Channels struct {
	Connections    chan *TimedValue
	Requests       chan *TimedValue
	AssignRequests chan *TimedValue
	ReadRequests   chan *TimedValue
	WriteRequests  chan *TimedValue
	DeleteRequests chan *TimedValue
	BytesIn        chan *TimedValue
	BytesOut       chan *TimedValue
}

var (
	Chan *Channels
)

func init() {
	Chan = &Channels{
		Connections:    make(chan *TimedValue, 100),
		Requests:       make(chan *TimedValue, 100),
		AssignRequests: make(chan *TimedValue, 100),
		ReadRequests:   make(chan *TimedValue, 100),
		WriteRequests:  make(chan *TimedValue, 100),
		DeleteRequests: make(chan *TimedValue, 100),
		BytesIn:        make(chan *TimedValue, 100),
		BytesOut:       make(chan *TimedValue, 100),
	}
}

func NewServerStats() *ServerStats {
	return &ServerStats{
		Requests:       NewDurationCounter(),
		Connections:    NewDurationCounter(),
		AssignRequests: NewDurationCounter(),
		ReadRequests:   NewDurationCounter(),
		WriteRequests:  NewDurationCounter(),
		DeleteRequests: NewDurationCounter(),
		BytesIn:        NewDurationCounter(),
		BytesOut:       NewDurationCounter(),
	}
}

func ConnectionOpen() {
	Chan.Connections <- NewTimedValue(time.Now(), 1)
}
func ConnectionClose() {
	Chan.Connections <- NewTimedValue(time.Now(), -1)
}
func RequestOpen() {
	Chan.Requests <- NewTimedValue(time.Now(), 1)
}
func RequestClose() {
	Chan.Requests <- NewTimedValue(time.Now(), -1)
}
func AssignRequest() {
	Chan.AssignRequests <- NewTimedValue(time.Now(), 1)
}
func ReadRequest() {
	Chan.ReadRequests <- NewTimedValue(time.Now(), 1)
}
func WriteRequest() {
	Chan.WriteRequests <- NewTimedValue(time.Now(), 1)
}
func DeleteRequest() {
	Chan.DeleteRequests <- NewTimedValue(time.Now(), 1)
}
func BytesIn(val int64) {
	Chan.BytesIn <- NewTimedValue(time.Now(), val)
}
func BytesOut(val int64) {
	Chan.BytesOut <- NewTimedValue(time.Now(), val)
}

func (ss *ServerStats) Start() {
	for {
		select {
		case tv := <-Chan.Connections:
			ss.Connections.Add(tv)
		case tv := <-Chan.Requests:
			ss.Requests.Add(tv)
		case tv := <-Chan.AssignRequests:
			ss.AssignRequests.Add(tv)
		case tv := <-Chan.ReadRequests:
			ss.ReadRequests.Add(tv)
		case tv := <-Chan.WriteRequests:
			ss.WriteRequests.Add(tv)
		case tv := <-Chan.DeleteRequests:
			ss.DeleteRequests.Add(tv)
		case tv := <-Chan.BytesIn:
			ss.BytesIn.Add(tv)
		case tv := <-Chan.BytesOut:
			ss.BytesOut.Add(tv)
		}
	}
}
