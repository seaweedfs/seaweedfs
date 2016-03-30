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

var (
	ServStats = NewServerStats()
)

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
	ServStats.Connections.Add(NewTimedValue(time.Now(), 1))
}
func ConnectionClose() {
	ServStats.Connections.Add(NewTimedValue(time.Now(), -1))
}
func RequestOpen() {
	ServStats.Requests.Add(NewTimedValue(time.Now(), 1))
}
func RequestClose() {
	ServStats.Requests.Add(NewTimedValue(time.Now(), 1))

}
func AssignRequest() {
	ServStats.AssignRequests.Add(NewTimedValue(time.Now(), 1))
}
func ReadRequest() {
	ServStats.ReadRequests.Add(NewTimedValue(time.Now(), 1))
}
func WriteRequest() {
	ServStats.WriteRequests.Add(NewTimedValue(time.Now(), 1))
}
func DeleteRequest() {
	ServStats.DeleteRequests.Add(NewTimedValue(time.Now(), 1))
}
func BytesIn(val int64) {
	ServStats.BytesIn.Add(NewTimedValue(time.Now(), 1))
}
func BytesOut(val int64) {
	ServStats.BytesOut.Add(NewTimedValue(time.Now(), 1))
}
