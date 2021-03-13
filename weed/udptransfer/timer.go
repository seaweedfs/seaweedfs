package udptransfer

import "time"

const Millisecond = 1e6

func Now() int64 {
	return time.Now().UnixNano()/Millisecond
}

func NowNS() int64 {
	return time.Now().UnixNano()
}

func NewTimerChan(d int64) <-chan time.Time {
	ticker := time.NewTimer(time.Duration(d) * time.Millisecond)
	return ticker.C
}
