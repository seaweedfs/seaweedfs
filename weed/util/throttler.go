package util

import "time"

type WriteThrottler struct {
	compactionBytePerSecond int64
	lastSizeCounter         int64
	lastSizeCheckTime       time.Time
}

func NewWriteThrottler(bytesPerSecond int64) *WriteThrottler {
	return &WriteThrottler{
		compactionBytePerSecond: bytesPerSecond,
		lastSizeCheckTime:       time.Now(),
	}
}

func (wt *WriteThrottler) MaybeSlowdown(delta int64) {
	if wt.compactionBytePerSecond > 0 {
		wt.lastSizeCounter += delta
		now := time.Now()
		elapsedDuration := now.Sub(wt.lastSizeCheckTime)
		if elapsedDuration > 100*time.Millisecond {
			overLimitBytes := wt.lastSizeCounter - wt.compactionBytePerSecond/10
			if overLimitBytes > 0 {
				overRatio := float64(overLimitBytes) / float64(wt.compactionBytePerSecond)
				sleepTime := time.Duration(overRatio*1000) * time.Millisecond
				// glog.V(0).Infof("currently %d bytes, limit to %d bytes, over by %d bytes, sleeping %v over %.4f", wt.lastSizeCounter, wt.compactionBytePerSecond/10, overLimitBytes, sleepTime, overRatio)
				time.Sleep(sleepTime)
			}
			wt.lastSizeCounter, wt.lastSizeCheckTime = 0, time.Now()
		}
	}
}
