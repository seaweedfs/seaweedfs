package logbuffer

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var (
	// Global is the singleton ring buffer instance.
	// Nil when log buffering is disabled (capacity=0).
	Global *RingBuffer

	revertMu    sync.Mutex
	revertTimer *time.Timer
)

// Init initializes the global log buffer and hooks into glog.
// Call this at program startup before any logging.
// A capacity of 0 disables log buffering entirely.
func Init(capacity int) {
	if capacity <= 0 {
		return
	}

	Global = NewRingBuffer(capacity)

	glog.LogInterceptor = func(level int32, msg []byte) {
		// Make a copy since glog reuses the buffer
		copied := make([]byte, len(msg))
		copy(copied, msg)
		entry := ParseLogLine(level, copied)
		Global.Write(entry)
	}
}

// SetVerbosityWithTTL changes the glog verbosity and optionally reverts after ttlMinutes.
// Returns the previous verbosity level.
func SetVerbosityWithTTL(v int32, ttlMinutes int) (previous int32, revertAt time.Time) {
	previous = glog.GetVerbosity()
	glog.SetVerbosity(v)

	revertMu.Lock()
	defer revertMu.Unlock()

	// Cancel any existing revert timer
	if revertTimer != nil {
		revertTimer.Stop()
		revertTimer = nil
	}

	if ttlMinutes > 0 {
		revertAt = time.Now().Add(time.Duration(ttlMinutes) * time.Minute)
		revertTimer = time.AfterFunc(time.Duration(ttlMinutes)*time.Minute, func() {
			glog.SetVerbosity(previous)
		})
	}

	return previous, revertAt
}
