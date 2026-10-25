package mount

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

const (
	proactiveFlushInterval  = 200 * time.Millisecond
	proactiveIdleThreshold  = 500 * time.Millisecond
	proactiveMaxHoldTime    = 5 * time.Second
	proactiveFillRatioNumer = 9
	proactiveFillRatioDenom = 10
	proactiveFrontierLag    = 2
)

func (wfs *WFS) loopProactiveFlush() {
	ticker := time.NewTicker(proactiveFlushInterval)
	defer ticker.Stop()

	glog.V(0).Infof("proactive chunk flusher started (idle=%v maxHold=%v)", proactiveIdleThreshold, proactiveMaxHoldTime)

	for range ticker.C {
		wfs.proactiveFlushOnce()
	}
}

func (wfs *WFS) proactiveFlushOnce() {
	nowNs := time.Now().UnixNano()
	idleNs := proactiveIdleThreshold.Nanoseconds()
	maxHoldNs := proactiveMaxHoldTime.Nanoseconds()
	fillRatio := wfs.option.ChunkSizeLimit * proactiveFillRatioNumer / proactiveFillRatioDenom

	var handles []*FileHandle
	wfs.fhMap.RLock()
	for _, fh := range wfs.fhMap.inode2fh {
		if fh != nil && fh.dirtyPages != nil {
			handles = append(handles, fh)
		}
	}
	wfs.fhMap.RUnlock()

	for _, fh := range handles {
		isSeq := fh.dirtyPages.writerPattern.IsSequentialMode()
		fh.dirtyPages.ProactiveFlush(nowNs, idleNs, maxHoldNs, fillRatio, proactiveFrontierLag, isSeq)
	}
}
