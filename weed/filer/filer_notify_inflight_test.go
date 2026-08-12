package filer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// TestMetaLogInflightFloor pins the in-flight stamp bookkeeping backing
// Filer.LocalFlushedThroughTsNs: the floor is the oldest outstanding stamp,
// duplicate stamps are reference-counted, and a cleared registry reports zero.
func TestMetaLogInflightFloor(t *testing.T) {
	var inflight metaLogInflight
	if got := inflight.minTsNs(); got != 0 {
		t.Fatalf("empty registry: floor=%d want 0", got)
	}
	ts1 := inflight.stamp()
	ts2 := inflight.stamp()
	if ts2 < ts1 {
		t.Fatalf("stamps not monotonic: %d then %d", ts1, ts2)
	}
	if got := inflight.minTsNs(); got != ts1 {
		t.Fatalf("floor=%d want oldest stamp %d", got, ts1)
	}
	inflight.done(ts1)
	if got := inflight.minTsNs(); got != ts2 {
		t.Fatalf("after done(ts1): floor=%d want %d", got, ts2)
	}
	inflight.done(ts2)
	if got := inflight.minTsNs(); got != 0 {
		t.Fatalf("cleared registry: floor=%d want 0", got)
	}
}

// TestLocalFlushedThroughTsNsBoundsInflight pins the flush-watermark claim: a
// drained buffer claims "now", but an event stamped and not yet appended caps
// the claim just below its timestamp - a peer bounding disk reads by the
// claim must not advance past an event still on its way into the buffer.
func TestLocalFlushedThroughTsNsBoundsInflight(t *testing.T) {
	f := &Filer{
		LocalMetaLogBuffer: log_buffer.NewLogBuffer("inflight-test", time.Minute, nil, nil, nil),
	}
	defer f.LocalMetaLogBuffer.ShutdownLogBuffer()

	now := time.Now().UnixNano()
	if got := f.LocalFlushedThroughTsNs(now); got != now {
		t.Fatalf("drained, nothing in flight: claim=%d want now=%d", got, now)
	}

	ts := f.metaLogInflight.stamp()
	if got := f.LocalFlushedThroughTsNs(time.Now().UnixNano()); got != ts-1 {
		t.Fatalf("with in-flight stamp %d: claim=%d want %d", ts, got, ts-1)
	}

	f.metaLogInflight.done(ts)
	now = time.Now().UnixNano()
	if got := f.LocalFlushedThroughTsNs(now); got != now {
		t.Fatalf("stamp cleared: claim=%d want now=%d", got, now)
	}
}
