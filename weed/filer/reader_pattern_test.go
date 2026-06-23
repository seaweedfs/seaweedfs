package filer

import (
	"sync/atomic"
	"testing"
)

const mb = 1 << 20

func TestReaderPatternSequentialAndHysteresis(t *testing.T) {
	rp := NewReaderPattern()
	rp.MonitorReadAt(0, mb)    // near(0 vs frontier 0) -> +1
	rp.MonitorReadAt(mb, mb)   // +2
	rp.MonitorReadAt(2*mb, mb) // +3 (capped)
	if rp.IsRandomMode() {
		t.Fatal("a stream from offset 0 must not be random")
	}
	// a single far outlier does not flip to random (hysteresis): +3 -> +2
	rp.MonitorReadAt(900*mb, mb)
	if rp.IsRandomMode() {
		t.Fatal("a single outlier read must not flip to random mode")
	}
}

func TestReaderPatternFarFirstReadIsRandom(t *testing.T) {
	rp := NewReaderPattern()
	rp.MonitorReadAt(500*mb, mb) // far from frontier 0 -> -1
	if !rp.IsRandomMode() {
		t.Fatal("a far first read should be random")
	}
}

func TestReaderPatternToleranceAbsorbsReorder(t *testing.T) {
	rp := NewReaderPattern()
	rp.MonitorReadAt(0, mb)    // +1, frontier 1MB
	rp.MonitorReadAt(6*mb, mb) // |6-1|=5MB <= 8MB tolerance -> near, +2, frontier 7MB
	rp.MonitorReadAt(3*mb, mb) // |3-7|=4MB <= 8MB -> near, +3
	if rp.IsRandomMode() {
		t.Fatal("reordered reads within tolerance must stay sequential")
	}
}

func TestReaderPatternRandomRecoveryNeedsHysteresis(t *testing.T) {
	rp := NewReaderPattern()
	rp.MonitorReadAt(100*mb, mb) // far -> -1
	rp.MonitorReadAt(300*mb, mb) // far -> -2
	rp.MonitorReadAt(500*mb, mb) // far -> -3 (capped), frontier 501MB
	// a single near read must not immediately flip back to sequential: -3 -> -2
	rp.MonitorReadAt(501*mb, mb)
	if !rp.IsRandomMode() {
		t.Fatal("one near read must not flip back from deep random mode")
	}
}

// The max-frontier (vs the old atomic.Swap of the last read's stop offset) is the
// load-bearing difference of this detector: the frontier must never move backward,
// or a backward read would lower the baseline and make later far reads look near.
func TestReaderPatternFrontierNeverRegresses(t *testing.T) {
	rp := NewReaderPattern()
	rp.MonitorReadAt(500*mb, mb) // far first read -> -1, frontier 501MB
	rp.MonitorReadAt(0, mb)      // a backward read must not pull the frontier back
	if got := atomic.LoadInt64(&rp.readFrontier); got != 501*mb {
		t.Fatalf("frontier regressed to %d; the max-frontier must never move backward", got)
	}
	// Behavioral consequence: reads far below the preserved frontier stay random.
	// Had the frontier regressed to ~1MB, this would wrongly read as sequential.
	if !rp.IsRandomMode() {
		t.Fatal("reads far below the preserved frontier must remain random")
	}
}

// SeqTolerance is the central new tuning knob; pin both sides of the inclusive
// boundary so an off-by-one or a '<' vs '<=' change can't slip through silently.
func TestReaderPatternToleranceBoundary(t *testing.T) {
	atBoundary := NewReaderPattern()
	atBoundary.MonitorReadAt(SeqTolerance, 0) // diff == SeqTolerance -> near (inclusive), +1
	if atBoundary.IsRandomMode() {
		t.Fatal("a read exactly at frontier+SeqTolerance must count as sequential")
	}
	pastBoundary := NewReaderPattern()
	pastBoundary.MonitorReadAt(SeqTolerance+1, 0) // diff == SeqTolerance+1 -> far, -1
	if !pastBoundary.IsRandomMode() {
		t.Fatal("a read one byte past frontier+SeqTolerance must count as random")
	}
}

// The escape from random mode: sustained near reads must climb the counter back
// out of the negative floor and re-enter sequential (whole-chunk cache) mode.
func TestReaderPatternRecoversFromRandom(t *testing.T) {
	rp := NewReaderPattern()
	rp.MonitorReadAt(100*mb, mb) // far -> -1
	rp.MonitorReadAt(300*mb, mb) // far -> -2
	rp.MonitorReadAt(500*mb, mb) // far -> -3 (capped), frontier 501MB
	if !rp.IsRandomMode() {
		t.Fatal("three far reads must be random")
	}
	// contiguous near reads: -3 -> -2 -> -1 -> 0 -> +1, back out of random mode
	for i := int64(0); i < 4; i++ {
		rp.MonitorReadAt(501*mb+i*mb, mb)
	}
	if rp.IsRandomMode() {
		t.Fatal("sustained near reads must recover sequential mode")
	}
}
