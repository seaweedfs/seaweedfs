package mount

import (
	"sync/atomic"
	"testing"
)

const mb = 1 << 20

func TestWriterPatternSequentialAndHysteresis(t *testing.T) {
	wp := NewWriterPattern(2 * mb)
	wp.MonitorWriteAt(0, mb)    // near(0 vs frontier 0) -> +1
	wp.MonitorWriteAt(mb, mb)   // +2
	wp.MonitorWriteAt(2*mb, mb) // +3 (capped)
	if !wp.IsSequentialMode() {
		t.Fatal("a stream from offset 0 must be sequential")
	}
	// a single far outlier does not flip to random (hysteresis): +3 -> +2
	wp.MonitorWriteAt(900*mb, mb)
	if !wp.IsSequentialMode() {
		t.Fatal("a single outlier write must not flip out of sequential mode")
	}
}

func TestWriterPatternFarFirstWriteIsRandom(t *testing.T) {
	wp := NewWriterPattern(2 * mb)
	wp.MonitorWriteAt(500*mb, mb) // far from frontier 0 -> -1
	if wp.IsSequentialMode() {
		t.Fatal("a far first write should be random")
	}
}

func TestWriterPatternToleranceAbsorbsReorder(t *testing.T) {
	wp := NewWriterPattern(2 * mb)
	wp.MonitorWriteAt(0, mb)    // +1, frontier 1MB
	wp.MonitorWriteAt(6*mb, mb) // |6-1|=5MB <= 8MB tolerance -> near, +2, frontier 7MB
	wp.MonitorWriteAt(3*mb, mb) // |3-7|=4MB <= 8MB -> near, +3
	if !wp.IsSequentialMode() {
		t.Fatal("reordered writes within tolerance must stay sequential")
	}
}

func TestWriterPatternRandomRecoveryNeedsHysteresis(t *testing.T) {
	wp := NewWriterPattern(2 * mb)
	wp.MonitorWriteAt(100*mb, mb) // far -> -1
	wp.MonitorWriteAt(300*mb, mb) // far -> -2
	wp.MonitorWriteAt(500*mb, mb) // far -> -3 (capped), frontier 501MB
	// a single near write must not immediately flip back to sequential: -3 -> -2
	wp.MonitorWriteAt(501*mb, mb)
	if wp.IsSequentialMode() {
		t.Fatal("one near write must not flip back from deep random mode")
	}
}

// The max-frontier (vs the old atomic.Swap of the last write's stop offset) is the
// load-bearing difference of this detector: the frontier must never move backward,
// or a backward write would lower the baseline and make later far writes look near.
func TestWriterPatternFrontierNeverRegresses(t *testing.T) {
	wp := NewWriterPattern(2 * mb)
	wp.MonitorWriteAt(500*mb, mb) // far first write -> -1, frontier 501MB
	wp.MonitorWriteAt(0, mb)      // a backward write must not pull the frontier back
	if got := atomic.LoadInt64(&wp.writeFrontier); got != 501*mb {
		t.Fatalf("frontier regressed to %d; the max-frontier must never move backward", got)
	}
	// Behavioral consequence: writes far below the preserved frontier stay random.
	// Had the frontier regressed to ~1MB, this would wrongly read as sequential.
	if wp.IsSequentialMode() {
		t.Fatal("writes far below the preserved frontier must remain random")
	}
}

// SeqTolerance is the central new tuning knob; pin both sides of the inclusive
// boundary so an off-by-one or a '<' vs '<=' change can't slip through silently.
func TestWriterPatternToleranceBoundary(t *testing.T) {
	atBoundary := NewWriterPattern(2 * mb)
	atBoundary.MonitorWriteAt(SeqTolerance, 0) // diff == SeqTolerance -> near (inclusive), +1
	if !atBoundary.IsSequentialMode() {
		t.Fatal("a write exactly at frontier+SeqTolerance must count as sequential")
	}
	pastBoundary := NewWriterPattern(2 * mb)
	pastBoundary.MonitorWriteAt(SeqTolerance+1, 0) // diff == SeqTolerance+1 -> far, -1
	if pastBoundary.IsSequentialMode() {
		t.Fatal("a write one byte past frontier+SeqTolerance must count as random")
	}
}

// The perf-relevant escape from random (swap-file) mode: sustained near writes must
// climb the counter back out of the negative floor and re-enter sequential (RAM) mode.
func TestWriterPatternRecoversToSequential(t *testing.T) {
	wp := NewWriterPattern(2 * mb)
	wp.MonitorWriteAt(100*mb, mb) // far -> -1
	wp.MonitorWriteAt(300*mb, mb) // far -> -2
	wp.MonitorWriteAt(500*mb, mb) // far -> -3 (capped), frontier 501MB
	if wp.IsSequentialMode() {
		t.Fatal("three far writes must be random")
	}
	// contiguous near writes: -3 -> -2 -> -1 -> 0 -> +1, back into sequential mode
	for i := int64(0); i < 4; i++ {
		wp.MonitorWriteAt(501*mb+i*mb, mb)
	}
	if !wp.IsSequentialMode() {
		t.Fatal("sustained near writes must recover sequential mode (escape the swap-file cliff)")
	}
}
