package filer

import "testing"

const mb = 1 << 20

func TestReaderPatternSequentialAndHysteresis(t *testing.T) {
	rp := NewReaderPattern()
	rp.MonitorReadAt(0, mb)     // near(0 vs frontier 0) -> +1
	rp.MonitorReadAt(mb, mb)    // +2
	rp.MonitorReadAt(2*mb, mb)  // +3 (capped)
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
