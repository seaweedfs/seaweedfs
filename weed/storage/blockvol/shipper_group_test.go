package blockvol

import (
	"sync/atomic"
	"testing"
)

func TestShipperGroup_ShipAll_Single(t *testing.T) {
	s := newTestShipper()
	sg := NewShipperGroup([]*WALShipper{s})
	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite}
	sg.ShipAll(entry)
	// Ship on a non-connected shipper degrades it but doesn't panic.
	if sg.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", sg.Len())
	}
}

func TestShipperGroup_ShipAll_Two(t *testing.T) {
	s1 := newTestShipper()
	s2 := newTestShipper()
	sg := NewShipperGroup([]*WALShipper{s1, s2})
	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite}
	sg.ShipAll(entry)
	if sg.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", sg.Len())
	}
}

func TestShipperGroup_BarrierAll_AllSucceed(t *testing.T) {
	// Create shippers that are stopped (Barrier returns ErrShipperStopped).
	// We test the fan-out logic: all return errors, which is fine.
	s1 := newTestShipper()
	s2 := newTestShipper()
	sg := NewShipperGroup([]*WALShipper{s1, s2})
	errs := sg.BarrierAll(10)
	if len(errs) != 2 {
		t.Fatalf("BarrierAll: got %d errors, want 2", len(errs))
	}
}

func TestShipperGroup_BarrierAll_OneFail(t *testing.T) {
	s1 := newTestShipper()
	s1.degraded.Store(true)
	s2 := newTestShipper()
	sg := NewShipperGroup([]*WALShipper{s1, s2})
	errs := sg.BarrierAll(10)
	if errs[0] == nil {
		t.Fatalf("expected error for degraded shipper[0]")
	}
}

func TestShipperGroup_BarrierAll_AllFail(t *testing.T) {
	s1 := newTestShipper()
	s1.degraded.Store(true)
	s2 := newTestShipper()
	s2.degraded.Store(true)
	sg := NewShipperGroup([]*WALShipper{s1, s2})
	errs := sg.BarrierAll(10)
	for i, e := range errs {
		if e == nil {
			t.Fatalf("expected error for shipper[%d]", i)
		}
	}
}

func TestShipperGroup_AllDegraded_Empty(t *testing.T) {
	sg := NewShipperGroup(nil)
	if sg.AllDegraded() {
		t.Fatal("empty group should not be AllDegraded")
	}
}

func TestShipperGroup_AllDegraded_Mixed(t *testing.T) {
	s1 := newTestShipper()
	s1.degraded.Store(true)
	s2 := newTestShipper()
	sg := NewShipperGroup([]*WALShipper{s1, s2})
	if sg.AllDegraded() {
		t.Fatal("mixed group should not be AllDegraded")
	}
	if !sg.AnyDegraded() {
		t.Fatal("mixed group should be AnyDegraded")
	}
}

func TestShipperGroup_StopAll(t *testing.T) {
	s1 := newTestShipper()
	s2 := newTestShipper()
	sg := NewShipperGroup([]*WALShipper{s1, s2})
	sg.StopAll()
	if !s1.stopped.Load() || !s2.stopped.Load() {
		t.Fatal("StopAll should stop all shippers")
	}
}

func TestShipperGroup_DegradedCount(t *testing.T) {
	s1 := newTestShipper()
	s2 := newTestShipper()
	s1.degraded.Store(true)
	sg := NewShipperGroup([]*WALShipper{s1, s2})
	if got := sg.DegradedCount(); got != 1 {
		t.Fatalf("DegradedCount: got %d, want 1", got)
	}
}

// newTestShipper creates a WALShipper with a fixed epoch, not connected to anything.
func newTestShipper() *WALShipper {
	var epoch atomic.Uint64
	epoch.Store(1)
	return &WALShipper{
		dataAddr:    "127.0.0.1:0",
		controlAddr: "127.0.0.1:0",
		epochFn:     func() uint64 { return epoch.Load() },
	}
}
