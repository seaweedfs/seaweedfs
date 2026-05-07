package reader

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func key(b string, kind s3lifecycle.ActionKind) s3lifecycle.ActionKey {
	return s3lifecycle.ActionKey{Bucket: b, ActionKind: kind}
}

func TestCursorMinTsNsEmpty(t *testing.T) {
	c := NewCursor()
	if got := c.MinTsNs(); got != 0 {
		t.Fatalf("empty MinTsNs=%d, want 0", got)
	}
}

func TestCursorAdvanceMonotonic(t *testing.T) {
	c := NewCursor()
	k := key("b", s3lifecycle.ActionKindExpirationDays)
	c.Advance(k, 100)
	c.Advance(k, 50) // backward, ignored
	c.Advance(k, 100) // equal, ignored
	c.Advance(k, 200)
	if got := c.Get(k); got != 200 {
		t.Fatalf("Get=%d, want 200", got)
	}
}

func TestCursorAdvanceIgnoresZero(t *testing.T) {
	c := NewCursor()
	k := key("b", s3lifecycle.ActionKindExpirationDays)
	c.Advance(k, 0)
	c.Advance(k, -1)
	if got := c.Get(k); got != 0 {
		t.Fatalf("Get=%d, want 0", got)
	}
}

func TestCursorMinTsNsAcrossKeys(t *testing.T) {
	c := NewCursor()
	c.Advance(key("a", s3lifecycle.ActionKindExpirationDays), 100)
	c.Advance(key("b", s3lifecycle.ActionKindExpirationDays), 50)
	c.Advance(key("c", s3lifecycle.ActionKindAbortMPU), 200)
	if got := c.MinTsNs(); got != 50 {
		t.Fatalf("MinTsNs=%d, want 50", got)
	}
}

func TestCursorFreeze(t *testing.T) {
	c := NewCursor()
	k := key("b", s3lifecycle.ActionKindExpirationDays)
	c.Advance(k, 100)
	c.Freeze(k, 100)
	c.Advance(k, 200) // frozen — ignored
	if got := c.Get(k); got != 100 {
		t.Fatalf("Get after freeze=%d, want 100", got)
	}
	if !c.IsFrozen(k) {
		t.Fatal("IsFrozen=false, want true")
	}
	c.Unfreeze(k)
	c.Advance(k, 200)
	if got := c.Get(k); got != 200 {
		t.Fatalf("Get after unfreeze=%d, want 200", got)
	}
}

func TestCursorFreezeOnUnsetKeySeedsPosition(t *testing.T) {
	// Freezing a key with no recorded position seeds it at the freeze tsNs;
	// subsequent MinTsNs reflects the freeze, so the subscription doesn't
	// rewind past it.
	c := NewCursor()
	k := key("b", s3lifecycle.ActionKindExpirationDays)
	c.Freeze(k, 500)
	if got := c.Get(k); got != 500 {
		t.Fatalf("Get after freeze-on-unset=%d, want 500", got)
	}
	if got := c.MinTsNs(); got != 500 {
		t.Fatalf("MinTsNs=%d, want 500", got)
	}
}

func TestCursorSnapshotRestore(t *testing.T) {
	c := NewCursor()
	a := key("a", s3lifecycle.ActionKindExpirationDays)
	b := key("b", s3lifecycle.ActionKindAbortMPU)
	c.Advance(a, 100)
	c.Advance(b, 200)

	snap := c.Snapshot()
	if len(snap) != 2 || snap[a] != 100 || snap[b] != 200 {
		t.Fatalf("Snapshot=%v", snap)
	}

	c2 := NewCursor()
	c2.Restore(snap)
	if got := c2.Get(a); got != 100 {
		t.Fatalf("restored Get(a)=%d, want 100", got)
	}
	if got := c2.Get(b); got != 200 {
		t.Fatalf("restored Get(b)=%d, want 200", got)
	}
	if c2.IsFrozen(a) {
		t.Fatal("Restore should not carry freezes")
	}
}
