package page_writer

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteBufferAccountant_Unlimited(t *testing.T) {
	a := NewWriteBufferAccountant(0)
	a.Reserve(1 << 30)
	a.Reserve(1 << 30)
	if got := a.Used(); got != 0 {
		t.Fatalf("unlimited accountant should not track usage, got %d", got)
	}
}

func TestWriteBufferAccountant_Nil(t *testing.T) {
	var a *WriteBufferAccountant
	a.Reserve(100) // must not panic
	a.Release(100)
	if a.Used() != 0 {
		t.Fatal("nil Used should return 0")
	}
}

func TestWriteBufferAccountant_ReserveAndRelease(t *testing.T) {
	a := NewWriteBufferAccountant(300)
	a.Reserve(100)
	a.Reserve(100)
	if got := a.Used(); got != 200 {
		t.Fatalf("expected 200 used, got %d", got)
	}
	a.Release(100)
	if got := a.Used(); got != 100 {
		t.Fatalf("expected 100 used after release, got %d", got)
	}
}

func TestWriteBufferAccountant_BlocksWhenOverCap(t *testing.T) {
	a := NewWriteBufferAccountant(100)
	a.Reserve(100)

	started := make(chan struct{})
	var landed atomic.Bool
	go func() {
		close(started)
		a.Reserve(50) // should block — used=100, cap=100
		landed.Store(true)
	}()

	// Wait until the goroutine is about to enter Reserve. Once it does,
	// Reserve cannot make progress until Release is called (cap is
	// full), so landed is guaranteed to stay false until we release.
	<-started
	if landed.Load() {
		t.Fatal("second reserve should be blocked while cap is full")
	}

	a.Release(100)

	deadline := time.Now().Add(time.Second)
	for !landed.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !landed.Load() {
		t.Fatal("second reserve should unblock after release")
	}
	if got := a.Used(); got != 50 {
		t.Fatalf("expected 50 used after handoff, got %d", got)
	}
}

func TestWriteBufferAccountant_AllowsOversizeWhenEmpty(t *testing.T) {
	// A single request larger than cap must succeed when nothing is in
	// flight, otherwise a single file handle with a large chunk size would
	// deadlock forever.
	a := NewWriteBufferAccountant(100)
	done := make(chan struct{})
	go func() {
		a.Reserve(500)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("oversize reserve on empty accountant should not block")
	}
}
