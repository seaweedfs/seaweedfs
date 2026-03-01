package blockvol

import (
	"testing"
	"time"
)

// TestBugGCPanicWaitersHung demonstrates that a panic in syncFunc
// leaves all Submit() waiters permanently blocked.
//
// BUG: Run() has no panic recovery. When syncFunc panics, the batch
// of waiters (each blocked on <-ch in Submit) are never notified.
// They hang forever, leaking goroutines.
//
// FIX: Add defer/recover in Run() that drains pending waiters with
// an error before exiting.
//
// This test FAILS until the bug is fixed.
func TestBugGCPanicWaitersHung(t *testing.T) {
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			panic("simulated disk panic")
		},
		MaxDelay: 10 * time.Millisecond,
	})

	// Wrap Run() so the panic doesn't kill the test process.
	go func() {
		defer func() { recover() }()
		gc.Run()
	}()

	// Submit should return an error (not hang forever).
	result := make(chan error, 1)
	go func() {
		result <- gc.Submit()
	}()

	select {
	case err := <-result:
		// GOOD: Submit returned (with an error, presumably).
		if err == nil {
			t.Error("Submit returned nil; expected a panic-related error")
		}
		t.Logf("Submit returned: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("BUG: Submit hung forever after syncFunc panic -- waiters not drained")
	}
}
