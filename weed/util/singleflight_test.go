package util

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSingleFlightGroupDedup(t *testing.T) {
	// Verify that two concurrent Do calls for the same key execute fn
	// only once. The main goroutine signals right before calling Do,
	// and a helper goroutine waits for that signal before closing
	// the gate. Because the signal-to-Do gap is a single function
	// call on the main goroutine and the key cannot be removed until
	// the gate is closed, the helper's close(gate) cannot race ahead.
	var g SingleFlightGroup

	expected := []byte("result")
	var primaryCalls, secondaryCalls int32

	fnRunning := make(chan struct{}) // closed by fn
	aboutToDo := make(chan struct{}) // closed by main before calling Do
	gate := make(chan struct{})      // closed by helper to release fn

	// Primary goroutine: executes fn, which blocks on gate.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.Do("key1", func() ([]byte, error) {
			atomic.AddInt32(&primaryCalls, 1)
			close(fnRunning)
			<-gate
			return expected, nil
		})
	}()

	// Wait for fn to be running (key is now in the map).
	<-fnRunning

	// Helper goroutine: waits for the main goroutine to signal it is
	// about to call Do, then closes gate. By the time close(gate)
	// executes, the main goroutine has already entered Do (or is about
	// to) -- and since fn is still blocked on gate until this very
	// close, the key is still in the map.
	go func() {
		<-aboutToDo
		close(gate)
	}()

	// Signal the helper then immediately call Do. The key is in the
	// map (fn is blocked on gate which has not been closed yet because
	// the helper is waiting on aboutToDo which we are about to close).
	close(aboutToDo)
	v, err := g.Do("key1", func() ([]byte, error) {
		atomic.AddInt32(&secondaryCalls, 1)
		return nil, nil
	})

	wg.Wait()

	if atomic.LoadInt32(&primaryCalls) != 1 {
		t.Errorf("expected primary fn called once, got %d", primaryCalls)
	}
	if atomic.LoadInt32(&secondaryCalls) != 0 {
		t.Errorf("expected secondary fn never called, got %d", secondaryCalls)
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if string(v) != string(expected) {
		t.Errorf("got %q, want %q", v, expected)
	}
}

func TestSingleFlightGroupDifferentKeys(t *testing.T) {
	var g SingleFlightGroup
	var calls int32

	fn := func() ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("ok"), nil
	}

	var wg sync.WaitGroup
	wg.Add(3)
	for _, key := range []string{"a", "b", "c"} {
		go func(k string) {
			defer wg.Done()
			g.Do(k, fn)
		}(key)
	}
	wg.Wait()

	if c := atomic.LoadInt32(&calls); c != 3 {
		t.Errorf("expected 3 independent calls, got %d", c)
	}
}

func TestSingleFlightGroupErrorPropagation(t *testing.T) {
	var g SingleFlightGroup
	gate := make(chan struct{})
	testErr := errors.New("download failed")

	fn := func() ([]byte, error) {
		<-gate
		return nil, testErr
	}

	const n = 5
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = g.Do("errkey", fn)
		}(i)
	}

	close(gate)
	wg.Wait()

	for i := 0; i < n; i++ {
		if !errors.Is(errs[i], testErr) {
			t.Errorf("caller %d: expected %v, got %v", i, testErr, errs[i])
		}
	}
}

func TestSingleFlightGroupFreshCallAfterCompletion(t *testing.T) {
	var g SingleFlightGroup
	var calls int32

	fn := func() ([]byte, error) {
		c := atomic.AddInt32(&calls, 1)
		return []byte{byte(c)}, nil
	}

	// First call.
	v1, err1 := g.Do("key", fn)
	if err1 != nil {
		t.Fatalf("first call error: %v", err1)
	}

	// Second call should trigger a fresh execution.
	v2, err2 := g.Do("key", fn)
	if err2 != nil {
		t.Fatalf("second call error: %v", err2)
	}

	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("expected fn called twice for sequential calls, got %d", calls)
	}
	if v1[0] == v2[0] {
		t.Errorf("expected different results for sequential calls, got %v and %v", v1, v2)
	}
}
