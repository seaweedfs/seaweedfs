package util

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAsyncBatchWorkerProcessesInOrder(t *testing.T) {
	var mu sync.Mutex
	var got []int
	w := NewAsyncBatchWorker(func(batch []int) {
		mu.Lock()
		got = append(got, batch...)
		mu.Unlock()
	})
	for i := 0; i < 100; i++ {
		w.Enqueue(i)
	}
	w.Drain()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 100 {
		t.Fatalf("processed %d items, want 100", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("item %d = %d, want %d (FIFO order broken)", i, v, i)
		}
	}
}

func TestAsyncBatchWorkerDrainWaitsForEnqueued(t *testing.T) {
	var processed int64
	w := NewAsyncBatchWorker(func(batch []int) {
		time.Sleep(time.Millisecond)
		atomic.AddInt64(&processed, int64(len(batch)))
	})
	w.Enqueue(1, 2, 3)
	w.Enqueue(4, 5)
	w.Drain()
	if n := atomic.LoadInt64(&processed); n != 5 {
		t.Fatalf("processed %d, want 5 after Drain", n)
	}
}

// Enqueue must not block even while the worker is busy processing under a lock
// the producer also holds — the unbounded queue is what guarantees this.
func TestAsyncBatchWorkerEnqueueNeverBlocks(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	w := NewAsyncBatchWorker(func(batch []int) {
		once.Do(func() { close(entered) })
		<-release
	})
	w.Enqueue(0) // worker picks this up and blocks in process
	<-entered

	done := make(chan struct{})
	go func() {
		for i := 1; i < 1000; i++ {
			w.Enqueue(i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked while worker was busy")
	}
	close(release)
	w.Shutdown()
}

// Drain must honor its contract even when Shutdown runs concurrently: it may
// not return until the in-flight batch has actually been processed.
func TestAsyncBatchWorkerDrainCompletesDuringShutdown(t *testing.T) {
	var processed int64
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	w := NewAsyncBatchWorker(func(batch []int) {
		once.Do(func() { close(entered) })
		<-release
		atomic.AddInt64(&processed, int64(len(batch)))
	})
	w.Enqueue(1, 2, 3)
	<-entered // worker has picked up the batch and is blocked in process

	drained := make(chan struct{})
	go func() { w.Drain(); close(drained) }()
	go w.Shutdown() // sets closed and broadcasts while the batch is still in flight

	select {
	case <-drained:
		t.Fatal("Drain returned before the in-flight batch finished")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-drained
	if n := atomic.LoadInt64(&processed); n != 3 {
		t.Fatalf("processed %d after Drain, want 3", n)
	}
}

func TestAsyncBatchWorkerShutdownDrainsThenStops(t *testing.T) {
	var processed int64
	w := NewAsyncBatchWorker(func(batch []int) {
		atomic.AddInt64(&processed, int64(len(batch)))
	})
	w.Enqueue(1, 2, 3, 4)
	w.Shutdown()
	if n := atomic.LoadInt64(&processed); n != 4 {
		t.Fatalf("processed %d, want 4 (Shutdown should drain)", n)
	}
	// Enqueue after Shutdown is dropped, not processed.
	w.Enqueue(5, 6)
	if n := atomic.LoadInt64(&processed); n != 4 {
		t.Fatalf("processed %d after post-shutdown Enqueue, want 4", n)
	}
}
