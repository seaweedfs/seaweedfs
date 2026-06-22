package util

import "sync"

// AsyncBatchWorker is an unbounded FIFO drained by one background goroutine,
// handing it everything queued so far per process call. Unbounded on purpose:
// a bound would let a stalled consumer block producers, which deadlocks when a
// producer is itself a goroutine the consumer can end up waiting on.
type AsyncBatchWorker[T any] struct {
	mu        sync.Mutex
	cond      *sync.Cond
	queue     []T
	enqueued  int64
	processed int64
	closed    bool
	done      chan struct{}
	process   func([]T)
}

// NewAsyncBatchWorker starts the worker. process runs outside the internal
// lock, so it may take locks held by goroutines that are themselves enqueueing.
func NewAsyncBatchWorker[T any](process func([]T)) *AsyncBatchWorker[T] {
	w := &AsyncBatchWorker[T]{
		process: process,
		done:    make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	go w.run()
	return w
}

// Enqueue appends items for the worker without blocking. Items enqueued after
// Shutdown are dropped.
func (w *AsyncBatchWorker[T]) Enqueue(items ...T) {
	if len(items) == 0 {
		return
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.queue = append(w.queue, items...)
	w.enqueued += int64(len(items))
	w.mu.Unlock()
	w.cond.Signal()
}

func (w *AsyncBatchWorker[T]) run() {
	defer close(w.done)
	for {
		w.mu.Lock()
		for len(w.queue) == 0 && !w.closed {
			w.cond.Wait()
		}
		if len(w.queue) == 0 { // closed and drained
			w.mu.Unlock()
			return
		}
		batch := w.queue
		w.queue = nil
		w.mu.Unlock()

		w.process(batch)

		// Once per batch, not per item: Drain only needs the count to reach its target.
		w.mu.Lock()
		w.processed += int64(len(batch))
		w.mu.Unlock()
		w.cond.Broadcast()
	}
}

// Drain blocks until every item enqueued so far has been processed, including
// across a concurrent Shutdown — Shutdown drains all accepted items, so
// processed always reaches the target and this cannot hang.
func (w *AsyncBatchWorker[T]) Drain() {
	w.mu.Lock()
	defer w.mu.Unlock()
	target := w.enqueued
	for w.processed < target {
		w.cond.Wait()
	}
}

// Shutdown processes any outstanding items, then stops the worker and waits for
// it to exit. Subsequent Enqueue calls are dropped.
func (w *AsyncBatchWorker[T]) Shutdown() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cond.Broadcast()
	<-w.done
}
