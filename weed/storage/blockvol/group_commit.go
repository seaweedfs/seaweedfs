package blockvol

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrGroupCommitShutdown = errors.New("blockvol: group committer shut down")

// GroupCommitter batches SyncCache requests and performs a single fsync
// for the entire batch. This amortizes the cost of fsync across many callers.
type GroupCommitter struct {
	syncFunc   func() error  // called to fsync (injectable for testing)
	maxDelay   time.Duration // max wait before flushing a partial batch
	maxBatch   int           // flush immediately when this many waiters accumulate
	onDegraded func()        // called when fsync fails

	mu       sync.Mutex
	pending  []chan error
	stopped  bool // set under mu by Run() before draining
	notifyCh chan struct{}
	stopCh   chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	syncCount atomic.Uint64 // number of fsyncs performed (for testing)
}

// GroupCommitterConfig configures the group committer.
type GroupCommitterConfig struct {
	SyncFunc   func() error  // required: the fsync function
	MaxDelay   time.Duration // default 1ms
	MaxBatch   int           // default 64
	OnDegraded func()        // optional: called on fsync error
}

// NewGroupCommitter creates a new group committer. Call Run() to start it.
func NewGroupCommitter(cfg GroupCommitterConfig) *GroupCommitter {
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = 1 * time.Millisecond
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 64
	}
	if cfg.OnDegraded == nil {
		cfg.OnDegraded = func() {}
	}
	return &GroupCommitter{
		syncFunc:   cfg.SyncFunc,
		maxDelay:   cfg.MaxDelay,
		maxBatch:   cfg.MaxBatch,
		onDegraded: cfg.OnDegraded,
		notifyCh:   make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// Run is the main loop. Call this in a goroutine.
func (gc *GroupCommitter) Run() {
	defer close(gc.done)
	for {
		// Wait for first waiter or shutdown.
		select {
		case <-gc.stopCh:
			gc.markStoppedAndDrain()
			return
		case <-gc.notifyCh:
		}

		// Collect batch: wait up to maxDelay for more waiters, or until maxBatch reached.
		deadline := time.NewTimer(gc.maxDelay)
		for {
			gc.mu.Lock()
			n := len(gc.pending)
			gc.mu.Unlock()
			if n >= gc.maxBatch {
				deadline.Stop()
				break
			}
			select {
			case <-gc.stopCh:
				deadline.Stop()
				gc.markStoppedAndDrain()
				return
			case <-deadline.C:
				goto flush
			case <-gc.notifyCh:
				continue
			}
		}

	flush:
		// Take all pending waiters.
		gc.mu.Lock()
		batch := gc.pending
		gc.pending = nil
		gc.mu.Unlock()

		if len(batch) == 0 {
			continue
		}

		// Perform fsync.
		err := gc.syncFunc()
		gc.syncCount.Add(1)
		if err != nil {
			gc.onDegraded()
		}

		// Wake all waiters.
		for _, ch := range batch {
			ch <- err
		}
	}
}

// Submit submits a sync request and blocks until the batch fsync completes.
// Returns nil on success, the fsync error, or ErrGroupCommitShutdown if stopped.
func (gc *GroupCommitter) Submit() error {
	ch := make(chan error, 1)
	gc.mu.Lock()
	if gc.stopped {
		gc.mu.Unlock()
		return ErrGroupCommitShutdown
	}
	gc.pending = append(gc.pending, ch)
	gc.mu.Unlock()

	// Non-blocking notify to wake Run().
	select {
	case gc.notifyCh <- struct{}{}:
	default:
	}

	return <-ch
}

// Stop shuts down the group committer. Pending waiters receive ErrGroupCommitShutdown.
// Safe to call multiple times.
func (gc *GroupCommitter) Stop() {
	gc.stopOnce.Do(func() {
		close(gc.stopCh)
	})
	<-gc.done
}

// SyncCount returns the number of fsyncs performed (for testing).
func (gc *GroupCommitter) SyncCount() uint64 {
	return gc.syncCount.Load()
}

// markStoppedAndDrain sets the stopped flag under mu and drains all pending
// waiters with ErrGroupCommitShutdown. This ensures no new Submit() can
// enqueue after we drain, closing the race window from QA-002.
func (gc *GroupCommitter) markStoppedAndDrain() {
	gc.mu.Lock()
	gc.stopped = true
	batch := gc.pending
	gc.pending = nil
	gc.mu.Unlock()
	for _, ch := range batch {
		ch <- ErrGroupCommitShutdown
	}
}
