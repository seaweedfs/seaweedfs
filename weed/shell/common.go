package shell

import (
	"errors"
	"sync"
)

var (
	// Default maximum parallelization/concurrency for commands supporting it.
	DefaultMaxParallelization = 10
)

// ErrorWaitGroup implements a goroutine wait group which aggregates errors, if any.
type ErrorWaitGroup struct {
	maxConcurrency int
	wg             *sync.WaitGroup
	wgSem          chan bool
	errors         []error
	errorsMu       sync.Mutex
}

type ErrorWaitGroupTask func() error

func NewErrorWaitGroup(maxConcurrency int) *ErrorWaitGroup {
	if maxConcurrency <= 0 {
		// no concurrency = one task at the time
		maxConcurrency = 1
	}
	return &ErrorWaitGroup{
		maxConcurrency: maxConcurrency,
		wg:             &sync.WaitGroup{},
		wgSem:          make(chan bool, maxConcurrency),
	}
}

// Reset restarts an ErrorWaitGroup, keeping original settings. Errors and pending goroutines, if any, are flushed.
func (ewg *ErrorWaitGroup) Reset() {
	close(ewg.wgSem)

	ewg.wg = &sync.WaitGroup{}
	ewg.wgSem = make(chan bool, ewg.maxConcurrency)
	ewg.errors = nil
}

// Add queues an ErrorWaitGroupTask to be executed as a goroutine.
func (ewg *ErrorWaitGroup) Add(f ErrorWaitGroupTask) {
	if ewg.maxConcurrency <= 1 {
		// keep run order deterministic when parallelization is off
		ewg.errors = append(ewg.errors, f())
		return
	}

	ewg.wg.Add(1)
	go func() {
		ewg.wgSem <- true

		err := f()
		ewg.errorsMu.Lock()
		ewg.errors = append(ewg.errors, err)
		ewg.errorsMu.Unlock()

		<-ewg.wgSem
		ewg.wg.Done()
	}()
}

// Wait sleeps until all ErrorWaitGroupTasks are completed, then returns errors for them.
func (ewg *ErrorWaitGroup) Wait() error {
	ewg.wg.Wait()
	return errors.Join(ewg.errors...)
}
