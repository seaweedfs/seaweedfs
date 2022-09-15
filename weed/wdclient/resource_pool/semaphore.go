package resource_pool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Semaphore interface {
	// Increment the semaphore counter by one.
	Release()

	// Decrement the semaphore counter by one, and block if counter < 0
	Acquire()

	// Decrement the semaphore counter by one, and block if counter < 0
	// Wait for up to the given duration.  Returns true if did not timeout
	TryAcquire(timeout time.Duration) bool
}

// A simple counting Semaphore.
type boundedSemaphore struct {
	slots chan struct{}
}

// Create a bounded semaphore. The count parameter must be a positive number.
// NOTE: The bounded semaphore will panic if the user tries to Release
// beyond the specified count.
func NewBoundedSemaphore(count uint) Semaphore {
	sem := &boundedSemaphore{
		slots: make(chan struct{}, int(count)),
	}
	for i := 0; i < cap(sem.slots); i++ {
		sem.slots <- struct{}{}
	}
	return sem
}

// Acquire returns on successful acquisition.
func (sem *boundedSemaphore) Acquire() {
	<-sem.slots
}

// TryAcquire returns true if it acquires a resource slot within the
// timeout, false otherwise.
func (sem *boundedSemaphore) TryAcquire(timeout time.Duration) bool {
	if timeout > 0 {
		// Wait until we get a slot or timeout expires.
		tm := time.NewTimer(timeout)
		defer tm.Stop()
		select {
		case <-sem.slots:
			return true
		case <-tm.C:
			// Timeout expired. In very rare cases this might happen even if
			// there is a slot available, e.g. GC pause after we create the timer
			// and select randomly picked this one out of the two available channels.
			// We should do one final immediate check below.
		}
	}

	// Return true if we have a slot available immediately and false otherwise.
	select {
	case <-sem.slots:
		return true
	default:
		return false
	}
}

// Release the acquired semaphore. You must not release more than you
// have acquired.
func (sem *boundedSemaphore) Release() {
	select {
	case sem.slots <- struct{}{}:
	default:
		// slots is buffered. If a send blocks, it indicates a programming
		// error.
		panic(fmt.Errorf("too many releases for boundedSemaphore"))
	}
}

// This returns an unbound counting semaphore with the specified initial count.
// The semaphore counter can be arbitrary large (i.e., Release can be called
// unlimited amount of times).
//
// NOTE: In general, users should use bounded semaphore since it is more
// efficient than unbounded semaphore.
func NewUnboundedSemaphore(initialCount int) Semaphore {
	res := &unboundedSemaphore{
		counter: int64(initialCount),
	}
	res.cond.L = &res.lock
	return res
}

type unboundedSemaphore struct {
	lock    sync.Mutex
	cond    sync.Cond
	counter int64
}

func (s *unboundedSemaphore) Release() {
	s.lock.Lock()
	s.counter += 1
	if s.counter > 0 {
		// Not broadcasting here since it's unlike we can satisfy all waiting
		// goroutines.  Instead, we will Signal again if there are left over
		// quota after Acquire, in case of lost wakeups.
		s.cond.Signal()
	}
	s.lock.Unlock()
}

func (s *unboundedSemaphore) Acquire() {
	s.lock.Lock()
	for s.counter < 1 {
		s.cond.Wait()
	}
	s.counter -= 1
	if s.counter > 0 {
		s.cond.Signal()
	}
	s.lock.Unlock()
}

func (s *unboundedSemaphore) TryAcquire(timeout time.Duration) bool {
	done := make(chan bool, 1)
	// Gate used to communicate between the threads and decide what the result
	// is.  If the main thread decides, we have timed out, otherwise we succeed.
	decided := new(int32)
	atomic.StoreInt32(decided, 0)
	go func() {
		s.Acquire()
		if atomic.SwapInt32(decided, 1) == 0 {
			// Acquire won the race
			done <- true
		} else {
			// If we already decided the result, and this thread did not win
			s.Release()
		}
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		if atomic.SwapInt32(decided, 1) == 1 {
			// The other thread already decided the result
			return true
		}
		return false
	}
}
