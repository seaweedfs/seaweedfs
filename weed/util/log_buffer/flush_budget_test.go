package log_buffer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/mem"
)

func TestFlushBudgetBoundsQueuedBytes(t *testing.T) {
	b := newFlushBudget(100)

	if got := b.reserve(0, 60); got != 60 {
		t.Fatalf("first reserve returned %d, want 60", got)
	}

	// 60 + 60 is over the limit, so the second producer has to wait.
	reserved := make(chan int)
	go func() { reserved <- b.reserve(1, 60) }()

	select {
	case <-reserved:
		t.Fatal("second reserve went through while the budget was full")
	case <-time.After(50 * time.Millisecond):
	}

	b.release(60)
	select {
	case got := <-reserved:
		if got != 60 {
			t.Errorf("second reserve returned %d, want 60", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second reserve never woke up after the release")
	}
}

// A window bigger than the whole budget still has to get through, or an
// oversized entry would wedge the flush loop instead of merely spiking it.
func TestFlushBudgetAdmitsOversizedWindow(t *testing.T) {
	b := newFlushBudget(100)

	if got := b.reserve(0, 50); got != 50 {
		t.Fatalf("reserve returned %d, want 50", got)
	}

	reserved := make(chan int)
	go func() { reserved <- b.reserve(1, 500) }()

	select {
	case <-reserved:
		t.Fatal("oversized reserve went through before the queue drained")
	case <-time.After(50 * time.Millisecond):
	}

	b.release(50)
	select {
	case got := <-reserved:
		if got != 100 {
			t.Errorf("oversized reserve booked %d, want the whole %d budget", got, 100)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversized reserve never got through on an empty queue")
	}
}

// Windows have to reach the flush loop in the order they were sealed. A
// producer can park here for seconds, so letting a later window overtake an
// earlier one would persist them out of order and walk the flushed watermarks
// backwards. Sized so only one window fits at a time, which is what makes the
// admission order observable rather than a race between the woken goroutines.
func TestFlushBudgetAdmitsInSealOrder(t *testing.T) {
	const windows = 5
	b := newFlushBudget(100)
	b.reserve(0, 100) // fills the budget, so every window below has to wait

	// Park them in reverse seal order, so arrival order cannot be what produces
	// the right answer. Each window needs the whole budget, so exactly one is
	// admitted per release and the order is observable.
	admitted := make(chan uint64, windows)
	for seq := uint64(windows); seq >= 1; seq-- {
		go func(seq uint64) {
			b.reserve(seq, 100)
			admitted <- seq
		}(seq)
		time.Sleep(20 * time.Millisecond) // let it reach the wait
	}

	for want := uint64(1); want <= windows; want++ {
		select {
		case seq := <-admitted:
			t.Fatalf("window %d was admitted while the budget was still held", seq)
		case <-time.After(20 * time.Millisecond):
		}

		b.release(100)
		select {
		case got := <-admitted:
			if got != want {
				t.Fatalf("admitted window %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("window %d never admitted", want)
		}
	}
}

// Shutdown must not be held up by a producer parked on the budget.
func TestFlushBudgetCloseReleasesWaiters(t *testing.T) {
	b := newFlushBudget(100)
	b.reserve(0, 100)

	done := make(chan struct{})
	go func() {
		b.reserve(1, 100)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("reserve went through while the budget was full")
	case <-time.After(50 * time.Millisecond):
	}

	b.close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("close did not release the parked producer")
	}
}

// What the queue actually holds is the pooled slab, which mem.Allocate rounds
// up to a size class. Charging the window length instead would let the queue
// retain roughly twice the ceiling.
func TestQueueFlushChargesTheSlabNotTheWindow(t *testing.T) {
	stall := make(chan struct{})
	lb := NewLogBuffer("slab", time.Hour, func(_ *LogBuffer, _, _ time.Time, _ []byte, _, _ int64) {
		<-stall
	}, nil, func() {})
	defer func() { close(stall); lb.ShutdownLogBuffer() }()

	// 5 MiB rounds up to an 8 MiB slot.
	const windowSize = 5 << 20
	data := mem.Allocate(windowSize)
	if cap(data) == len(data) {
		t.Skipf("allocator returned an exact fit (%d bytes), nothing to distinguish", cap(data))
	}

	if !lb.queueFlush(&dataToFlush{data: data}) {
		t.Fatal("queueFlush dropped the window")
	}

	lb.flushBudget.mu.Lock()
	queued := lb.flushBudget.queued
	lb.flushBudget.mu.Unlock()

	if queued != cap(data) {
		t.Errorf("charged %d bytes for a window holding a %d byte slab (len %d)", queued, cap(data), len(data))
	}
}

// The window is already sealed by the time queueFlush runs, so dropping it
// loses records the caller was told were accepted. A shutdown racing the
// hand-off must not cost data while the queue still has room for it.
func TestQueueFlushKeepsSealedWindowWhenQueueHasRoom(t *testing.T) {
	stall := make(chan struct{})
	var flushed atomic.Int64
	lb := NewLogBuffer("shutdown-race", time.Hour, func(_ *LogBuffer, _, _ time.Time, _ []byte, _, _ int64) {
		<-stall
		flushed.Add(1)
	}, nil, func() {})
	defer func() { close(stall) }()

	// Simulate a shutdown landing between the seal and the hand-off.
	close(lb.shutdownCh)
	lb.isStopping.Store(true)

	// Stay inside the channel's capacity so room is guaranteed for every one of
	// them; loopFlush is parked in the stalled flushFn and drains nothing.
	for i := 0; i < flushQueueDepth; i++ {
		if !lb.queueFlush(&dataToFlush{data: mem.Allocate(1024), seq: uint64(i)}) {
			t.Fatalf("window %d was dropped even though the queue had room", i)
		}
	}
}

// The point of the budget: a stalled flush must stop producers rather than let
// them keep handing over copies. Asserted on how many producers get through,
// which is independent of the counter reserve maintains.
func TestStalledFlushBlocksOversizedProducers(t *testing.T) {
	stall := make(chan struct{})
	var completed atomic.Int64

	lb := NewLogBuffer("stalled", time.Hour, func(_ *LogBuffer, _, _ time.Time, _ []byte, _, _ int64) {
		<-stall
	}, nil, func() {})

	// Just over BufferSize is enough to get a window per entry; the package
	// leaves plenty of other LogBuffers alive, so keep the footprint small
	// enough for a 32-bit runner.
	entry := make([]byte, BufferSize+1)
	const producers = flushQueueDepth

	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lb.AddDataToBuffer(nil, entry, 0); err == nil {
				completed.Add(1)
			}
		}()
	}

	time.Sleep(2 * time.Second)

	got := completed.Load()
	// Some have to get through -- a budget that admits nobody is a deadlock,
	// not a bound -- and the rest have to be parked.
	if got == 0 {
		t.Error("no producer got through a stalled flush; the budget deadlocked")
	}
	if got >= producers {
		t.Errorf("all %d producers got through a stalled flush; the budget did not bind", producers)
	}

	close(stall)
	lb.ShutdownLogBuffer()
	wg.Wait()
}

// ...and once the flush drains, everyone gets through: the bound must not
// deadlock the producers it parks.
func TestBlockedProducersDrainOnceFlushResumes(t *testing.T) {
	stall := make(chan struct{})
	var completed atomic.Int64

	lb := NewLogBuffer("drain", time.Hour, func(_ *LogBuffer, _, _ time.Time, _ []byte, _, _ int64) {
		<-stall
	}, nil, func() {})

	entry := make([]byte, BufferSize+1)
	const producers = 6

	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := lb.AddDataToBuffer(nil, entry, 0); err == nil {
				completed.Add(1)
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(stall)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("producers never drained after the flush resumed")
	}
	if got := completed.Load(); got != producers {
		t.Errorf("%d of %d producers completed", got, producers)
	}
	lb.ShutdownLogBuffer()
}
