package dash

import (
	"sync"
	"testing"
	"time"
)

type fakeAdminLocker struct {
	releaseDelay time.Duration

	mu       sync.Mutex
	requests []time.Time
	releases []time.Time // recorded when ReleaseLock returns
}

func (f *fakeAdminLocker) RequestLock(clientName string) {
	f.mu.Lock()
	f.requests = append(f.requests, time.Now())
	f.mu.Unlock()
}

func (f *fakeAdminLocker) ReleaseLock() {
	if f.releaseDelay > 0 {
		time.Sleep(f.releaseDelay)
	}
	f.mu.Lock()
	f.releases = append(f.releases, time.Now())
	f.mu.Unlock()
}

func (f *fakeAdminLocker) SetMessage(message string) {}

func (f *fakeAdminLocker) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests), len(f.releases)
}

func newTestLockManager(locker adminLocker, yieldWindow, fairnessWindow time.Duration) *AdminLockManager {
	m := &AdminLockManager{
		locker:         locker,
		clientName:     "test",
		yieldWindow:    yieldWindow,
		fairnessWindow: fairnessWindow,
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func TestAdminLockManagerRefCountsOverlappingHolds(t *testing.T) {
	t.Parallel()

	locker := &fakeAdminLocker{}
	m := newTestLockManager(locker, defaultAdminLockYieldWindow, defaultAdminLockFairnessWindow)

	release1, _ := m.Acquire("a")
	release2, _ := m.Acquire("b")

	if requests, releases := locker.counts(); requests != 1 || releases != 0 {
		t.Fatalf("expected one lease request while overlapping, got requests=%d releases=%d", requests, releases)
	}

	release1()
	if _, releases := locker.counts(); releases != 0 {
		t.Fatalf("lock released while still held by another caller")
	}
	release2()
	if _, releases := locker.counts(); releases != 1 {
		t.Fatalf("expected one release after last holder, got %d", releases)
	}
}

func TestAdminLockManagerYieldsBeforeReacquire(t *testing.T) {
	t.Parallel()

	locker := &fakeAdminLocker{}
	m := newTestLockManager(locker, 100*time.Millisecond, defaultAdminLockFairnessWindow)

	release, _ := m.Acquire("first")
	release()
	release, _ = m.Acquire("second")
	release()

	locker.mu.Lock()
	gap := locker.requests[1].Sub(locker.releases[0])
	locker.mu.Unlock()
	if gap < 90*time.Millisecond {
		t.Fatalf("re-acquire did not yield to competing clients: gap=%v", gap)
	}
}

func TestAdminLockManagerFairnessWindowForcesFullRelease(t *testing.T) {
	t.Parallel()

	locker := &fakeAdminLocker{}
	m := newTestLockManager(locker, 20*time.Millisecond, 50*time.Millisecond)

	release1, _ := m.Acquire("long-hold")
	m.mu.Lock()
	m.heldSince = time.Now().Add(-time.Second)
	m.mu.Unlock()

	acquired := make(chan func(), 1)
	go func() {
		release2, _ := m.Acquire("late-joiner")
		acquired <- release2
	}()

	select {
	case <-acquired:
		t.Fatalf("late acquire joined a hold older than the fairness window")
	case <-time.After(50 * time.Millisecond):
	}

	release1()

	select {
	case release2 := <-acquired:
		release2()
	case <-time.After(time.Second):
		t.Fatalf("late acquire did not proceed after full release")
	}

	if requests, releases := locker.counts(); requests != 2 || releases != 2 {
		t.Fatalf("expected a full release/re-acquire cycle, got requests=%d releases=%d", requests, releases)
	}
}

func TestAdminLockManagerAcquireWaitsForReleaseInFlight(t *testing.T) {
	t.Parallel()

	locker := &fakeAdminLocker{releaseDelay: 50 * time.Millisecond}
	m := newTestLockManager(locker, time.Millisecond, defaultAdminLockFairnessWindow)

	release1, _ := m.Acquire("first")
	releaseDone := make(chan struct{})
	go func() {
		release1()
		close(releaseDone)
	}()
	// Let the release enter the slow ReleaseLock before re-acquiring.
	time.Sleep(10 * time.Millisecond)
	release2, _ := m.Acquire("second")
	<-releaseDone
	release2()

	locker.mu.Lock()
	defer locker.mu.Unlock()
	if len(locker.requests) != 2 || len(locker.releases) != 2 {
		t.Fatalf("unexpected lease traffic: requests=%d releases=%d", len(locker.requests), len(locker.releases))
	}
	if locker.requests[1].Before(locker.releases[0]) {
		t.Fatalf("RequestLock ran while ReleaseLock was still in flight")
	}
}
