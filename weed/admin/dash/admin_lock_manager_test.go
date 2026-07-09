package dash

import (
	"sync"
	"testing"
	"time"
)

type fakeAdminLocker struct {
	mu       sync.Mutex
	requests []time.Time
	releases []time.Time
}

func (f *fakeAdminLocker) RequestLock(clientName string) {
	f.mu.Lock()
	f.requests = append(f.requests, time.Now())
	f.mu.Unlock()
}

func (f *fakeAdminLocker) ReleaseLock() {
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

func newTestLockManager(locker adminLocker) *AdminLockManager {
	m := &AdminLockManager{locker: locker, clientName: "test"}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func TestAdminLockManagerRefCountsOverlappingHolds(t *testing.T) {
	locker := &fakeAdminLocker{}
	m := newTestLockManager(locker)

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
	savedYield := adminLockYieldWindow
	adminLockYieldWindow = 100 * time.Millisecond
	defer func() { adminLockYieldWindow = savedYield }()

	locker := &fakeAdminLocker{}
	m := newTestLockManager(locker)

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
	savedFairness := adminLockFairnessWindow
	savedYield := adminLockYieldWindow
	adminLockFairnessWindow = 50 * time.Millisecond
	adminLockYieldWindow = 20 * time.Millisecond
	defer func() {
		adminLockFairnessWindow = savedFairness
		adminLockYieldWindow = savedYield
	}()

	locker := &fakeAdminLocker{}
	m := newTestLockManager(locker)

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
