package dash

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

const (
	adminLockName       = cluster.AdminShellLockName
	adminLockClientName = "admin-plugin"
)

var (
	// A competing shell client polls the master once a second, while a local
	// re-acquire would win immediately. Leave the lock free for slightly more
	// than one poll interval so a waiting operator gets it first.
	adminLockYieldWindow = 2 * time.Second
	// After the lock has been held continuously for this long (overlapping
	// reference-counted holds can keep it held indefinitely), new Acquire
	// calls wait for a full release so shell clients are not starved.
	adminLockFairnessWindow = time.Minute
)

// adminLocker is the subset of exclusive_locks.ExclusiveLocker the manager uses.
type adminLocker interface {
	RequestLock(clientName string)
	ReleaseLock()
	SetMessage(message string)
}

// AdminLockManager coordinates exclusive admin locks with reference counting.
// It is safe for concurrent use.
type AdminLockManager struct {
	locker     adminLocker
	clientName string

	mu        sync.Mutex
	cond      *sync.Cond
	acquiring bool
	holdCount int
	heldSince time.Time

	lastAcquiredAt time.Time
	lastReleasedAt time.Time
	waitingSince   time.Time
	waitingReason  string
	currentReason  string
}

func NewAdminLockManager(masterClient *wdclient.MasterClient, clientName string) *AdminLockManager {
	if masterClient == nil {
		return nil
	}
	if clientName == "" {
		clientName = adminLockClientName
	}
	manager := &AdminLockManager{
		locker:     exclusive_locks.NewExclusiveLocker(masterClient, adminLockName),
		clientName: clientName,
	}
	manager.cond = sync.NewCond(&manager.mu)
	return manager
}

// Acquire takes the shared cluster lock, reference counted so overlapping
// admin-side holders piggyback on one lease. It must not be called while the
// caller's own call stack already holds the lock: once the fairness window
// makes new acquires wait for a full release, a nested Acquire deadlocks.
func (m *AdminLockManager) Acquire(reason string) (func(), error) {
	if m == nil || m.locker == nil {
		return func() {}, nil
	}

	m.mu.Lock()
	if reason != "" {
		m.locker.SetMessage(reason)
		m.currentReason = reason
	}
	for m.acquiring || (m.holdCount > 0 && time.Since(m.heldSince) > adminLockFairnessWindow) {
		m.cond.Wait()
	}
	if m.holdCount == 0 {
		m.acquiring = true
		m.waitingSince = time.Now().UTC()
		m.waitingReason = reason
		yield := time.Duration(0)
		if !m.lastReleasedAt.IsZero() {
			if since := time.Since(m.lastReleasedAt); since < adminLockYieldWindow {
				yield = adminLockYieldWindow - since
			}
		}
		m.mu.Unlock()
		if yield > 0 {
			time.Sleep(yield)
		}
		m.locker.RequestLock(m.clientName)
		m.mu.Lock()
		m.acquiring = false
		m.holdCount = 1
		m.heldSince = time.Now()
		m.lastAcquiredAt = time.Now().UTC()
		m.waitingSince = time.Time{}
		m.waitingReason = ""
		m.cond.Broadcast()
		m.mu.Unlock()
		return m.Release, nil
	}
	m.holdCount++
	if reason != "" {
		m.currentReason = reason
	}
	m.mu.Unlock()
	return m.Release, nil
}

func (m *AdminLockManager) Release() {
	if m == nil || m.locker == nil {
		return
	}

	m.mu.Lock()
	if m.holdCount <= 0 {
		m.mu.Unlock()
		return
	}
	m.holdCount--
	shouldRelease := m.holdCount == 0
	m.mu.Unlock()

	if shouldRelease {
		m.locker.ReleaseLock()
		m.mu.Lock()
		m.lastReleasedAt = time.Now().UTC()
		m.currentReason = ""
		m.cond.Broadcast()
		m.mu.Unlock()
	}
}

type LockStatus struct {
	Held           bool       `json:"held"`
	HoldCount      int        `json:"hold_count"`
	Acquiring      bool       `json:"acquiring"`
	Message        string     `json:"message,omitempty"`
	WaitingReason  string     `json:"waiting_reason,omitempty"`
	LastAcquiredAt *time.Time `json:"last_acquired_at,omitempty"`
	LastReleasedAt *time.Time `json:"last_released_at,omitempty"`
	WaitingSince   *time.Time `json:"waiting_since,omitempty"`
}

func (m *AdminLockManager) Status() LockStatus {
	if m == nil {
		return LockStatus{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	status := LockStatus{
		Held:          m.holdCount > 0,
		HoldCount:     m.holdCount,
		Acquiring:     m.acquiring,
		Message:       m.currentReason,
		WaitingReason: m.waitingReason,
	}
	if !m.lastAcquiredAt.IsZero() {
		at := m.lastAcquiredAt
		status.LastAcquiredAt = &at
	}
	if !m.lastReleasedAt.IsZero() {
		at := m.lastReleasedAt
		status.LastReleasedAt = &at
	}
	if !m.waitingSince.IsZero() {
		at := m.waitingSince
		status.WaitingSince = &at
	}
	return status
}
