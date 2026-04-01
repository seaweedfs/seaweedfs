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

// AdminLockManager coordinates exclusive admin locks with reference counting.
// It is safe for concurrent use.
type AdminLockManager struct {
	locker     *exclusive_locks.ExclusiveLocker
	clientName string

	mu        sync.Mutex
	cond      *sync.Cond
	acquiring bool
	holdCount int

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

func (m *AdminLockManager) Acquire(reason string) (func(), error) {
	if m == nil || m.locker == nil {
		return func() {}, nil
	}

	m.mu.Lock()
	if reason != "" {
		m.locker.SetMessage(reason)
		m.currentReason = reason
	}
	for m.acquiring {
		m.cond.Wait()
	}
	if m.holdCount == 0 {
		m.acquiring = true
		m.waitingSince = time.Now().UTC()
		m.waitingReason = reason
		m.mu.Unlock()
		m.locker.RequestLock(m.clientName)
		m.mu.Lock()
		m.acquiring = false
		m.holdCount = 1
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
		m.mu.Lock()
		m.lastReleasedAt = time.Now().UTC()
		m.currentReason = ""
		m.mu.Unlock()
		m.locker.ReleaseLock()
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
