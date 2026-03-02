package dash

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

const (
	adminLockName       = "shell"
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
	}
	for m.acquiring {
		m.cond.Wait()
	}
	if m.holdCount == 0 {
		m.acquiring = true
		m.mu.Unlock()
		m.locker.RequestLock(m.clientName)
		m.mu.Lock()
		m.acquiring = false
		m.holdCount = 1
		m.cond.Broadcast()
		m.mu.Unlock()
		return m.Release, nil
	}
	m.holdCount++
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
	}
}
