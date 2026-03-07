package dash

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

const adminPresenceClientName = "admin-server"

type adminPresenceLock struct {
	locker *exclusive_locks.ExclusiveLocker
	stopCh chan struct{}
}

func newAdminPresenceLock(masterClient *wdclient.MasterClient) *adminPresenceLock {
	if masterClient == nil {
		return nil
	}
	return &adminPresenceLock{
		locker: exclusive_locks.NewExclusiveLocker(masterClient, cluster.AdminServerPresenceLockName),
		stopCh: make(chan struct{}),
	}
}

func (l *adminPresenceLock) Start() {
	if l == nil || l.locker == nil {
		return
	}
	l.locker.SetMessage("admin server connected")
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			if !l.locker.IsLocked() {
				l.locker.RequestLock(adminPresenceClientName)
			}
			select {
			case <-l.stopCh:
				return
			case <-ticker.C:
			}
		}
	}()
}

func (l *adminPresenceLock) Stop() {
	if l == nil {
		return
	}
	select {
	case <-l.stopCh:
	default:
		close(l.stopCh)
	}
	if l.locker != nil {
		l.locker.ReleaseLock()
	}
}
