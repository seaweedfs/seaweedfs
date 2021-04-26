package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"math/rand"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

/*
How exclusive lock works?
-----------

Shell
------
When shell lock,
  * lease an admin token (lockTime, token)
  * start a goroutine to renew the admin token periodically

When shell unlock
  * stop the renewal goroutine
  * sends a release lock request

Master
------
Master maintains:
  * randomNumber
  * lastLockTime
When master receives the lease/renew request from shell
  If lastLockTime still fresh {
    if is a renew and token is valid {
      // for renew
      generate the randomNumber => token
      return
    }
    refuse
    return
  } else {
    // for fresh lease request
    generate the randomNumber => token
    return
  }

When master receives the release lock request from shell
  set the lastLockTime to zero


The volume server does not need to verify.
This makes the lock/unlock optional, similar to what golang code usually does.

*/

const (
	LockDuration = 10 * time.Second
)

type AdminLock struct {
	accessSecret   int64
	accessLockTime time.Time
	lastClient     string
}

type AdminLocks struct {
	locks map[string]*AdminLock
	sync.RWMutex
}

func NewAdminLocks() *AdminLocks {
	return &AdminLocks{
		locks: make(map[string]*AdminLock),
	}
}

func (locks *AdminLocks) isLocked(lockName string) (clientName string, isLocked bool) {
	locks.RLock()
	defer locks.RUnlock()
	adminLock, found := locks.locks[lockName]
	if !found {
		return "", false
	}
	glog.V(4).Infof("isLocked %v", adminLock.lastClient)
	return adminLock.lastClient, adminLock.accessLockTime.Add(LockDuration).After(time.Now())
}

func (locks *AdminLocks) isValidToken(lockName string, ts time.Time, token int64) bool {
	locks.RLock()
	defer locks.RUnlock()
	adminLock, found := locks.locks[lockName]
	if !found {
		return false
	}
	return adminLock.accessLockTime.Equal(ts) && adminLock.accessSecret == token
}

func (locks *AdminLocks) generateToken(lockName string, clientName string) (ts time.Time, token int64) {
	locks.Lock()
	defer locks.Unlock()
	lock := &AdminLock{
		accessSecret:   rand.Int63(),
		accessLockTime: time.Now(),
		lastClient:     clientName,
	}
	locks.locks[lockName] = lock
	return lock.accessLockTime, lock.accessSecret
}

func (locks *AdminLocks) deleteLock(lockName string) {
	locks.Lock()
	defer locks.Unlock()
	delete(locks.locks, lockName)
}

func (ms *MasterServer) LeaseAdminToken(ctx context.Context, req *master_pb.LeaseAdminTokenRequest) (*master_pb.LeaseAdminTokenResponse, error) {
	resp := &master_pb.LeaseAdminTokenResponse{}

	if lastClient, isLocked := ms.adminLocks.isLocked(req.LockName); isLocked {
		glog.V(4).Infof("LeaseAdminToken %v", lastClient)
		if req.PreviousToken != 0 && ms.adminLocks.isValidToken(req.LockName, time.Unix(0, req.PreviousLockTime), req.PreviousToken) {
			// for renew
			ts, token := ms.adminLocks.generateToken(req.LockName, req.ClientName)
			resp.Token, resp.LockTsNs = token, ts.UnixNano()
			return resp, nil
		}
		// refuse since still locked
		return resp, fmt.Errorf("already locked by " + lastClient)
	}
	// for fresh lease request
	ts, token := ms.adminLocks.generateToken(req.LockName, req.ClientName)
	resp.Token, resp.LockTsNs = token, ts.UnixNano()
	return resp, nil
}

func (ms *MasterServer) ReleaseAdminToken(ctx context.Context, req *master_pb.ReleaseAdminTokenRequest) (*master_pb.ReleaseAdminTokenResponse, error) {
	resp := &master_pb.ReleaseAdminTokenResponse{}
	if ms.adminLocks.isValidToken(req.LockName, time.Unix(0, req.PreviousLockTime), req.PreviousToken) {
		ms.adminLocks.deleteLock(req.LockName)
	}
	return resp, nil
}
