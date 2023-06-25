package lock_manager

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"time"
)

type DistributedLockManager struct {
	lockManager *LockManager
	LockRing    *LockRing
}

func NewDistributedLockManager() *DistributedLockManager {
	return &DistributedLockManager{
		lockManager: NewLockManager(),
		LockRing:    NewLockRing(time.Second * 5),
	}
}

func (dlm *DistributedLockManager) Lock(host pb.ServerAddress, key string, expiredAtNs int64, token string) (renewToken string, movedTo pb.ServerAddress, err error) {
	servers := dlm.LockRing.GetSnapshot()
	if servers == nil {
		err = fmt.Errorf("no lock server found")
		return
	}

	server := hashKeyToServer(key, servers)
	if server != host {
		movedTo = server
		return
	}
	renewToken, err = dlm.lockManager.Lock(key, expiredAtNs, token)
	return
}

func (dlm *DistributedLockManager) Unlock(host pb.ServerAddress, key string, token string) (movedTo pb.ServerAddress, err error) {
	servers := dlm.LockRing.GetSnapshot()
	if servers == nil {
		err = fmt.Errorf("no lock server found")
		return
	}

	server := hashKeyToServer(key, servers)
	if server != host {
		movedTo = server
		return
	}
	_, err = dlm.lockManager.Unlock(key, token)
	return
}

// InsertLock is used to insert a lock to a server unconditionally
// It is used when a server is down and the lock is moved to another server
func (dlm *DistributedLockManager) InsertLock(key string, expiredAtNs int64, token string) {
	dlm.lockManager.InsertLock(key, expiredAtNs, token)
}
func (dlm *DistributedLockManager) SelectNotOwnedLocks(host pb.ServerAddress, servers []pb.ServerAddress) (locks []*Lock) {
	return dlm.lockManager.SelectLocks(func(key string) bool {
		server := hashKeyToServer(key, servers)
		return server != host
	})
}
func (dlm *DistributedLockManager) CalculateTargetServer(key string, servers []pb.ServerAddress) pb.ServerAddress {
	return hashKeyToServer(key, servers)
}
