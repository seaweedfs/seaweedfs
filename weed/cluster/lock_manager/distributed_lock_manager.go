package lock_manager

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"time"
)

const MaxDuration = 1<<63 - 1

var NoLockServerError = fmt.Errorf("no lock server found")

type DistributedLockManager struct {
	lockManager *LockManager
	LockRing    *LockRing
	Host        pb.ServerAddress
}

func NewDistributedLockManager(host pb.ServerAddress) *DistributedLockManager {
	return &DistributedLockManager{
		lockManager: NewLockManager(),
		LockRing:    NewLockRing(time.Second * 5),
		Host:        host,
	}
}

func (dlm *DistributedLockManager) Lock(key string, token string) (renewToken string, movedTo pb.ServerAddress, err error) {
	return dlm.LockWithTimeout(key, MaxDuration, token)
}

func (dlm *DistributedLockManager) LockWithTimeout(key string, expiredAtNs int64, token string) (renewToken string, movedTo pb.ServerAddress, err error) {
	movedTo, err = dlm.FindLockOwner(key)
	if err != nil {
		return
	}
	if movedTo != dlm.Host {
		return
	}
	renewToken, err = dlm.lockManager.Lock(key, expiredAtNs, token)
	return
}

func (dlm *DistributedLockManager) FindLockOwner(key string) (movedTo pb.ServerAddress, err error) {
	servers := dlm.LockRing.GetSnapshot()
	if servers == nil {
		err = NoLockServerError
		return
	}

	movedTo = hashKeyToServer(key, servers)
	return
}

func (dlm *DistributedLockManager) Unlock(key string, token string) (movedTo pb.ServerAddress, err error) {
	servers := dlm.LockRing.GetSnapshot()
	if servers == nil {
		err = NoLockServerError
		return
	}

	server := hashKeyToServer(key, servers)
	if server != dlm.Host {
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
func (dlm *DistributedLockManager) SelectNotOwnedLocks(servers []pb.ServerAddress) (locks []*Lock) {
	return dlm.lockManager.SelectLocks(func(key string) bool {
		server := hashKeyToServer(key, servers)
		return server != dlm.Host
	})
}
func (dlm *DistributedLockManager) CalculateTargetServer(key string, servers []pb.ServerAddress) pb.ServerAddress {
	return hashKeyToServer(key, servers)
}

func (dlm *DistributedLockManager) IsLocal(key string) bool {
	servers := dlm.LockRing.GetSnapshot()
	if len(servers) <= 1 {
		return true
	}
	return hashKeyToServer(key, servers) == dlm.Host
}
