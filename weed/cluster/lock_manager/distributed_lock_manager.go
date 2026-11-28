package lock_manager

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

const RenewInterval = time.Second * 3
const LiveLockTTL = time.Second * 7

var NoLockServerError = fmt.Errorf("no lock server found")

// ReplicateFunc is called to replicate a lock operation to a backup server.
// The caller (filer server) provides this to avoid a circular dependency.
// seq is a per-lock monotonic sequence number for causal ordering — the backup
// rejects mutations with seq <= its current seq for that key.
type ReplicateFunc func(server pb.ServerAddress, key string, expiredAtNs int64, token string, prevToken string, owner string, generation int64, seq int64, isUnlock bool)

type DistributedLockManager struct {
	lockManager *LockManager
	LockRing    *LockRing
	Host        pb.ServerAddress
	ReplicateFn ReplicateFunc // set by filer server after creation
}

func NewDistributedLockManager(host pb.ServerAddress) *DistributedLockManager {
	return &DistributedLockManager{
		lockManager: NewLockManager(),
		LockRing:    NewLockRing(time.Second * 5),
		Host:        host,
	}
}

func (dlm *DistributedLockManager) LockWithTimeout(key string, expiredAtNs int64, token string, owner string) (lockOwner string, renewToken string, generation int64, movedTo pb.ServerAddress, err error) {
	primary, _ := dlm.LockRing.GetPrimaryAndBackup(key)
	if primary == "" {
		err = NoLockServerError
		return
	}
	if primary != dlm.Host {
		// If this is a renewal (non-empty token) and we still hold the lock locally,
		// serve it here rather than redirecting. This handles the window between
		// ring update and lock transfer completion — the old primary remains
		// authoritative for locks it still holds.
		if token != "" {
			if lock, found := dlm.lockManager.GetLock(key); found && !lock.IsBackup && (lock.Token == token || lock.PrevToken == token) {
				var seq, leaseExpires int64
				lockOwner, renewToken, generation, seq, leaseExpires, err = dlm.lockManager.lockWithExpiry(key, expiredAtNs, token, owner)
				if err == nil && renewToken != "" {
					dlm.replicateToBackup(key, leaseExpires, renewToken, token, owner, generation, seq, false)
				}
				return
			}
		}
		movedTo = primary
		return
	}
	var seq, leaseExpires int64
	lockOwner, renewToken, generation, seq, leaseExpires, err = dlm.lockManager.lockWithExpiry(key, expiredAtNs, token, owner)
	if err == nil && renewToken != "" {
		dlm.replicateToBackup(key, leaseExpires, renewToken, token, owner, generation, seq, false)
	}
	return
}

func (dlm *DistributedLockManager) FindLockOwner(key string) (owner string, movedTo pb.ServerAddress, err error) {
	primary, _ := dlm.LockRing.GetPrimaryAndBackup(key)
	if primary == "" {
		err = NoLockServerError
		return
	}
	if primary != dlm.Host {
		// If we still hold this lock locally, serve it here
		if lock, found := dlm.lockManager.GetLock(key); found && !lock.IsBackup {
			owner = lock.Owner
			return
		}
		movedTo = primary
		servers := dlm.LockRing.GetSnapshot()
		glog.V(0).Infof("lock %s not on current %s but on %s from %v", key, dlm.Host, movedTo, servers)
		return
	}
	owner, err = dlm.lockManager.GetLockOwner(key)
	return
}

func (dlm *DistributedLockManager) Unlock(key string, token string) (movedTo pb.ServerAddress, err error) {
	primary, _ := dlm.LockRing.GetPrimaryAndBackup(key)
	if primary == "" {
		err = NoLockServerError
		return
	}
	if primary != dlm.Host {
		// The old primary can still release a lease awaiting handoff.
		lock, found := dlm.lockManager.GetLock(key)
		if !found || lock.IsBackup || token == "" || (lock.Token != token && lock.PrevToken != token) {
			movedTo = primary
			return
		}
	}
	released, unlockErr := dlm.lockManager.unlock(key, token)
	err = unlockErr
	if released != nil {
		dlm.replicateToBackup(key, released.ExpiredAtNs, released.Token, "", released.Owner, released.Generation, released.Seq, true)
	}
	return
}

// InsertLock is used to insert a lock to a server unconditionally.
// It is used when a server is down and the lock is moved to another server.
// After inserting, it replicates to the backup for this key.
func (dlm *DistributedLockManager) InsertLock(key string, expiredAtNs int64, token string, prevToken string, owner string, generation int64, seq int64) {
	if dlm.lockManager.InsertLock(key, expiredAtNs, token, prevToken, owner, generation, seq) {
		dlm.replicateToBackup(key, expiredAtNs, token, prevToken, owner, generation, seq, false)
	}
}

// InsertBackupLock inserts a lock as a backup copy, rejecting stale seq
func (dlm *DistributedLockManager) InsertBackupLock(key string, expiredAtNs int64, token string, prevToken string, owner string, generation int64, seq int64) {
	dlm.lockManager.InsertBackupLock(key, expiredAtNs, token, prevToken, owner, generation, seq)
}

// RemoveBackupLock removes a backup lock unconditionally
func (dlm *DistributedLockManager) RemoveBackupLock(key string) {
	dlm.lockManager.RemoveLock(key)
}

// RemoveBackupLockIfSeq records a release and removes only the matching lease.
func (dlm *DistributedLockManager) RemoveBackupLockIfSeq(key string, generation int64, seq int64, token string, expiredAtNs int64) {
	dlm.lockManager.RemoveBackupLockIfSeq(key, generation, seq, token, expiredAtNs)
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
	primary := dlm.LockRing.GetPrimary(key)
	if primary == "" {
		return true
	}
	return primary == dlm.Host
}

// AllLocks returns all non-expired locks on this node
func (dlm *DistributedLockManager) AllLocks() []*Lock {
	return dlm.lockManager.AllLocks()
}

// PromoteLock promotes a backup lock to primary
func (dlm *DistributedLockManager) PromoteLock(key string) bool {
	return dlm.lockManager.PromoteLock(key)
}

// DemoteLock demotes a primary lock to backup
func (dlm *DistributedLockManager) DemoteLock(key string) bool {
	return dlm.lockManager.DemoteLock(key)
}

// GetLock returns a copy of a lock if it exists
func (dlm *DistributedLockManager) GetLock(key string) (*Lock, bool) {
	return dlm.lockManager.GetLock(key)
}

// PrepareLockTransfer records that a handoff may send this lease elsewhere.
// Do not send a snapshot if its lease has already been released locally.
func (dlm *DistributedLockManager) PrepareLockTransfer(lock *Lock) bool {
	return dlm.lockManager.markReplicated(lock.Key, lock.Token)
}

// ReplicateLock sends an existing lease to its current backup after a topology
// change, recording that the local lease may now have a remote copy.
func (dlm *DistributedLockManager) ReplicateLock(lock *Lock) {
	dlm.replicateToBackup(lock.Key, lock.ExpiredAtNs, lock.Token, lock.PrevToken, lock.Owner, lock.Generation, lock.Seq, false)
}

// replicateToBackup asynchronously replicates a lock operation to the backup server
func (dlm *DistributedLockManager) replicateToBackup(key string, expiredAtNs int64, token string, prevToken string, owner string, generation int64, seq int64, isUnlock bool) {
	_, backup := dlm.LockRing.GetPrimaryAndBackup(key)
	if backup == "" {
		return // single-server deployment, no backup
	}
	if dlm.ReplicateFn != nil {
		if !isUnlock && !dlm.lockManager.markReplicated(key, token) {
			return
		}
		go dlm.ReplicateFn(backup, key, expiredAtNs, token, prevToken, owner, generation, seq, isUnlock)
	}
}
