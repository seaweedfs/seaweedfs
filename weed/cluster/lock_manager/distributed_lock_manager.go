package lock_manager

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	mutations   *util.LockTable[string]
	LockRing    *LockRing
	Host        pb.ServerAddress
	ReplicateFn ReplicateFunc // set by filer server after creation
	// TransferFn waits for the receiver; an error may still mean it committed.
	TransferFn func(pb.ServerAddress, *Lock) error
	// RetryTransferFn enqueues work without waiting for this key gate.
	RetryTransferFn func(string)
	// Tests set this before concurrent use, to observe admission contention.
	onMutationWait func(string)
}

func NewDistributedLockManager(host pb.ServerAddress) *DistributedLockManager {
	return &DistributedLockManager{
		lockManager: NewLockManager(),
		mutations:   util.NewLockTable[string](),
		LockRing:    NewLockRing(time.Second * 5),
		Host:        host,
	}
}

func (dlm *DistributedLockManager) acquireMutation(key string) *util.ActiveLock {
	if mutation, ok := dlm.mutations.TryAcquireLock("lock mutation", key, util.ExclusiveLock); ok {
		return mutation
	}
	if dlm.onMutationWait != nil {
		dlm.onMutationWait(key)
	}
	return dlm.mutations.AcquireLock("lock mutation", key, util.ExclusiveLock)
}

func (dlm *DistributedLockManager) LockWithTimeout(key string, expiredAtNs int64, token string, owner string) (lockOwner string, renewToken string, generation int64, movedTo pb.ServerAddress, err error) {
	mutation := dlm.acquireMutation(key)
	defer dlm.mutations.ReleaseLock(key, mutation)
	primary, _ := dlm.LockRing.GetPrimaryAndBackup(key)
	if primary != "" && primary != dlm.Host {
		// The new owner must receive any local lease before the client is
		// forwarded. Only the new primary can rotate its renewal token.
		if primary, err = dlm.transferToOwnerLocked(key); err != nil {
			return
		}
	}
	if primary == "" {
		err = NoLockServerError
		return
	}
	if primary != dlm.Host {
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
	mutation := dlm.acquireMutation(key)
	defer dlm.mutations.ReleaseLock(key, mutation)
	primary, _ := dlm.LockRing.GetPrimaryAndBackup(key)
	if primary != "" && primary != dlm.Host {
		// Install the lease at the new primary before forwarding unlock,
		// including when unlock wins admission before the topology callback.
		if primary, err = dlm.transferToOwnerLocked(key); err != nil {
			return
		}
	}
	if primary == "" {
		err = NoLockServerError
		return
	}
	if primary != dlm.Host {
		movedTo = primary
		return
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
	if effectiveExpiry, accepted := dlm.lockManager.insertLock(key, expiredAtNs, token, prevToken, owner, generation, seq); accepted {
		dlm.replicateToBackup(key, effectiveExpiry, token, prevToken, owner, generation, seq, false)
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

// TransferLock serializes a fresh snapshot and handoff with local mutations.
// Incoming replication bypasses admission so the receiver can replicate back.
func (dlm *DistributedLockManager) TransferLock(key string, keepBackup bool, transfer func(*Lock) error) error {
	mutation := dlm.acquireMutation(key)
	defer dlm.mutations.ReleaseLock(key, mutation)
	return dlm.transferLocked(key, keepBackup, transfer)
}

func (dlm *DistributedLockManager) transferLocked(key string, keepBackup bool, transfer func(*Lock) error) error {
	current, err := dlm.sendTransferLocked(key, transfer)
	if err != nil || current == nil {
		return err
	}
	dlm.lockManager.finishTransfer(key, current.Token, keepBackup)
	return nil
}

func (dlm *DistributedLockManager) sendTransferLocked(key string, transfer func(*Lock) error) (*Lock, error) {
	current, found := dlm.lockManager.beginTransfer(key)
	if !found {
		return nil, nil
	}
	// On error retain the frozen copy: the receiver may have committed.
	return current, transfer(current)
}

// TransferLockToOwner is also called by the retry worker without a ring change.
func (dlm *DistributedLockManager) TransferLockToOwner(key string) error {
	mutation := dlm.acquireMutation(key)
	defer dlm.mutations.ReleaseLock(key, mutation)
	_, err := dlm.transferToOwnerLocked(key)
	return err
}

// transferToOwnerLocked returns the primary that owns the lease afterwards.
func (dlm *DistributedLockManager) transferToOwnerLocked(key string) (owner pb.ServerAddress, err error) {
	defer func() {
		if err != nil && dlm.RetryTransferFn != nil {
			dlm.RetryTransferFn(key)
		}
	}()
	primary, backup := dlm.LockRing.GetPrimaryAndBackup(key)
	current, found := dlm.lockManager.GetLock(key)
	if !found {
		return primary, nil
	}
	if primary == "" {
		return "", NoLockServerError
	}
	if primary == dlm.Host {
		if current.handoff {
			dlm.lockManager.PromoteLock(key)
		}
		return primary, nil
	}
	if current.IsBackup && !current.handoff && backup == dlm.Host {
		return primary, nil
	}
	if dlm.TransferFn == nil {
		return "", LockHandoffPending
	}
	transferred, err := dlm.sendTransferLocked(key, func(lock *Lock) error {
		return dlm.TransferFn(primary, lock)
	})
	if err != nil || transferred == nil {
		return primary, err
	}
	// Background retries can overlap a newer topology callback. Never evict
	// a lease now owned here or discard it before its newest owner receives it.
	currentPrimary, currentBackup := dlm.LockRing.GetPrimaryAndBackup(key)
	if currentPrimary == dlm.Host {
		dlm.lockManager.PromoteLock(key)
	} else if currentPrimary == primary {
		dlm.lockManager.finishTransfer(key, transferred.Token, currentBackup == dlm.Host)
	} else {
		return "", LockHandoffPending
	}
	return currentPrimary, nil
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
