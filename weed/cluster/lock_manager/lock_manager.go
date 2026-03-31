package lock_manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var LockErrorNonEmptyTokenOnNewLock = fmt.Errorf("lock: non-empty token on a new lock")
var LockErrorNonEmptyTokenOnExpiredLock = fmt.Errorf("lock: non-empty token on an expired lock")
var LockErrorTokenMismatch = fmt.Errorf("lock: token mismatch")
var UnlockErrorTokenMismatch = fmt.Errorf("unlock: token mismatch")
var LockNotFound = fmt.Errorf("lock not found")

// LockManager local lock manager, used by distributed lock manager
type LockManager struct {
	locks            map[string]*Lock
	accessLock       sync.RWMutex
	nextGeneration   atomic.Int64
}
type Lock struct {
	Token       string
	ExpiredAtNs int64
	Key         string // only used for moving locks
	Owner       string
	IsBackup    bool   // true if this node holds the lock as a backup
	Generation  int64  // monotonic fencing token, increments on fresh acquisition
	Seq         int64  // per-lock sequence number, increments on every mutation (acquire/renew/unlock)
}

func NewLockManager() *LockManager {
	t := &LockManager{
		locks: make(map[string]*Lock),
	}
	go t.CleanUp()
	return t
}

func (lm *LockManager) NextGeneration() int64 {
	return lm.nextGeneration.Add(1)
}

func (lm *LockManager) Lock(path string, expiredAtNs int64, token string, owner string) (lockOwner, renewToken string, generation int64, seq int64, err error) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	glog.V(4).Infof("lock %s %v %v %v", path, time.Unix(0, expiredAtNs), token, owner)

	if oldValue, found := lm.locks[path]; found {
		if oldValue.ExpiredAtNs > 0 && oldValue.ExpiredAtNs < time.Now().UnixNano() {
			// lock is expired, set to a new lock
			if token != "" {
				glog.V(4).Infof("lock expired key %s non-empty token %v owner %v ts %s", path, token, owner, time.Unix(0, oldValue.ExpiredAtNs))
				err = LockErrorNonEmptyTokenOnExpiredLock
				return
			} else {
				// new lock
				renewToken = uuid.New().String()
				generation = lm.NextGeneration()
				seq = 1
				glog.V(4).Infof("key %s new token %v owner %v generation %d", path, renewToken, owner, generation)
				lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation, Seq: seq}
				return
			}
		}
		// not expired
		lockOwner = oldValue.Owner
		if oldValue.Token == token {
			// token matches, renew the lock
			renewToken = uuid.New().String()
			generation = oldValue.Generation // keep same generation on renewal
			seq = oldValue.Seq + 1
			glog.V(4).Infof("key %s old token %v owner %v => %v owner %v", path, oldValue.Token, oldValue.Owner, renewToken, owner)
			lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation, Seq: seq}
			return
		} else {
			if token == "" {
				// new lock
				glog.V(4).Infof("key %s locked by %v", path, oldValue.Owner)
				err = fmt.Errorf("lock already owned by %v", oldValue.Owner)
				return
			}
			glog.V(4).Infof("key %s expected token %v owner %v received %v from %v", path, oldValue.Token, oldValue.Owner, token, owner)
			err = fmt.Errorf("lock: token mismatch")
			return
		}
	} else {
		glog.V(4).Infof("key %s no lock owner %v", path, owner)
		if token == "" {
			// new lock
			renewToken = uuid.New().String()
			generation = lm.NextGeneration()
			seq = 1
			glog.V(4).Infof("key %s new token %v owner %v generation %d", path, renewToken, owner, generation)
			lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation, Seq: seq}
			return
		} else {
			glog.V(4).Infof("key %s non-empty token %v owner %v", path, token, owner)
			err = LockErrorNonEmptyTokenOnNewLock
			return
		}
	}
}

func (lm *LockManager) Unlock(path string, token string) (isUnlocked bool, seq int64, err error) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if oldValue, found := lm.locks[path]; found {
		now := time.Now()
		if oldValue.ExpiredAtNs > 0 && oldValue.ExpiredAtNs < now.UnixNano() {
			// lock is expired, delete it
			isUnlocked = true
			seq = oldValue.Seq + 1
			glog.V(4).Infof("key %s expired at %v", path, time.Unix(0, oldValue.ExpiredAtNs))
			delete(lm.locks, path)
			return
		}
		if oldValue.Token == token {
			isUnlocked = true
			seq = oldValue.Seq + 1
			glog.V(4).Infof("key %s unlocked with %v", path, token)
			delete(lm.locks, path)
			return
		} else {
			isUnlocked = false
			err = UnlockErrorTokenMismatch
			return
		}
	}
	err = LockNotFound
	return
}

func (lm *LockManager) CleanUp() {

	for {
		time.Sleep(1 * time.Minute)
		now := time.Now().UnixNano()

		lm.accessLock.Lock()
		for key, value := range lm.locks {
			if value == nil {
				continue
			}
			if now > value.ExpiredAtNs {
				glog.V(4).Infof("key %s expired at %v", key, time.Unix(0, value.ExpiredAtNs))
				delete(lm.locks, key)
			}
		}
		lm.accessLock.Unlock()
	}
}

// SelectLocks takes out locks by key
// if selectFn returns true, the lock will be removed and returned
func (lm *LockManager) SelectLocks(selectFn func(key string) bool) (locks []*Lock) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	now := time.Now().UnixNano()

	for key, lock := range lm.locks {
		if now > lock.ExpiredAtNs {
			glog.V(4).Infof("key %s expired at %v", key, time.Unix(0, lock.ExpiredAtNs))
			delete(lm.locks, key)
			continue
		}
		if selectFn(key) {
			glog.V(4).Infof("key %s selected and deleted", key)
			delete(lm.locks, key)
			lock.Key = key
			locks = append(locks, lock)
		}
	}
	return
}

// InsertLock inserts a lock unconditionally (used for lock transfers)
func (lm *LockManager) InsertLock(path string, expiredAtNs int64, token string, owner string, generation int64, seq int64) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	lm.locks[path] = &Lock{Token: token, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation, Seq: seq}
}

// InsertBackupLock inserts or updates a lock as a backup copy.
// It rejects stale mutations: if the incoming seq is <= the current seq, the update is ignored.
func (lm *LockManager) InsertBackupLock(path string, expiredAtNs int64, token string, owner string, generation int64, seq int64) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if existing, found := lm.locks[path]; found && existing.IsBackup && seq <= existing.Seq {
		// Stale replication — ignore
		glog.V(4).Infof("backup lock %s: rejecting stale seq %d (current %d)", path, seq, existing.Seq)
		return
	}
	lm.locks[path] = &Lock{Token: token, ExpiredAtNs: expiredAtNs, Owner: owner, IsBackup: true, Generation: generation, Seq: seq}
	// Advance nextGeneration so that after failover promotion, new locks
	// get a generation strictly greater than any replicated lock.
	if cur := lm.nextGeneration.Load(); generation >= cur {
		lm.nextGeneration.Store(generation)
	}
}

// RemoveLock removes a lock by key
func (lm *LockManager) RemoveLock(path string) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()
	delete(lm.locks, path)
}

// RemoveBackupLockIfSeq removes a backup lock only if the seq matches or exceeds the current seq.
// This prevents a late-arriving unlock replication from deleting a newer lock.
func (lm *LockManager) RemoveBackupLockIfSeq(path string, seq int64) bool {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if existing, found := lm.locks[path]; found && existing.IsBackup {
		if seq >= existing.Seq {
			delete(lm.locks, path)
			return true
		}
		glog.V(4).Infof("backup lock %s: rejecting stale unlock seq %d (current %d)", path, seq, existing.Seq)
		return false
	}
	// Not found or not a backup — remove anyway
	delete(lm.locks, path)
	return true
}

// GetLock returns a copy of the lock for a key, if it exists and is not expired
func (lm *LockManager) GetLock(key string) (*Lock, bool) {
	lm.accessLock.RLock()
	defer lm.accessLock.RUnlock()

	lock, found := lm.locks[key]
	if !found {
		return nil, false
	}
	if time.Now().UnixNano() > lock.ExpiredAtNs {
		return nil, false
	}
	// Return a copy
	cp := *lock
	return &cp, true
}

func (lm *LockManager) GetLockOwner(key string) (owner string, err error) {
	lm.accessLock.RLock()
	defer lm.accessLock.RUnlock()

	if lock, found := lm.locks[key]; found {
		return lock.Owner, nil
	}
	err = LockNotFound
	return
}

// AllLocks returns a copy of all non-expired locks
func (lm *LockManager) AllLocks() []*Lock {
	lm.accessLock.RLock()
	defer lm.accessLock.RUnlock()

	now := time.Now().UnixNano()
	var result []*Lock
	for key, lock := range lm.locks {
		if now > lock.ExpiredAtNs {
			continue
		}
		cp := *lock
		cp.Key = key
		result = append(result, &cp)
	}
	return result
}

// PromoteLock changes a backup lock to a primary lock
func (lm *LockManager) PromoteLock(key string) bool {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if lock, found := lm.locks[key]; found && lock.IsBackup {
		lock.IsBackup = false
		return true
	}
	return false
}

// DemoteLock changes a primary lock to a backup lock
func (lm *LockManager) DemoteLock(key string) bool {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if lock, found := lm.locks[key]; found && !lock.IsBackup {
		lock.IsBackup = true
		return true
	}
	return false
}
