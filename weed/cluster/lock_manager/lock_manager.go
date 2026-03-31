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

func (lm *LockManager) Lock(path string, expiredAtNs int64, token string, owner string) (lockOwner, renewToken string, generation int64, err error) {
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
				glog.V(4).Infof("key %s new token %v owner %v generation %d", path, renewToken, owner, generation)
				lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation}
				return
			}
		}
		// not expired
		lockOwner = oldValue.Owner
		if oldValue.Token == token {
			// token matches, renew the lock
			renewToken = uuid.New().String()
			generation = oldValue.Generation // keep same generation on renewal
			glog.V(4).Infof("key %s old token %v owner %v => %v owner %v", path, oldValue.Token, oldValue.Owner, renewToken, owner)
			lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation}
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
			glog.V(4).Infof("key %s new token %v owner %v generation %d", path, renewToken, owner, generation)
			lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner, Generation: generation}
			return
		} else {
			glog.V(4).Infof("key %s non-empty token %v owner %v", path, token, owner)
			err = LockErrorNonEmptyTokenOnNewLock
			return
		}
	}
}

func (lm *LockManager) Unlock(path string, token string) (isUnlocked bool, err error) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	if oldValue, found := lm.locks[path]; found {
		now := time.Now()
		if oldValue.ExpiredAtNs > 0 && oldValue.ExpiredAtNs < now.UnixNano() {
			// lock is expired, delete it
			isUnlocked = true
			glog.V(4).Infof("key %s expired at %v", path, time.Unix(0, oldValue.ExpiredAtNs))
			delete(lm.locks, path)
			return
		}
		if oldValue.Token == token {
			isUnlocked = true
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
// if keyFn return true, the lock will be taken out
func (lm *LockManager) SelectLocks(selectFn func(key string) bool) (locks []*Lock) {
	lm.accessLock.RLock()
	defer lm.accessLock.RUnlock()

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

// InsertLock inserts a lock unconditionally
func (lm *LockManager) InsertLock(path string, expiredAtNs int64, token string, owner string) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	lm.locks[path] = &Lock{Token: token, ExpiredAtNs: expiredAtNs, Owner: owner}
}

// InsertBackupLock inserts a lock as a backup copy
func (lm *LockManager) InsertBackupLock(path string, expiredAtNs int64, token string, owner string, generation int64) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()

	lm.locks[path] = &Lock{Token: token, ExpiredAtNs: expiredAtNs, Owner: owner, IsBackup: true, Generation: generation}
}

// RemoveLock removes a lock by key
func (lm *LockManager) RemoveLock(path string) {
	lm.accessLock.Lock()
	defer lm.accessLock.Unlock()
	delete(lm.locks, path)
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
