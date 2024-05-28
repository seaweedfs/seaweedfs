package lock_manager

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"sync"
	"time"
)

var LockErrorNonEmptyTokenOnNewLock = fmt.Errorf("lock: non-empty token on a new lock")
var LockErrorNonEmptyTokenOnExpiredLock = fmt.Errorf("lock: non-empty token on an expired lock")
var LockErrorTokenMismatch = fmt.Errorf("lock: token mismatch")
var UnlockErrorTokenMismatch = fmt.Errorf("unlock: token mismatch")
var LockNotFound = fmt.Errorf("lock not found")

// LockManager local lock manager, used by distributed lock manager
type LockManager struct {
	locks      map[string]*Lock
	accessLock sync.RWMutex
}
type Lock struct {
	Token       string
	ExpiredAtNs int64
	Key         string // only used for moving locks
	Owner       string
}

func NewLockManager() *LockManager {
	t := &LockManager{
		locks: make(map[string]*Lock),
	}
	go t.CleanUp()
	return t
}

func (lm *LockManager) Lock(path string, expiredAtNs int64, token string, owner string) (lockOwner, renewToken string, err error) {
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
				glog.V(4).Infof("key %s new token %v owner %v", path, renewToken, owner)
				lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner}
				return
			}
		}
		// not expired
		lockOwner = oldValue.Owner
		if oldValue.Token == token {
			// token matches, renew the lock
			renewToken = uuid.New().String()
			glog.V(4).Infof("key %s old token %v owner %v => %v owner %v", path, oldValue.Token, oldValue.Owner, renewToken, owner)
			lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner}
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
			glog.V(4).Infof("key %s new token %v owner %v", path, token, owner)
			renewToken = uuid.New().String()
			lm.locks[path] = &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner}
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

func (lm *LockManager) GetLockOwner(key string) (owner string, err error) {
	lm.accessLock.RLock()
	defer lm.accessLock.RUnlock()

	if lock, found := lm.locks[key]; found {
		return lock.Owner, nil
	}
	err = LockNotFound
	return
}
