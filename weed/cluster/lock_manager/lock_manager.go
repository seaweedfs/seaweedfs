package lock_manager

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v2"
	"time"
)

var LockErrorNonEmptyTokenOnNewLock = fmt.Errorf("lock: non-empty token on a new lock")
var LockErrorNonEmptyTokenOnExpiredLock = fmt.Errorf("lock: non-empty token on an expired lock")
var LockErrorTokenMismatch = fmt.Errorf("lock: token mismatch")
var UnlockErrorTokenMismatch = fmt.Errorf("unlock: token mismatch")

// LockManager local lock manager, used by distributed lock manager
type LockManager struct {
	locks *xsync.MapOf[string, *Lock]
}
type Lock struct {
	Token       string
	ExpiredAtNs int64
	Key         string // only used for moving locks
	Owner       string
}

func NewLockManager() *LockManager {
	t := &LockManager{
		locks: xsync.NewMapOf[*Lock](),
	}
	go t.CleanUp()
	return t
}

func (lm *LockManager) Lock(path string, expiredAtNs int64, token string, owner string) (renewToken string, err error) {
	lm.locks.Compute(path, func(oldValue *Lock, loaded bool) (newValue *Lock, delete bool) {
		if oldValue != nil {
			if oldValue.ExpiredAtNs > 0 && oldValue.ExpiredAtNs < time.Now().UnixNano() {
				// lock is expired, set to a new lock
				if token != "" {
					err = LockErrorNonEmptyTokenOnExpiredLock
					return nil, false
				} else {
					// new lock
					renewToken = uuid.New().String()
					return &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner}, false
				}
			}
			// not expired
			if oldValue.Token == token {
				// token matches, renew the lock
				renewToken = uuid.New().String()
				return &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner}, false
			} else {
				err = LockErrorTokenMismatch
				return oldValue, false
			}
		} else {
			if token == "" {
				// new lock
				renewToken = uuid.New().String()
				return &Lock{Token: renewToken, ExpiredAtNs: expiredAtNs, Owner: owner}, false
			} else {
				err = LockErrorNonEmptyTokenOnNewLock
				return nil, false
			}
		}
	})
	return
}

func (lm *LockManager) Unlock(path string, token string) (isUnlocked bool, err error) {
	lm.locks.Compute(path, func(oldValue *Lock, loaded bool) (newValue *Lock, delete bool) {
		if oldValue != nil {
			now := time.Now()
			if oldValue.ExpiredAtNs > 0 && oldValue.ExpiredAtNs < now.UnixNano() {
				// lock is expired, delete it
				isUnlocked = true
				return nil, true
			}
			if oldValue.Token == token {
				if oldValue.ExpiredAtNs <= now.UnixNano() {
					isUnlocked = true
					return nil, true
				}
				return oldValue, false
			} else {
				isUnlocked = false
				err = UnlockErrorTokenMismatch
				return oldValue, false
			}
		} else {
			isUnlocked = true
			return nil, true
		}
	})
	return
}

func (lm *LockManager) CleanUp() {
	for {
		time.Sleep(1 * time.Minute)
		now := time.Now().UnixNano()
		lm.locks.Range(func(key string, value *Lock) bool {
			if value == nil {
				return true
			}
			if now > value.ExpiredAtNs {
				lm.locks.Delete(key)
				return true
			}
			return true
		})
	}
}

// SelectLocks takes out locks by key
// if keyFn return true, the lock will be taken out
func (lm *LockManager) SelectLocks(selectFn func(key string) bool) (locks []*Lock) {
	now := time.Now().UnixNano()
	lm.locks.Range(func(key string, lock *Lock) bool {
		if now > lock.ExpiredAtNs {
			lm.locks.Delete(key)
			return true
		}
		if selectFn(key) {
			lm.locks.Delete(key)
			lock.Key = key
			locks = append(locks, lock)
		}
		return true
	})
	return
}

// InsertLock inserts a lock unconditionally
func (lm *LockManager) InsertLock(path string, expiredAtNs int64, token string, owner string) {
	lm.locks.Store(path, &Lock{Token: token, ExpiredAtNs: expiredAtNs, Owner: owner})
}

func (lm *LockManager) GetLockOwner(key string) (owner string, err error) {
	lm.locks.Range(func(k string, lock *Lock) bool {
		if k == key && lock != nil {
			owner = lock.Owner
			return false
		}
		return true
	})
	return

}
