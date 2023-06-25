package lock_manager

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v2"
	"time"
)

// LockManager lock manager
type LockManager struct {
	locks *xsync.MapOf[string, *Lock]
}
type Lock struct {
	Token        string
	ExpirationNs int64
	Key          string // only used for moving locks
}

func NewLockManager() *LockManager {
	t := &LockManager{
		locks: xsync.NewMapOf[*Lock](),
	}
	go t.CleanUp()
	return t
}

func (lm *LockManager) Lock(path string, ttlDuration time.Duration, token string) (renewToken string, err error) {
	lm.locks.Compute(path, func(oldValue *Lock, loaded bool) (newValue *Lock, delete bool) {
		if oldValue != nil {
			now := time.Now()
			if oldValue.ExpirationNs > 0 && oldValue.ExpirationNs < now.UnixNano() {
				// lock is expired, set to a new lock
				expirationNs := time.Now().Add(ttlDuration).UnixNano()
				return &Lock{Token: token, ExpirationNs: expirationNs}, false
			}
			if oldValue.Token == token {
				expirationNs := time.Now().Add(ttlDuration).UnixNano()
				return &Lock{Token: token, ExpirationNs: expirationNs}, false
			} else {
				err = fmt.Errorf("lock: token mismatch")
				return oldValue, false
			}
		} else {
			expirationNs := time.Now().Add(ttlDuration).UnixNano()
			if token == "" {
				renewToken = uuid.New().String()
				return &Lock{Token: renewToken, ExpirationNs: expirationNs}, false
			} else {
				err = fmt.Errorf("lock: non-empty token on a new lock")
				return nil, false
			}
			return &Lock{Token: token, ExpirationNs: expirationNs}, false
		}
	})
	return
}

func (lm *LockManager) Unlock(path string, token string) (isUnlocked bool, err error) {
	lm.locks.Compute(path, func(oldValue *Lock, loaded bool) (newValue *Lock, delete bool) {
		if oldValue != nil {
			now := time.Now()
			if oldValue.ExpirationNs > 0 && oldValue.ExpirationNs < now.UnixNano() {
				// lock is expired, delete it
				isUnlocked = true
				return nil, true
			}
			if oldValue.Token == token {
				if oldValue.ExpirationNs <= now.UnixNano() {
					isUnlocked = true
					return nil, true
				}
				return oldValue, false
			} else {
				isUnlocked = false
				err = fmt.Errorf("unlock: token mismatch")
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
			if now > value.ExpirationNs {
				lm.locks.Delete(key)
				return true
			}
			return true
		})
	}
}

// TakeOutLocksByKey takes out locks by key
// if keyFn return true, the lock will be taken out
func (lm *LockManager) TakeOutLocksByKey(keyFn func(key string) bool) (locks []*Lock) {
	now := time.Now().UnixNano()
	lm.locks.Range(func(key string, lock *Lock) bool {
		if now > lock.ExpirationNs {
			lm.locks.Delete(key)
			return true
		}
		if keyFn(key) {
			lm.locks.Delete(key)
			lock.Key = key
			locks = append(locks, lock)
		}
		return true
	})
	return
}
