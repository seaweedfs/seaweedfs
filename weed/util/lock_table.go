package util

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"sync"
	"sync/atomic"
)

// LockTable is a table of locks that can be acquired.
// Locks are acquired in order of request.
type LockTable[T comparable] struct {
	mu        sync.Mutex
	locks     map[T]*LockEntry
	lockIdSeq int64
}

type LockEntry struct {
	mu                   sync.Mutex
	waiters              []*ActiveLock // ordered waiters that are blocked by exclusive locks
	activeLockOwnerCount int32
	lockType             LockType
	cond                 *sync.Cond
}

type LockType int

const (
	SharedLock LockType = iota
	ExclusiveLock
)

type ActiveLock struct {
	ID        int64
	isDeleted bool
	intention string // for debugging
}

func NewLockTable[T comparable]() *LockTable[T] {
	return &LockTable[T]{
		locks: make(map[T]*LockEntry),
	}
}

func (lt *LockTable[T]) NewActiveLock(intention string) *ActiveLock {
	id := atomic.AddInt64(&lt.lockIdSeq, 1)
	l := &ActiveLock{ID: id, intention: intention}
	return l
}

func (lt *LockTable[T]) AcquireLock(intention string, key T, lockType LockType) (lock *ActiveLock) {
	lt.mu.Lock()
	// Get or create the lock entry for the key
	entry, exists := lt.locks[key]
	if !exists {
		entry = &LockEntry{}
		entry.cond = sync.NewCond(&entry.mu)
		lt.locks[key] = entry
	}
	lt.mu.Unlock()

	lock = lt.NewActiveLock(intention)

	// If the lock is held exclusively, wait
	entry.mu.Lock()
	if len(entry.waiters) > 0 || lockType == ExclusiveLock {
		glog.V(4).Infof("ActiveLock %d %s wait for %+v type=%v with waiters %d active %d.\n", lock.ID, lock.intention, key, lockType, len(entry.waiters), entry.activeLockOwnerCount)
		if len(entry.waiters) > 0 {
			for _, waiter := range entry.waiters {
				fmt.Printf(" %d", waiter.ID)
			}
			fmt.Printf("\n")
		}
		entry.waiters = append(entry.waiters, lock)
		if lockType == ExclusiveLock {
			for !lock.isDeleted && ((len(entry.waiters) > 0 && lock.ID != entry.waiters[0].ID) || entry.activeLockOwnerCount > 0) {
				entry.cond.Wait()
			}
		} else {
			for !lock.isDeleted && (len(entry.waiters) > 0 && lock.ID != entry.waiters[0].ID) {
				entry.cond.Wait()
			}
		}
		// Remove the transaction from the waiters list
		if len(entry.waiters) > 0 && lock.ID == entry.waiters[0].ID {
			entry.waiters = entry.waiters[1:]
			entry.cond.Broadcast()
		}
	}
	entry.activeLockOwnerCount++

	// Otherwise, grant the lock
	entry.lockType = lockType
	glog.V(4).Infof("ActiveLock %d %s locked %+v type=%v with waiters %d active %d.\n", lock.ID, lock.intention, key, lockType, len(entry.waiters), entry.activeLockOwnerCount)
	if len(entry.waiters) > 0 {
		for _, waiter := range entry.waiters {
			fmt.Printf(" %d", waiter.ID)
		}
		fmt.Printf("\n")
	}
	entry.mu.Unlock()

	return lock
}

func (lt *LockTable[T]) ReleaseLock(key T, lock *ActiveLock) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	entry, exists := lt.locks[key]
	if !exists {
		return
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	// Remove the transaction from the waiters list
	for i, waiter := range entry.waiters {
		if waiter == lock {
			waiter.isDeleted = true
			entry.waiters = append(entry.waiters[:i], entry.waiters[i+1:]...)
			break
		}
	}

	// If there are no waiters, release the lock
	if len(entry.waiters) == 0 {
		delete(lt.locks, key)
	}

	glog.V(4).Infof("ActiveLock %d %s unlocked %+v type=%v with waiters %d active %d.\n", lock.ID, lock.intention, key, entry.lockType, len(entry.waiters), entry.activeLockOwnerCount)
	if len(entry.waiters) > 0 {
		for _, waiter := range entry.waiters {
			fmt.Printf(" %d", waiter.ID)
		}
		fmt.Printf("\n")
	}
	entry.activeLockOwnerCount--

	// Notify the next waiter
	entry.cond.Broadcast()
}

func main() {

}
