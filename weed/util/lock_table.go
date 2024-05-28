package util

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// LockTable is a table of locks that can be acquired.
// Locks are acquired in order of request.
type LockTable[T comparable] struct {
	lockIdSeq     int64
	mu            sync.Mutex
	locks         map[T]*LockEntry
	locksInFlight map[T]int
}

type LockEntry struct {
	mu                            sync.Mutex
	waiters                       []*ActiveLock // ordered waiters that are blocked by exclusive locks
	activeSharedLockOwnerCount    int32
	activeExclusiveLockOwnerCount int32
	cond                          *sync.Cond
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
	lockType  LockType
}

func NewLockTable[T comparable]() *LockTable[T] {
	return &LockTable[T]{
		locks:         make(map[T]*LockEntry),
		locksInFlight: make(map[T]int),
	}
}

func (lt *LockTable[T]) NewActiveLock(intention string, lockType LockType) *ActiveLock {
	id := atomic.AddInt64(&lt.lockIdSeq, 1)
	l := &ActiveLock{ID: id, intention: intention, lockType: lockType}
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
		lt.locksInFlight[key] = 0
	}
	lt.locksInFlight[key]++
	lt.mu.Unlock()

	lock = lt.NewActiveLock(intention, lockType)

	// If the lock is held exclusively, wait
	entry.mu.Lock()
	if len(entry.waiters) > 0 || lockType == ExclusiveLock || entry.activeExclusiveLockOwnerCount > 0 {
		if glog.V(4) {
			fmt.Printf("ActiveLock %d %s wait for %+v type=%v with waiters %d active r%d w%d.\n", lock.ID, lock.intention, key, lockType, len(entry.waiters), entry.activeSharedLockOwnerCount, entry.activeExclusiveLockOwnerCount)
			if len(entry.waiters) > 0 {
				for _, waiter := range entry.waiters {
					fmt.Printf(" %d", waiter.ID)
				}
				fmt.Printf("\n")
			}
		}
		entry.waiters = append(entry.waiters, lock)
		if lockType == ExclusiveLock {
			for !lock.isDeleted && ((len(entry.waiters) > 0 && lock.ID != entry.waiters[0].ID) || entry.activeExclusiveLockOwnerCount > 0 || entry.activeSharedLockOwnerCount > 0) {
				entry.cond.Wait()
			}
		} else {
			for !lock.isDeleted && (len(entry.waiters) > 0 && lock.ID != entry.waiters[0].ID) || entry.activeExclusiveLockOwnerCount > 0 {
				entry.cond.Wait()
			}
		}
		// Remove the transaction from the waiters list
		if len(entry.waiters) > 0 && lock.ID == entry.waiters[0].ID {
			entry.waiters = entry.waiters[1:]
			entry.cond.Broadcast()
		}
	}

	// Otherwise, grant the lock
	if glog.V(4) {
		fmt.Printf("ActiveLock %d %s locked %+v type=%v with waiters %d active r%d w%d.\n", lock.ID, lock.intention, key, lockType, len(entry.waiters), entry.activeSharedLockOwnerCount, entry.activeExclusiveLockOwnerCount)
		if len(entry.waiters) > 0 {
			for _, waiter := range entry.waiters {
				fmt.Printf(" %d", waiter.ID)
			}
			fmt.Printf("\n")
		}
	}
	if lock.lockType == ExclusiveLock {
		entry.activeExclusiveLockOwnerCount++
	} else {
		entry.activeSharedLockOwnerCount++
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

	lt.locksInFlight[key]--
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

	if lock.lockType == ExclusiveLock {
		entry.activeExclusiveLockOwnerCount--
	} else {
		entry.activeSharedLockOwnerCount--
	}

	// If there are no waiters, release the lock
	if len(entry.waiters) == 0 && lt.locksInFlight[key] <= 0 && entry.activeExclusiveLockOwnerCount <= 0 && entry.activeSharedLockOwnerCount <= 0 {
		delete(lt.locks, key)
		delete(lt.locksInFlight, key)
	}

	if glog.V(4) {
		fmt.Printf("ActiveLock %d %s unlocked %+v type=%v with waiters %d active r%d w%d.\n", lock.ID, lock.intention, key, lock.lockType, len(entry.waiters), entry.activeSharedLockOwnerCount, entry.activeExclusiveLockOwnerCount)
		if len(entry.waiters) > 0 {
			for _, waiter := range entry.waiters {
				fmt.Printf(" %d", waiter.ID)
			}
			fmt.Printf("\n")
		}
	}

	// Notify the next waiter
	entry.cond.Broadcast()
}

func main() {

}
