package util

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestOrderedLock(t *testing.T) {
	lt := NewLockTable[string]()

	var wg sync.WaitGroup
	// Simulate transactions requesting locks
	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "resource"
			lockType := SharedLock
			if i%5 == 0 {
				lockType = ExclusiveLock
			}

			// Simulate attempting to acquire the lock
			lock := lt.AcquireLock("", key, lockType)

			// Lock acquired, perform some work
			fmt.Printf("ActiveLock %d acquired lock %v\n", lock.ID, lockType)

			// Simulate some work
			time.Sleep(time.Duration(rand.Int31n(10)*10) * time.Millisecond)

			// Release the lock
			lt.ReleaseLock(key, lock)
			fmt.Printf("ActiveLock %d released lock %v\n", lock.ID, lockType)
		}(i)
	}

	wg.Wait()
}

func TestShouldWaitForSharedLock(t *testing.T) {
	lock := &ActiveLock{ID: 2, lockType: SharedLock}
	entry := &LockEntry{
		waiters: []*ActiveLock{{ID: 1}, lock},
	}

	if !shouldWaitForSharedLock(lock, entry) {
		t.Fatal("shared lock should wait behind earlier waiters")
	}

	entry.waiters = []*ActiveLock{lock}
	entry.activeExclusiveLockOwnerCount = 1
	if !shouldWaitForSharedLock(lock, entry) {
		t.Fatal("shared lock should wait while an exclusive owner is active")
	}

	lock.isDeleted = true
	if shouldWaitForSharedLock(lock, entry) {
		t.Fatal("deleted shared lock should stop waiting even if an exclusive owner is active")
	}
}
