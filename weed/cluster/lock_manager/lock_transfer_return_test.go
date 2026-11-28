package lock_manager

import (
	"testing"
	"time"
)

func TestLiveLockMayReturnAfterOwnershipEviction(t *testing.T) {
	for _, selectLocks := range []bool{false, true} {
		manager := NewLockManager()
		expiry := time.Now().Add(time.Minute).UnixNano()
		manager.InsertLock("key", expiry, "token", "", "owner", 3, 1)
		if selectLocks {
			manager.SelectLocks(func(string) bool { return true })
		} else {
			manager.RemoveLock("key")
		}
		if !manager.InsertLock("key", expiry, "token", "", "owner", 3, 1) {
			t.Fatalf("live transfer rejected after eviction (SelectLocks=%v)", selectLocks)
		}
		manager.Unlock("key", "token")
		if manager.InsertLock("key", expiry, "token", "", "owner", 3, 1) {
			t.Fatal("completed unlock resurrected by transfer")
		}
	}
}
