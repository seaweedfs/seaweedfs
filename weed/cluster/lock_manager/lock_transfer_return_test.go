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
		if ok, _, _, err := manager.Unlock("key", "token"); !ok || err != nil {
			t.Fatalf("unlock: ok=%v err=%v", ok, err)
		}
		if manager.InsertLock("key", expiry, "token", "", "owner", 3, 1) {
			t.Fatal("completed unlock resurrected by transfer")
		}
		// Installing and then evicting a newer acquisition must not erase the
		// earlier release record. The newer live lease may still return.
		_, nextToken, gen, seq, err := manager.Lock("key", expiry, "", "next-owner")
		if err != nil {
			t.Fatal(err)
		}
		if selectLocks {
			manager.SelectLocks(func(string) bool { return true })
		} else {
			manager.RemoveLock("key")
		}
		if manager.InsertLock("key", expiry, "token", "", "owner", 3, 1) {
			t.Fatal("new acquisition and ownership eviction erased an earlier release")
		}
		manager.InsertBackupLock("key", expiry, "token", "", "owner", 3, 1)
		if _, found := manager.GetLock("key"); found {
			t.Fatal("earlier release resurrected by replication after ownership eviction")
		}
		if !manager.InsertLock("key", expiry, nextToken, "", "next-owner", gen, seq) {
			t.Fatal("newer live lease could not return after ownership eviction")
		}
	}
}
