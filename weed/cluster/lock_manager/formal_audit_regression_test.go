package lock_manager_test

import (
	"testing"
	"time"

	lm "github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// These regression specifications intentionally fail on the audited revision.
// They call the real Go implementation, not a reimplementation of the model.
func TestFormalAuditReleasedLockMustNotResurrect(t *testing.T) {
	for _, releaseFirst := range []bool{false, true} {
		name := "insert_release_delayed_renewal"
		if releaseFirst {
			name = "release_before_insert"
		}
		t.Run(name, func(t *testing.T) {
			backup := lm.NewLockManager()
			expires := time.Now().Add(time.Minute).UnixNano()
			if !releaseFirst {
				backup.InsertBackupLock("key", expires, "lease/token", "", "owner", 1, 1)
			}
			if !backup.RemoveBackupLockIfSeq("key", 1, 3, "lease/token3", expires) {
				t.Fatal("release unexpectedly rejected")
			}
			backup.InsertBackupLock("key", expires, "lease/token2", "lease/token", "owner", 1, 2)
			if got, ok := backup.GetLock("key"); ok {
				t.Fatalf("released lock resurrected: owner=%s generation=%d seq=%d", got.Owner, got.Generation, got.Seq)
			}
		})
	}
}

func TestFormalAuditStaleTransferMustNotResurrect(t *testing.T) {
	manager := lm.NewLockManager()
	expires := time.Now().Add(time.Minute).UnixNano()
	manager.InsertLock("key", expires, "token", "", "owner", 3, 1)
	if ok, _, _, err := manager.Unlock("key", "token"); !ok || err != nil {
		t.Fatalf("unlock: ok=%v err=%v", ok, err)
	}
	if accepted := manager.InsertLock("key", expires, "token", "", "owner", 3, 1); accepted {
		t.Fatal("completed release was undone by a stale transfer")
	}
}

func TestFormalAuditOrderedReplicationControl(t *testing.T) {
	backup := lm.NewLockManager()
	expires := time.Now().Add(time.Minute).UnixNano()
	backup.InsertBackupLock("key", expires, "lease/token", "", "owner", 1, 1)
	backup.RemoveBackupLockIfSeq("key", 1, 2, "lease/token", expires)
	if _, ok := backup.GetLock("key"); ok {
		t.Fatal("ordered release failed")
	}
	backup.InsertBackupLock("key", expires, "new", "", "new-owner", 2, 1)
	backup.RemoveBackupLockIfSeq("key", 1, 2, "lease/token", expires)
	got, ok := backup.GetLock("key")
	if !ok || got.Generation != 2 {
		t.Fatal("older delete removed a newer live record")
	}
}

// Drive the real asynchronous replication callback and actual DLM acquire /
// release APIs. The callback is a controllable reliable network: each captured
// message is delivered exactly once, with no loss, duplication or fabrication.
func TestFormalAuditAsyncReplicationReleaseFailover(t *testing.T) {
	for _, reordered := range []bool{false, true} {
		name := "ordered_control"
		if reordered {
			name = "reordered"
		}
		t.Run(name, func(t *testing.T) {
			hosts := []pb.ServerAddress{"formal-a:8888", "formal-b:8888"}
			a, b := lm.NewDistributedLockManager(hosts[0]), lm.NewDistributedLockManager(hosts[1])
			a.LockRing.SetSnapshot(append([]pb.ServerAddress(nil), hosts...), 1)
			b.LockRing.SetSnapshot(append([]pb.ServerAddress(nil), hosts...), 1)
			key := "formal-replication-key"
			primary, _ := a.LockRing.GetPrimaryAndBackup(key)
			if primary != a.Host {
				a, b = b, a
			}
			type message struct {
				expires, generation, seq int64
				token, prev, owner       string
				isUnlock                 bool
			}
			network := make(chan message, 4)
			a.ReplicateFn = func(_ pb.ServerAddress, _ string, expiry int64, token, prev, owner string, gen, seq int64, unlock bool) {
				network <- message{expiry, gen, seq, token, prev, owner, unlock}
			}
			receive := func() message {
				select {
				case m := <-network:
					return m
				case <-time.After(5 * time.Second):
					t.Fatal("replication callback not invoked")
					return message{}
				}
			}
			deliver := func(m message) {
				if m.isUnlock {
					b.RemoveBackupLockIfSeq(key, m.generation, m.seq, m.token, m.expires)
				} else {
					b.InsertBackupLock(key, m.expires, m.token, m.prev, m.owner, m.generation, m.seq)
				}
			}
			expires := time.Now().Add(time.Minute).UnixNano()
			_, token, _, moved, err := a.LockWithTimeout(key, expires, "", "alice")
			if err != nil || moved != "" || token == "" {
				t.Fatalf("acquire: %v moved=%s", err, moved)
			}
			acquire := receive()
			if moved, err := a.Unlock(key, token); err != nil || moved != "" {
				t.Fatalf("unlock: %v moved=%s", err, moved)
			}
			release := receive()
			if reordered {
				deliver(release)
				deliver(acquire)
			} else {
				deliver(acquire)
				deliver(release)
			}
			// Remove the old primary only after both replication RPCs completed.
			b.LockRing.SetSnapshot([]pb.ServerAddress{b.Host}, 2)
			b.PromoteLock(key)
			_, newToken, _, moved, err := b.LockWithTimeout(key, expires, "", "bob")
			if err != nil || moved != "" || newToken == "" {
				t.Fatalf("completed release must allow new owner after failover: error=%v moved=%s token=%q", err, moved, newToken)
			}
		})
	}
}
