package lock_manager

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

type leaseMutation struct {
	key, token, prev, owner string
	expires, gen, seq       int64
	unlock                  bool
}

func captureLeaseMutations(t *testing.T, dlm *DistributedLockManager) func() leaseMutation {
	t.Helper()
	messages := make(chan leaseMutation, 4)
	dlm.ReplicateFn = func(_ pb.ServerAddress, key string, expires int64, token, prev, owner string, gen, seq int64, unlock bool) {
		messages <- leaseMutation{key, token, prev, owner, expires, gen, seq, unlock}
	}
	return func() leaseMutation {
		t.Helper()
		select {
		case m := <-messages:
			return m
		case <-time.After(5 * time.Second):
			t.Fatal("replication callback not invoked")
			return leaseMutation{}
		}
	}
}

func (m leaseMutation) deliver(dlm *DistributedLockManager) {
	if m.unlock {
		dlm.RemoveBackupLockIfSeq(m.key, m.gen, m.seq, m.token, m.expires)
	} else {
		dlm.InsertBackupLock(m.key, m.expires, m.token, m.prev, m.owner, m.gen, m.seq)
	}
}

func TestDLM_FreshPrimaryAfterReleasedLease(t *testing.T) {
	for _, previousGeneration := range []int{1, 20} {
		t.Run(fmt.Sprint(previousGeneration), func(t *testing.T) {
			a := NewDistributedLockManager("release-a:8888")
			c := NewDistributedLockManager("release-c:8888")
			initial := []pb.ServerAddress{a.Host, c.Host}
			a.LockRing.SetSnapshot(initial, 1)
			// A newly joined or restarted process has an empty generation counter.
			b := NewDistributedLockManager("release-b:8888")
			next := []pb.ServerAddress{a.Host, b.Host, c.Host}
			b.LockRing.SetSnapshot(next, 2)
			key := ""
			for i := 0; i < 1000; i++ {
				candidate := fmt.Sprintf("key-%d", i)
				oldPrimary, _ := a.LockRing.GetPrimaryAndBackup(candidate)
				primary, backup := b.LockRing.GetPrimaryAndBackup(candidate)
				if oldPrimary == a.Host && primary == b.Host && backup == a.Host {
					key = candidate
					break
				}
			}
			require.NotEmpty(t, key)
			receiveA := captureLeaseMutations(t, a)
			expires := time.Now().Add(time.Minute).UnixNano()
			var acquire, release leaseMutation
			var previousAcquires []leaseMutation
			for i := 0; i < previousGeneration; i++ {
				_, token, _, moved, err := a.LockWithTimeout(key, expires, "", "writer")
				require.NoError(t, err)
				require.Empty(t, moved)
				acquire = receiveA()
				previousAcquires = append(previousAcquires, acquire)
				acquire.deliver(c)
				_, err = a.Unlock(key, token)
				require.NoError(t, err)
				release = receiveA()
				release.deliver(c)
			}
			require.NotEmpty(t, a.lockManager.removed)
			require.Empty(t, a.AllLocks()) // topology handoff has no live lease to transfer
			a.LockRing.SetSnapshot(next, 2)
			receiveB := captureLeaseMutations(t, b)
			_, token, gen, moved, err := b.LockWithTimeout(key, expires, "", "writer")
			require.NoError(t, err)
			require.Empty(t, moved)
			require.EqualValues(t, 1, gen)
			require.NotEqual(t, leaseID(acquire.token), leaseID(token))
			receiveB().deliver(a)
			lock, found := a.GetLock(key)
			require.True(t, found, "new primary's valid lease must be replicated")
			require.Equal(t, token, lock.Token)

			// Old releases must not delete a distinct lease, even at an equal or
			// higher generation. Old transfers must still be rejected.
			release.deliver(a)
			for _, delayed := range previousAcquires {
				delayed.deliver(a)
				require.False(t, a.lockManager.InsertLock(key, delayed.expires, delayed.token, delayed.prev, delayed.owner, delayed.gen, delayed.seq))
			}
			lock, found = a.GetLock(key)
			require.True(t, found)
			require.Equal(t, token, lock.Token)

			// B fails before its lease expires; A must preserve that lease.
			a.LockRing.SetSnapshot([]pb.ServerAddress{a.Host}, 3)
			require.True(t, a.PromoteLock(key))
			_, conflicting, _, _, err := a.LockWithTimeout(key, expires, "", "another-owner")
			require.Error(t, err)
			require.Empty(t, conflicting)
			_, renewed, _, _, err := a.LockWithTimeout(key, expires, token, "writer")
			require.NoError(t, err)
			require.NotEmpty(t, renewed)
		})
	}
}

func TestDLM_ReleaseCoversAllRenewalExpiries(t *testing.T) {
	a := NewDistributedLockManager("expiry-a:8888")
	b := NewDistributedLockManager("expiry-b:8888")
	hosts := []pb.ServerAddress{a.Host, b.Host}
	a.LockRing.SetSnapshot(hosts, 1)
	b.LockRing.SetSnapshot(hosts, 1)
	if primary, _ := a.LockRing.GetPrimaryAndBackup("key"); primary != a.Host {
		a, b = b, a
	}
	receive := captureLeaseMutations(t, a)
	expires := time.Now().Add(time.Minute).UnixNano()
	_, token1, _, _, err := a.LockWithTimeout("key", expires, "", "owner")
	require.NoError(t, err)
	acquire := receive()
	_, token2, _, _, err := a.LockWithTimeout("key", time.Now().Add(time.Second).UnixNano(), token1, "owner")
	require.NoError(t, err)
	renewal := receive()
	require.NotEqual(t, token1, token2)
	require.Equal(t, leaseID(token1), leaseID(token2))
	require.Equal(t, expires, renewal.expires, "renewal cannot shorten the validity bound")
	// Retry a lost response with a shorter requested TTL as well.
	_, replayed, _, _, err := a.LockWithTimeout("key", time.Now().Add(time.Second).UnixNano(), token1, "owner")
	require.NoError(t, err)
	require.Equal(t, token2, replayed)
	retry := receive()
	_, err = a.Unlock("key", token1) // release using the superseded token
	require.NoError(t, err)
	release := receive()
	require.True(t, release.unlock)
	require.Equal(t, token2, release.token)
	require.Equal(t, expires, release.expires)
	release.deliver(b)
	for _, delayed := range []leaseMutation{acquire, renewal, retry} {
		delayed.deliver(b)
		require.False(t, b.lockManager.InsertLock(delayed.key, delayed.expires, delayed.token, delayed.prev, delayed.owner, delayed.gen, delayed.seq))
	}
	_, found := b.GetLock("key")
	require.False(t, found)
	require.NotEmpty(t, a.lockManager.removed, "renewal must preserve replication tracking")
	b.lockManager.cleanUp(expires - 1)
	require.Len(t, b.lockManager.removed, 1)
	b.lockManager.cleanUp(expires)
	require.Nil(t, b.lockManager.removed)
}

func TestDLM_LocalReleasesDoNotRetainRecords(t *testing.T) {
	dlm := NewDistributedLockManager("single:8888")
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{dlm.Host}, 1)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("object-%d", i)
		_, token, _, _, err := dlm.LockWithTimeout(key, time.Now().Add(time.Minute).UnixNano(), "", "writer")
		require.NoError(t, err)
		_, err = dlm.Unlock(key, token)
		require.NoError(t, err)
	}
	require.Empty(t, dlm.AllLocks())
	require.Empty(t, dlm.lockManager.removed)
}

func TestLockManager_ReleaseRecordReclamation(t *testing.T) {
	manager := NewLockManager()
	expires := time.Now().Add(time.Minute).UnixNano()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("object-%d", i)
		token := fmt.Sprintf("lease-%d/secret", i)
		require.True(t, manager.InsertLock(key, expires, token, "", "writer", int64(i+1), 1))
		ok, _, _, err := manager.Unlock(key, token)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.Empty(t, manager.AllLocks())
	require.Len(t, manager.removed, 1000)
	manager.cleanUp(expires)
	require.Nil(t, manager.removed)

	// Replay the exact original payload after its actual validity bound passes.
	expiredLease := time.Now().Add(100 * time.Millisecond).UnixNano()
	require.True(t, manager.InsertLock("expired", expiredLease, "expired-lease/secret", "", "writer", 1, 1))
	ok, _, _, err := manager.Unlock("expired", "expired-lease/secret")
	require.NoError(t, err)
	require.True(t, ok)
	require.Eventually(t, func() bool { return time.Now().UnixNano() >= expiredLease }, time.Second, time.Millisecond)
	manager.cleanUp(time.Now().UnixNano())
	require.Nil(t, manager.removed)
	require.False(t, manager.InsertLock("expired", expiredLease, "expired-lease/secret", "", "writer", 1, 1))
	manager.InsertBackupLock("expired", expiredLease, "expired-lease/secret", "", "writer", 1, 1)
	manager.RemoveBackupLockIfSeq("expired", 1, 2, "expired-lease/secret", expiredLease)
	require.Empty(t, manager.locks)
	require.Nil(t, manager.removed)
}

func TestLockManager_FirstReplicationRacesWithRelease(t *testing.T) {
	for _, replicateFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(replicateFirst), func(t *testing.T) {
			manager := NewLockManager()
			_, token, _, _, err := manager.Lock("key", time.Now().Add(time.Minute).UnixNano(), "", "owner")
			require.NoError(t, err)
			if replicateFirst {
				require.True(t, manager.markReplicated("key", token))
			}
			ok, _, _, err := manager.Unlock("key", token)
			require.NoError(t, err)
			require.True(t, ok)
			require.False(t, manager.markReplicated("key", token), "a released local lease cannot be sent later")
			require.Equal(t, replicateFirst, len(manager.removed) != 0)
		})
	}
}

func TestDLM_HandoffTracksPreviouslyLocalLease(t *testing.T) {
	dlm := NewDistributedLockManager("handoff:8888")
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{dlm.Host}, 1)
	expires := time.Now().Add(time.Minute).UnixNano()
	_, token, _, _, err := dlm.LockWithTimeout("key", expires, "", "owner")
	require.NoError(t, err)
	locks := dlm.AllLocks()
	require.Len(t, locks, 1)
	require.NoError(t, dlm.TransferLock("key", true, func(current *Lock) error {
		require.Equal(t, token, current.Token)
		return nil
	}))
	_, err = dlm.Unlock("key", token)
	require.NoError(t, err)
	require.NotEmpty(t, dlm.lockManager.removed)
	require.NoError(t, dlm.TransferLock("key", true, func(*Lock) error {
		t.Fatal("released lease was handed off")
		return nil
	}))
	require.False(t, dlm.lockManager.InsertLock("key", expires, token, "", "owner", locks[0].Generation, locks[0].Seq))
}

func TestLockManager_LegacyReleaseMetadata(t *testing.T) {
	manager := NewLockManager()
	expires := time.Now().Add(time.Minute).UnixNano()
	manager.InsertBackupLock("key", expires, "legacy-token", "", "owner", 1, 1)
	require.True(t, manager.RemoveBackupLockIfSeq("key", 1, 2, "", 0))
	_, found := manager.GetLock("key")
	require.False(t, found)
	require.True(t, manager.InsertLock("key", expires, "new-lease/secret", "", "owner", 1, 1))
	// An old primary could have released the previous generation-1 lease,
	// or a handed-off slash-format lease. Its identityless message cannot
	// distinguish those cases. Keep the valid lease until its expiry rather
	// than permitting a stale release to delete a different acquisition.
	require.False(t, manager.RemoveBackupLockIfSeq("key", 1, 2, "", 0))
	lock, found := manager.GetLock("key")
	require.True(t, found)
	require.Equal(t, "new-lease/secret", lock.Token)
}

func TestDLM_TransferReplicatesEffectiveExpiry(t *testing.T) {
	for _, incomingTTL := range []time.Duration{30 * time.Second, time.Minute, 90 * time.Second} {
		t.Run(incomingTTL.String(), func(t *testing.T) {
			a, b := NewDistributedLockManager("expiry-a:8888"), NewDistributedLockManager("expiry-b:8888")
			hosts := []pb.ServerAddress{a.Host, b.Host}
			a.LockRing.SetSnapshot(hosts, 1)
			b.LockRing.SetSnapshot(hosts, 1)
			if primary, _ := a.LockRing.GetPrimaryAndBackup("key"); primary != a.Host {
				a, b = b, a
			}
			now := time.Now()
			storedExpiry := now.Add(time.Minute).UnixNano()
			incomingExpiry := now.Add(incomingTTL).UnixNano()
			require.True(t, a.lockManager.InsertLock("key", storedExpiry, "lease/old", "", "owner", 1, 1))
			receive := captureLeaseMutations(t, a)
			a.InsertLock("key", incomingExpiry, "lease/new", "lease/old", "owner", 1, 2)
			mutation := receive()
			require.Equal(t, max(storedExpiry, incomingExpiry), mutation.expires)
			mutation.deliver(b)
			primary, found := a.GetLock("key")
			require.True(t, found)
			backup, found := b.GetLock("key")
			require.True(t, found)
			require.Equal(t, primary.ExpiredAtNs, backup.ExpiredAtNs)
		})
	}
}
