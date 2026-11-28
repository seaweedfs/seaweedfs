package lock_manager

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/require"
)

func TestDLM_TransferLockUsesFreshSnapshot(t *testing.T) {
	dlm := NewDistributedLockManager("handoff:8888")
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{dlm.Host}, 1)
	expires := time.Now().Add(time.Minute).UnixNano()
	_, original, _, _, err := dlm.LockWithTimeout("key", expires, "", "owner")
	require.NoError(t, err)
	// A topology callback may have enumerated this key before its renewal.
	_, renewed, _, _, err := dlm.LockWithTimeout("key", expires, original, "owner")
	require.NoError(t, err)
	require.NoError(t, dlm.TransferLock("key", true, func(current *Lock) error {
		require.Equal(t, renewed, current.Token)
		require.EqualValues(t, 2, current.Seq)
		// The receiver can replicate the lease back before acknowledging.
		dlm.InsertBackupLock(current.Key, current.ExpiredAtNs, current.Token, current.PrevToken, current.Owner, current.Generation, current.Seq)
		return nil
	}))
	current, found := dlm.GetLock("key")
	require.True(t, found)
	require.True(t, current.IsBackup)
	require.Equal(t, renewed, current.Token)
}

func TestDLM_TransferLockFailureFreezesAuthority(t *testing.T) {
	dlm := NewDistributedLockManager("handoff:8888")
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{dlm.Host}, 1)
	expires := time.Now().Add(time.Minute).UnixNano()
	_, token, _, _, err := dlm.LockWithTimeout("key", expires, "", "owner")
	require.NoError(t, err)
	failed := errors.New("handoff failed")
	target := NewDistributedLockManager("target:8888")
	target.LockRing.SetSnapshot([]pb.ServerAddress{target.Host}, 1)
	require.ErrorIs(t, dlm.TransferLock("key", false, func(current *Lock) error {
		// The receiver committed, but its acknowledgement was lost.
		target.InsertLock(current.Key, current.ExpiredAtNs, current.Token, current.PrevToken, current.Owner, current.Generation, current.Seq)
		// A stalled handoff must not hold a global mutex across its RPC.
		_, _, _, _, err := dlm.LockWithTimeout("other-key", expires, "", "other-owner")
		require.NoError(t, err)
		return failed
	}), failed)
	current, found := dlm.GetLock("key")
	require.True(t, found)
	require.True(t, current.IsBackup)
	_, renewed, _, _, err := dlm.LockWithTimeout("key", expires, token, "owner")
	require.ErrorIs(t, err, LockHandoffPending)
	require.Empty(t, renewed, "an ambiguous handoff cannot retain renewal authority")
	_, renewed, _, _, err = target.LockWithTimeout("key", expires, token, "owner")
	require.NoError(t, err)
	require.NotEmpty(t, renewed)
	require.NotEqual(t, token, renewed)
	_, err = dlm.Unlock("key", token)
	require.ErrorIs(t, err, LockHandoffPending)
}

func TestDLM_TransferCompletionPreservesDifferentLease(t *testing.T) {
	for _, keepBackup := range []bool{false, true} {
		t.Run(fmt.Sprint(keepBackup), func(t *testing.T) {
			dlm := NewDistributedLockManager("handoff:8888")
			expires := time.Now().Add(time.Minute).UnixNano()
			dlm.InsertLock("key", expires, "original/secret", "", "owner", 1, 1)
			require.NoError(t, dlm.TransferLock("key", keepBackup, func(*Lock) error {
				// Incoming replication is independent of client admission.
				dlm.InsertLock("key", expires, "replacement/secret", "", "new-owner", 2, 1)
				return nil
			}))
			current, found := dlm.GetLock("key")
			require.True(t, found)
			require.Equal(t, "replacement/secret", current.Token)
			require.False(t, current.IsBackup, "completion must not demote a different lease")
		})
	}
}

// Confirm actual same-key admission contention, rather than inferring it from
// a goroutine start or a sleep in the RPC test.
func TestDLM_UnlockWaitsForHandoffAdmission(t *testing.T) {
	source := NewDistributedLockManager("source:8888")
	target := NewDistributedLockManager("target:8888")
	source.LockRing.SetSnapshot([]pb.ServerAddress{source.Host}, 1)
	expires := time.Now().Add(time.Minute).UnixNano()
	_, token, _, _, err := source.LockWithTimeout("key", expires, "", "owner")
	require.NoError(t, err)
	source.LockRing.SetSnapshot([]pb.ServerAddress{target.Host}, 2)
	entered, proceed := make(chan struct{}), make(chan struct{})
	waiting := make(chan struct{})
	source.onMutationWait = func(key string) {
		if key == "key" {
			close(waiting)
		}
	}
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(proceed) }) }
	defer unblock()
	transferred := make(chan error, 1)
	go func() {
		transferred <- source.TransferLock("key", false, func(lock *Lock) error {
			close(entered)
			<-proceed
			target.InsertLock(lock.Key, lock.ExpiredAtNs, lock.Token, lock.PrevToken, lock.Owner, lock.Generation, lock.Seq)
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handoff did not start")
	}
	released := make(chan pb.ServerAddress, 1)
	go func() { moved, _ := source.Unlock("key", token); released <- moved }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("unlock did not reach contended admission")
	}
	select {
	case <-released:
		t.Fatal("unlock bypassed the handoff gate")
	default:
	}
	unblock()
	select {
	case err := <-transferred:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handoff did not finish")
	}
	select {
	case moved := <-released:
		require.Equal(t, target.Host, moved)
	case <-time.After(5 * time.Second):
		t.Fatal("unlock did not finish after the handoff")
	}
}

func TestDLM_HandoffCompletionRechecksOwner(t *testing.T) {
	for _, next := range []pb.ServerAddress{"source:8888", "third:8888"} {
		t.Run(string(next), func(t *testing.T) {
			source := NewDistributedLockManager("source:8888")
			source.LockRing.SetSnapshot([]pb.ServerAddress{source.Host}, 1)
			_, _, _, _, err := source.LockWithTimeout("key", time.Now().Add(time.Minute).UnixNano(), "", "owner")
			require.NoError(t, err)
			source.LockRing.SetSnapshot([]pb.ServerAddress{"target:8888"}, 2)
			var destination pb.ServerAddress
			var retries int
			source.RetryTransferFn = func(key string) { require.Equal(t, "key", key); retries++ }
			source.TransferFn = func(to pb.ServerAddress, _ *Lock) error {
				destination = to
				source.LockRing.SetSnapshot([]pb.ServerAddress{next}, 3)
				return nil
			}
			err = source.TransferLockToOwner("key")
			current, found := source.GetLock("key")
			require.True(t, found, "an old handoff cannot discard the newest owner's lease")
			if next == source.Host {
				require.NoError(t, err)
				require.False(t, current.IsBackup)
			} else {
				require.ErrorIs(t, err, LockHandoffPending)
				require.Equal(t, 1, retries)
				require.NoError(t, source.TransferLockToOwner("key"))
				require.Equal(t, next, destination)
				_, found = source.GetLock("key")
				require.False(t, found)
			}
		})
	}
}

func TestDLM_ServesLocallyWhenHandoffEndsHere(t *testing.T) {
	source := NewDistributedLockManager("source:8888")
	source.LockRing.SetSnapshot([]pb.ServerAddress{source.Host}, 1)
	expires := time.Now().Add(time.Minute).UnixNano()
	_, token, _, _, err := source.LockWithTimeout("key", expires, "", "owner")
	require.NoError(t, err)
	version := int64(1)
	source.TransferFn = func(pb.ServerAddress, *Lock) error {
		version++
		source.LockRing.SetSnapshot([]pb.ServerAddress{source.Host}, version)
		return nil
	}
	moveAway := func() {
		version++
		source.LockRing.SetSnapshot([]pb.ServerAddress{"target:8888"}, version)
	}
	moveAway()
	_, renewed, _, movedTo, err := source.LockWithTimeout("key", expires, token, "owner")
	require.NoError(t, err)
	require.Empty(t, movedTo, "the ring moved back here during the handoff")
	require.NotEqual(t, token, renewed)
	moveAway()
	movedTo, err = source.Unlock("key", renewed)
	require.NoError(t, err)
	require.Empty(t, movedTo)
	_, found := source.GetLock("key")
	require.False(t, found)
}
