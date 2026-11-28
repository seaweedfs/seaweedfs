package lock_manager

import (
	"errors"
	"fmt"
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

func TestDLM_TransferLockFailureKeepsAuthority(t *testing.T) {
	dlm := NewDistributedLockManager("handoff:8888")
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{dlm.Host}, 1)
	expires := time.Now().Add(time.Minute).UnixNano()
	_, token, _, _, err := dlm.LockWithTimeout("key", expires, "", "owner")
	require.NoError(t, err)
	failed := errors.New("handoff failed")
	require.ErrorIs(t, dlm.TransferLock("key", false, func(*Lock) error {
		// A stalled handoff must not hold a global mutex across its RPC.
		_, _, _, _, err := dlm.LockWithTimeout("other-key", expires, "", "other-owner")
		require.NoError(t, err)
		return failed
	}), failed)
	current, found := dlm.GetLock("key")
	require.True(t, found)
	require.False(t, current.IsBackup)
	_, renewed, _, _, err := dlm.LockWithTimeout("key", expires, token, "owner")
	require.NoError(t, err, "failure must release the mutation gate")
	require.NotEmpty(t, renewed)
}

func TestDLM_TransferCompletionPreservesDifferentLease(t *testing.T) {
	for _, keepBackup := range []bool{false, true} {
		t.Run(fmt.Sprint(keepBackup), func(t *testing.T) {
			dlm := NewDistributedLockManager("handoff:8888")
			expires := time.Now().Add(time.Minute).UnixNano()
			dlm.InsertLock("key", expires, "original/secret", "", "owner", 1, 1)
			require.NoError(t, dlm.TransferLock("key", keepBackup, func(*Lock) error {
				// Incoming replication is independent of client admission.
				dlm.InsertBackupLock("key", expires, "replacement/secret", "", "new-owner", 2, 1)
				return nil
			}))
			current, found := dlm.GetLock("key")
			require.True(t, found)
			require.Equal(t, "replacement/secret", current.Token)
			require.False(t, current.IsBackup, "completion must not demote a different lease")
		})
	}
}
