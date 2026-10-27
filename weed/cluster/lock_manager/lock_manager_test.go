package lock_manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLockManager_GetLockOwnerIgnoresExpiredLock(t *testing.T) {
	lm := NewLockManager()

	lm.InsertLock("key1", time.Now().Add(-time.Second).UnixNano(), "token1", "", "owner1", 7, 3)

	owner, err := lm.GetLockOwner("key1")
	assert.Empty(t, owner)
	assert.ErrorIs(t, err, LockNotFound)
}

func TestLockManager_InsertLockRejectsStaleTransfer(t *testing.T) {
	lm := NewLockManager()

	lm.InsertLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "", "owner1", 8, 4)
	lm.InsertLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-old", "", "owner1", 7, 3)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(8), lock.Generation)
	assert.Equal(t, int64(4), lock.Seq)
}

func TestLockManager_InsertLockAdvancesGenerationCounter(t *testing.T) {
	lm := NewLockManager()

	lm.InsertLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token1", "", "owner1", 12, 1)

	_, renewToken, generation, _, err := lm.Lock("key2", time.Now().Add(30*time.Second).UnixNano(), "", "owner2")
	assert.NoError(t, err)
	assert.NotEmpty(t, renewToken)
	assert.Greater(t, generation, int64(12))
}

func TestLockManager_InsertBackupLockRejectsOlderGeneration(t *testing.T) {
	lm := NewLockManager()

	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "", "owner1", 8, 1)
	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-old", "", "owner1", 7, 9)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(8), lock.Generation)
	assert.Equal(t, int64(1), lock.Seq)
}

func TestLockManager_InsertBackupLockKeepsPrimaryRole(t *testing.T) {
	lm := NewLockManager()

	ok := lm.InsertLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-old", "", "owner1", 8, 1)
	assert.True(t, ok)

	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "", "owner1", 8, 2)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.False(t, lock.IsBackup)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(8), lock.Generation)
	assert.Equal(t, int64(2), lock.Seq)
}

func TestLockManager_RemoveBackupLockRejectsOlderGeneration(t *testing.T) {
	lm := NewLockManager()

	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "", "owner1", 8, 1)

	removed := lm.RemoveBackupLockIfSeq("key1", 7, 9)
	assert.False(t, removed)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(8), lock.Generation)
	assert.Equal(t, int64(1), lock.Seq)
}

func TestLockManager_LostRenewalResponseReplaysToken(t *testing.T) {
	lm := NewLockManager()
	expires := time.Now().Add(30 * time.Second).UnixNano()

	_, token1, _, _, err := lm.Lock("key1", expires, "", "owner1")
	assert.NoError(t, err)

	// The renewal rotated token1 to token2; the client never learned it.
	_, token2, _, _, err := lm.Lock("key1", expires, token1, "owner1")
	assert.NoError(t, err)
	assert.NotEqual(t, token1, token2)

	// Retrying with the superseded token replays the current one instead of
	// conflicting with its own lease until it expires.
	owner, replayed, _, _, err := lm.Lock("key1", expires, token1, "owner1")
	assert.NoError(t, err)
	assert.Equal(t, "owner1", owner)
	assert.Equal(t, token2, replayed)

	// A replay from a different owner still mismatches.
	_, _, _, _, err = lm.Lock("key1", expires, token1, "intruder")
	assert.Error(t, err)

	// The next rotation retires the superseded token for good.
	_, token3, _, _, err := lm.Lock("key1", expires, token2, "owner1")
	assert.NoError(t, err)
	assert.NotEqual(t, token2, token3)
	_, _, _, _, err = lm.Lock("key1", expires, token1, "owner1")
	assert.Error(t, err)

	// A superseded token still releases its own lock.
	unlocked, _, _, err := lm.Unlock("key1", token2)
	assert.NoError(t, err)
	assert.True(t, unlocked)
}

func TestLockManager_LostRenewalReplayAdvancesSeq(t *testing.T) {
	lm := NewLockManager()
	expires := time.Now().Add(30 * time.Second).UnixNano()
	later := time.Now().Add(60 * time.Second).UnixNano()

	_, token1, _, _, err := lm.Lock("key1", expires, "", "owner1")
	assert.NoError(t, err)

	_, token2, _, seq2, err := lm.Lock("key1", expires, token1, "owner1")
	assert.NoError(t, err)

	// A replay that extends the lease must advance seq, otherwise backups
	// reject the replicated expiry as a stale mutation and can grant the
	// lock when the old expiry passes after a promotion.
	_, replayed, _, seq3, err := lm.Lock("key1", later, token1, "owner1")
	assert.NoError(t, err)
	assert.Equal(t, token2, replayed)
	assert.Equal(t, seq2+1, seq3)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.Equal(t, later, lock.ExpiredAtNs)
	assert.Equal(t, seq3, lock.Seq)

	// Repeated retries keep advancing instead of colliding at one seq.
	_, _, _, seq4, err := lm.Lock("key1", later, token1, "owner1")
	assert.NoError(t, err)
	assert.Equal(t, seq3+1, seq4)
}

func TestLockManager_InsertBackupLockKeepsPrevToken(t *testing.T) {
	lm := NewLockManager()
	expires := time.Now().Add(30 * time.Second).UnixNano()

	// Backup copy of a renewed lock carries both the current token and the
	// superseded one a client may still present after a lost response.
	lm.InsertBackupLock("key1", expires, "token2", "token1", "owner1", 8, 2)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.Equal(t, "token2", lock.Token)
	assert.Equal(t, "token1", lock.PrevToken)

	// Once promoted, the node still replays a retry of the superseded token.
	assert.True(t, lm.PromoteLock("key1"))
	owner, replayed, _, _, err := lm.Lock("key1", expires, "token1", "owner1")
	assert.NoError(t, err)
	assert.Equal(t, "owner1", owner)
	assert.Equal(t, "token2", replayed)
}
