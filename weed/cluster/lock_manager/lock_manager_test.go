package lock_manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLockManager_GetLockOwnerIgnoresExpiredLock(t *testing.T) {
	lm := NewLockManager()

	lm.InsertLock("key1", time.Now().Add(-time.Second).UnixNano(), "token1", "owner1", 7, 3)

	owner, err := lm.GetLockOwner("key1")
	assert.Empty(t, owner)
	assert.ErrorIs(t, err, LockNotFound)
}

func TestLockManager_InsertLockRejectsStaleTransfer(t *testing.T) {
	lm := NewLockManager()

	lm.InsertLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "owner1", 8, 4)
	lm.InsertLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-old", "owner1", 7, 3)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(8), lock.Generation)
	assert.Equal(t, int64(4), lock.Seq)
}

func TestLockManager_InsertLockAdvancesGenerationCounter(t *testing.T) {
	lm := NewLockManager()

	lm.InsertLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token1", "owner1", 12, 1)

	_, renewToken, generation, _, err := lm.Lock("key2", time.Now().Add(30*time.Second).UnixNano(), "", "owner2")
	assert.NoError(t, err)
	assert.NotEmpty(t, renewToken)
	assert.Greater(t, generation, int64(12))
}

func TestLockManager_InsertBackupLockRejectsOlderGeneration(t *testing.T) {
	lm := NewLockManager()

	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "owner1", 8, 1)
	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-old", "owner1", 7, 9)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(8), lock.Generation)
	assert.Equal(t, int64(1), lock.Seq)
}

func TestLockManager_InsertBackupLockKeepsPrimaryRole(t *testing.T) {
	lm := NewLockManager()

	ok := lm.InsertLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-old", "owner1", 8, 1)
	assert.True(t, ok)

	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "owner1", 8, 2)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.False(t, lock.IsBackup)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(8), lock.Generation)
	assert.Equal(t, int64(2), lock.Seq)
}

func TestLockManager_RemoveBackupLockRejectsOlderGeneration(t *testing.T) {
	lm := NewLockManager()

	lm.InsertBackupLock("key1", time.Now().Add(30*time.Second).UnixNano(), "token-new", "owner1", 8, 1)

	removed := lm.RemoveBackupLockIfSeq("key1", 7, 9)
	assert.False(t, removed)

	lock, found := lm.GetLock("key1")
	assert.True(t, found)
	assert.Equal(t, "token-new", lock.Token)
	assert.Equal(t, int64(8), lock.Generation)
	assert.Equal(t, int64(1), lock.Seq)
}
