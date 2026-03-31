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
