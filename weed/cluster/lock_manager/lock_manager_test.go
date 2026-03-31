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
