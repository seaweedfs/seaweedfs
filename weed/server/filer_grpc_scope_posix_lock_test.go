package weed_server

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestScopePosixLockOperationsUseReadWriteGrants(t *testing.T) {
	const key = scopePosixLockKeyPrefix + aliceWS + "/file"
	operations := []struct {
		name  string
		op    filer_pb.PosixLockOp
		write bool
	}{
		{name: "get lock", op: filer_pb.PosixLockOp_GET_LK},
		{name: "try lock", op: filer_pb.PosixLockOp_TRY_LOCK, write: true},
		{name: "unlock", op: filer_pb.PosixLockOp_UNLOCK, write: true},
		{name: "release posix owner", op: filer_pb.PosixLockOp_RELEASE_POSIX_OWNER, write: true},
		{name: "release flock owner", op: filer_pb.PosixLockOp_RELEASE_FLOCK_OWNER, write: true},
		{name: "keep alive", op: filer_pb.PosixLockOp_KEEP_ALIVE, write: true},
	}

	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			h := newScopeHarness(t)
			c := h.client(scopedToken("alice", time.Hour, nil, []string{aliceWS}))
			call := func() error {
				_, err := c.PosixLock(scopeCtx(t), &filer_pb.PosixLockRequest{
					Key: key,
					Op:  tc.op,
					Lock: &filer_pb.PosixLockRange{
						Start: 1,
						End:   2,
					},
				})
				return err
			}
			if tc.write {
				expectScopeDenied(t, h, "PosixLock", "write operation under read-only prefix", call)
				return
			}
			expectScopeForwarded(t, h, "PosixLock", "GET_LK under read-only prefix", call)
		})

		t.Run(tc.name+" with write grant", func(t *testing.T) {
			h := newScopeHarness(t)
			c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
			expectScopeForwarded(t, h, "PosixLock", "operation under read-write prefix", func() error {
				_, err := c.PosixLock(scopeCtx(t), &filer_pb.PosixLockRequest{
					Key: key,
					Op:  tc.op,
					Lock: &filer_pb.PosixLockRange{
						Start: 1,
						End:   2,
					},
				})
				return err
			})
		})
	}
}

func TestScopePosixLockHardLinkOperationsRequireWriteGrant(t *testing.T) {
	id := []byte("known-hard-link")
	key := scopePosixLockKeyPrefix + "hl:" + hex.EncodeToString(id)

	readOnly := newScopeHarness(t)
	readOnly.fake.putKV(id, []byte("/buckets/t1/users/alice/workspace/file"))
	readOnlyClient := readOnly.client(scopedToken("alice", time.Hour, nil, []string{aliceWS}))
	expectScopeForwarded(t, readOnly, "PosixLock", "GET_LK on a known hard link with a read-only grant", func() error {
		_, err := readOnlyClient.PosixLock(scopeCtx(t), &filer_pb.PosixLockRequest{
			Key:  key,
			Op:   filer_pb.PosixLockOp_GET_LK,
			Lock: &filer_pb.PosixLockRange{},
		})
		return err
	})
	expectScopeDenied(t, readOnly, "PosixLock", "mutating a hard-link lock with only a read-only grant", func() error {
		_, err := readOnlyClient.PosixLock(scopeCtx(t), &filer_pb.PosixLockRequest{
			Key:  key,
			Op:   filer_pb.PosixLockOp_TRY_LOCK,
			Lock: &filer_pb.PosixLockRange{},
		})
		return err
	})

	write := newScopeHarness(t)
	write.fake.putKV(id, []byte("/buckets/t1/users/alice/workspace/file"))
	writeClient := write.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	expectScopeForwarded(t, write, "PosixLock", "mutating a hard-link lock with a write grant", func() error {
		_, err := writeClient.PosixLock(scopeCtx(t), &filer_pb.PosixLockRequest{
			Key:  key,
			Op:   filer_pb.PosixLockOp_TRY_LOCK,
			Lock: &filer_pb.PosixLockRange{},
		})
		return err
	})
}
